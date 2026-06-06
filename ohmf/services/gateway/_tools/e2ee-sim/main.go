// Command e2ee-sim is an in-process micro-benchmark / simulation of E2EE message
// *generation and validation*. It is NOT a WebSocket load test and produces no
// system-level evidence.
//
// What it actually does:
//   - Generates synthetic encrypted/plaintext message payloads in memory
//     (random session keys, nonces, an ephemeral ed25519 keypair per message,
//     and per-recipient wrapped keys).
//   - "Validates" each one with a fixed time.Sleep, then reports throughput and
//     min/avg/max per-op timings.
//
// What it explicitly does NOT do:
//   - It does not open WebSocket (or any network) connections to the gateway.
//   - It does not measure end-to-end p95 latency, message loss, or throughput
//     of the running system. The reported "times" are dominated by a hardcoded
//     sleep and the cost of key generation, not by the messaging path.
//
// Treat the numbers as a CPU micro-benchmark for payload construction only. For
// what a credible system load test must capture, see benchmarks/README.md at the
// repository root.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"
)

// SimConfig holds simulation parameters.
type SimConfig struct {
	NumMessages     int
	NumRecipients   int
	EncryptionRatio float64 // fraction of messages that are encrypted
	Concurrent      int
	Verbose         bool
}

// SimResult holds statistics from a simulation run.
type SimResult struct {
	TotalMessages     int
	EncryptedMessages int
	PlaintextMessages int
	TotalTime         time.Duration
	MessageRate       float64 // messages per second
	AvgTime           time.Duration
	MinTime           time.Duration
	MaxTime           time.Duration
	ErrorCount        int
}

// Simulator generates and validates synthetic E2EE message payloads in memory.
type Simulator struct {
	config SimConfig
	result SimResult
	mu     sync.Mutex
	times  []time.Duration
}

// NewSimulator creates a new simulator.
func NewSimulator(config SimConfig) *Simulator {
	return &Simulator{
		config: config,
		times:  make([]time.Duration, 0, config.NumMessages),
	}
}

// GenerateEncryptedMessage builds one synthetic E2EE message payload.
func (s *Simulator) GenerateEncryptedMessage() (map[string]any, error) {
	// Generate session key
	sessionKey := make([]byte, 32)
	if _, err := rand.Read(sessionKey); err != nil {
		return nil, err
	}

	// Generate nonce
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	// Generate ephemeral keypair for signing
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	// Sign the ciphertext
	ciphertext := sessionKey
	signature := ed25519.Sign(privKey, ciphertext)

	msg := map[string]any{
		"ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
		"nonce":      base64.StdEncoding.EncodeToString(nonce),
		"encryption": map[string]any{
			"scheme":            "OHMF_SIGNAL_V1",
			"sender_user_id":    fmt.Sprintf("user-%d", mrand.IntN(1000)),
			"sender_device_id":  fmt.Sprintf("device-%d", mrand.IntN(100)),
			"sender_signature":  base64.StdEncoding.EncodeToString(signature),
			"sender_public_key": base64.StdEncoding.EncodeToString(pubKey),
			"recipients":        s.generateRecipients(),
		},
	}

	return msg, nil
}

// GeneratePlaintextMessage builds one synthetic plaintext message payload.
func (s *Simulator) GeneratePlaintextMessage() map[string]any {
	return map[string]any{
		"text": fmt.Sprintf("Test message %d", mrand.IntN(10000)),
	}
}

// generateRecipients creates a list of per-recipient wrapped device keys.
func (s *Simulator) generateRecipients() []map[string]any {
	recipients := make([]map[string]any, 0, s.config.NumRecipients)

	for i := 0; i < s.config.NumRecipients; i++ {
		key := make([]byte, 32)
		rand.Read(key)
		nonce := make([]byte, 12)
		rand.Read(nonce)

		recipients = append(recipients, map[string]any{
			"user_id":     fmt.Sprintf("user-%d", mrand.IntN(1000)),
			"device_id":   fmt.Sprintf("device-%d", mrand.IntN(100)),
			"wrapped_key": base64.StdEncoding.EncodeToString(key),
			"wrap_nonce":  base64.StdEncoding.EncodeToString(nonce),
		})
	}

	return recipients
}

// ProcessMessage simulates message validation with a fixed cost. NOTE: this sleep
// is the dominant term in the reported timings; it is a placeholder, not a real
// validation path.
func (s *Simulator) ProcessMessage(msg map[string]any) error {
	if _, ok := msg["encryption"]; ok {
		// Encrypted message takes slightly longer
		time.Sleep(time.Microsecond * 10)
	} else {
		// Plaintext message
		time.Sleep(time.Microsecond * 5)
	}
	return nil
}

// Run executes the simulation.
func (s *Simulator) Run() {
	fmt.Printf("Starting E2EE message-generation simulation: %d messages, %.0f%% encrypted\n", s.config.NumMessages, s.config.EncryptionRatio*100)

	startTime := time.Now()
	sem := make(chan struct{}, s.config.Concurrent)
	var wg sync.WaitGroup

	for i := 0; i < s.config.NumMessages; i++ {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore slot

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore slot

			opStart := time.Now()

			var msg map[string]any
			var err error

			if mrand.Float64() < s.config.EncryptionRatio {
				msg, err = s.GenerateEncryptedMessage()
				s.mu.Lock()
				s.result.EncryptedMessages++
				s.mu.Unlock()
			} else {
				msg = s.GeneratePlaintextMessage()
				s.mu.Lock()
				s.result.PlaintextMessages++
				s.mu.Unlock()
			}

			if err != nil {
				s.mu.Lock()
				s.result.ErrorCount++
				s.mu.Unlock()
				return
			}

			if err := s.ProcessMessage(msg); err != nil {
				s.mu.Lock()
				s.result.ErrorCount++
				s.mu.Unlock()
				return
			}

			elapsed := time.Since(opStart)
			s.mu.Lock()
			s.times = append(s.times, elapsed)
			if s.config.Verbose && idx%100 == 0 {
				fmt.Printf("  Processed %d messages...\n", idx)
			}
			s.mu.Unlock()
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	s.result.TotalMessages = s.config.NumMessages
	s.result.TotalTime = totalTime
	s.result.MessageRate = float64(s.config.NumMessages) / totalTime.Seconds()

	s.calculateStats()
	s.PrintResults()
}

// calculateStats computes min, max, and average times.
func (s *Simulator) calculateStats() {
	if len(s.times) == 0 {
		return
	}

	minTime := s.times[0]
	maxTime := s.times[0]
	totalTime := time.Duration(0)

	for _, t := range s.times {
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
		totalTime += t
	}

	s.result.MinTime = minTime
	s.result.MaxTime = maxTime
	s.result.AvgTime = time.Duration(totalTime.Nanoseconds() / int64(len(s.times)))
}

// PrintResults prints simulation results.
func (s *Simulator) PrintResults() {
	bar := strings.Repeat("=", 60)
	fmt.Println("\n" + bar)
	fmt.Println("E2EE MESSAGE-GENERATION SIMULATION RESULTS (NOT a load test)")
	fmt.Println(bar)
	fmt.Printf("Total Messages:       %d\n", s.result.TotalMessages)
	fmt.Printf("  - Encrypted:       %d (%.1f%%)\n", s.result.EncryptedMessages, float64(s.result.EncryptedMessages)*100/float64(s.result.TotalMessages))
	fmt.Printf("  - Plaintext:       %d (%.1f%%)\n", s.result.PlaintextMessages, float64(s.result.PlaintextMessages)*100/float64(s.result.TotalMessages))
	fmt.Printf("Errors:              %d\n", s.result.ErrorCount)
	fmt.Printf("Total Time:          %v\n", s.result.TotalTime)
	fmt.Printf("Gen Rate:            %.2f msg/sec (in-process, not network throughput)\n", s.result.MessageRate)
	fmt.Printf("Average Time:        %v (dominated by a fixed sleep; not real latency)\n", s.result.AvgTime)
	fmt.Printf("Min Time:            %v\n", s.result.MinTime)
	fmt.Printf("Max Time:            %v\n", s.result.MaxTime)
	fmt.Println(bar)
}

func main() {
	numMessages := flag.Int("messages", 1000, "Number of messages to generate")
	numRecipients := flag.Int("recipients", 5, "Number of recipients per encrypted message")
	encryptionRatio := flag.Float64("encrypted", 0.5, "Ratio of encrypted messages (0.0-1.0)")
	concurrent := flag.Int("concurrent", 10, "Number of concurrent generators")
	verbose := flag.Bool("verbose", false, "Verbose output")

	flag.Parse()

	if *numMessages < 0 || *concurrent < 1 {
		log.Fatal("invalid parameters")
	}

	if *encryptionRatio < 0 || *encryptionRatio > 1 {
		log.Fatal("encryption ratio must be between 0 and 1")
	}

	config := SimConfig{
		NumMessages:     *numMessages,
		NumRecipients:   *numRecipients,
		EncryptionRatio: *encryptionRatio,
		Concurrent:      *concurrent,
		Verbose:         *verbose,
	}

	sim := NewSimulator(config)
	sim.Run()
}
