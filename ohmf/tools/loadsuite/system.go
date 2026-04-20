package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type jsonlWriter struct {
	mu         sync.Mutex
	file       *os.File
	onRawEvent func(rawEvent)
}

func newJSONLWriter(path string) (*jsonlWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &jsonlWriter{file: file}, nil
}

func (w *jsonlWriter) write(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(body, '\n')); err != nil {
		return err
	}
	if event, ok := v.(rawEvent); ok && w.onRawEvent != nil {
		w.onRawEvent(event)
	}
	return nil
}

func (w *jsonlWriter) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		mode:            "full",
		profile:         "active_sustain",
		baseURL:         "http://localhost:18080",
		hostBaseURL:     "http://localhost:18080",
		composeFile:     filepath.Join("infra", "docker", "docker-compose.yml"),
		listenAddr:      ":18181",
		initialDevices:  100,
		maxDevices:      3000,
		proveDuration:   3 * time.Minute,
		sustainDuration: 30 * time.Minute,
		warmupDuration:  90 * time.Second,
		connectWaveWait: 15 * time.Second,
		readyThreshold:  0.98,
		artifactRoot:    filepath.Join("docs", "reports", "validation"),
		seed:            time.Now().UnixNano(),
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--mode":
			i++
			cfg.mode = args[i]
		case "--profile":
			i++
			cfg.profile = args[i]
		case "--base-url":
			i++
			cfg.baseURL = args[i]
		case "--host-base-url":
			i++
			cfg.hostBaseURL = args[i]
		case "--compose-file":
			i++
			cfg.composeFile = args[i]
		case "--listen-addr":
			i++
			cfg.listenAddr = args[i]
		case "--controller-url":
			i++
			cfg.controllerURL = args[i]
		case "--agent-id":
			i++
			cfg.agentID = args[i]
		case "--shared-secret":
			i++
			cfg.sharedSecret = args[i]
		case "--agent-share":
			i++
			cfg.agentShare = args[i]
		case "--initial-devices":
			i++
			value, err := strconv.Atoi(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.initialDevices = value
		case "--max-devices":
			i++
			value, err := strconv.Atoi(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.maxDevices = value
		case "--prove-duration":
			i++
			value, err := time.ParseDuration(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.proveDuration = value
		case "--sustain-duration":
			i++
			value, err := time.ParseDuration(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.sustainDuration = value
		case "--artifact-dir":
			i++
			cfg.artifactRoot = args[i]
		case "--fixture-dir":
			i++
			cfg.fixtureDir = args[i]
		case "--connect-wave-size":
			i++
			value, err := strconv.Atoi(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.connectWaveSize = value
		case "--connect-wave-interval":
			i++
			value, err := time.ParseDuration(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.connectWaveWait = value
		case "--warmup-duration":
			i++
			value, err := time.ParseDuration(args[i])
			if err != nil {
				return config{}, err
			}
			cfg.warmupDuration = value
		case "--ready-threshold":
			i++
			value, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				return config{}, err
			}
			if value > 1 {
				value /= 100
			}
			cfg.readyThreshold = value
		case "--seed":
			i++
			value, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return config{}, err
			}
			cfg.seed = value
		default:
			return config{}, fmt.Errorf("unknown flag %q", args[i])
		}
	}

	if cfg.initialDevices <= 0 || cfg.maxDevices < cfg.initialDevices {
		return config{}, errors.New("invalid device bounds")
	}
	if strings.TrimSpace(cfg.hostBaseURL) == "" {
		cfg.hostBaseURL = cfg.baseURL
	}
	if strings.TrimSpace(cfg.profile) == "" {
		switch strings.ToLower(cfg.mode) {
		case "resilience":
			cfg.profile = "resilience"
		case "connected_idle":
			cfg.profile = "connected_idle"
		default:
			cfg.profile = "active_sustain"
		}
	}
	cfg.runID = fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	cfg.runDir = filepath.Join(cfg.artifactRoot, cfg.runID)
	if strings.TrimSpace(cfg.fixtureDir) == "" {
		cfg.fixtureDir = filepath.Join(cfg.runDir, "fixture")
	}
	return cfg, nil
}

func prepareRunDirectory(cfg config) error {
	return os.MkdirAll(cfg.runDir, 0o755)
}

func composeArgs(composeFile string, args ...string) []string {
	composeArgs := []string{"compose"}
	for _, item := range strings.Split(composeFile, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		composeArgs = append(composeArgs, "-f", item)
	}
	composeArgs = append(composeArgs, args...)
	return composeArgs
}

func benchmarkComposeEnabled(composeFile string) bool {
	return strings.Contains(strings.ToLower(composeFile), "docker-compose.benchmark")
}

func runCommand(ctx context.Context, dir string, command string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s failed: %w\n%s", command, strings.Join(args, " "), err, string(output))
	}
	return output, nil
}

func ensureServicesRunning(ctx context.Context, cfg config) error {
	services := []string{
		"db",
		"redis",
		"kafka",
		"cassandra",
		"apps",
		"api",
		"messages-processor",
		"delivery-processor",
		"sms-processor",
		"prometheus",
	}
	if benchmarkComposeEnabled(cfg.composeFile) {
		services = append(services,
			"messages-processor-b",
			"sync-fanout-worker-a",
			"sync-fanout-worker-b",
			"messages-projector",
		)
	}
	if os.Getenv("OHMF_LOAD_SKIP_STACK_UP") != "1" {
		upArgs := append(composeArgs(cfg.composeFile, "up", "-d", "--build", "--force-recreate"), services...)
		if _, err := runCommand(ctx, ".", "docker", upArgs...); err != nil {
			return err
		}
	}

	required := append([]string(nil), services...)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		output, err := runCommand(ctx, ".", "docker", composeArgs(cfg.composeFile, "ps", "--services", "--filter", "status=running")...)
		if err == nil {
			running := map[string]struct{}{}
			for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
				line = strings.TrimSpace(line)
				if line != "" {
					running[line] = struct{}{}
				}
			}
			missing := false
			for _, service := range required {
				if _, ok := running[service]; !ok {
					missing = true
					break
				}
			}
			if !missing && waitForHealthyEndpoints(cfg.baseURL) == nil {
				return nil
			}
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("required OHMF services did not reach running state")
}

func waitForHealthyEndpoints(baseURL string) error {
	targets := []string{
		strings.TrimRight(baseURL, "/") + "/healthz",
		"http://localhost:18086/healthz",
		"http://localhost:19090/-/healthy",
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, target := range targets {
		resp, err := client.Get(target)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s returned %d", target, resp.StatusCode)
		}
	}
	return nil
}

type resourceSampler struct {
	reducer     *reducer
	resourceCSV *csv.Writer
	lagCSV      *csv.Writer
	resourceMu  sync.Mutex
	lagMu       sync.Mutex
	lagSamples  []lagSample
}

type lagSample struct {
	Timestamp time.Time
	Group     string
	TotalLag  int64
}

func newResourceSampler(runDir string, reducer *reducer) (*resourceSampler, error) {
	resourceFile, err := os.Create(filepath.Join(runDir, "resource_samples.csv"))
	if err != nil {
		return nil, err
	}
	lagFile, err := os.Create(filepath.Join(runDir, "kafka_lag.csv"))
	if err != nil {
		resourceFile.Close()
		return nil, err
	}
	resourceWriter := csv.NewWriter(resourceFile)
	lagWriter := csv.NewWriter(lagFile)
	resourceWriter.Write([]string{"timestamp", "container", "cpu_percent", "memory_bytes"})
	resourceWriter.Flush()
	lagWriter.Write([]string{"timestamp", "group", "total_lag"})
	lagWriter.Flush()
	return &resourceSampler{
		reducer:     reducer,
		resourceCSV: resourceWriter,
		lagCSV:      lagWriter,
	}, nil
}

func (s *resourceSampler) start(ctx context.Context) {
	go s.sampleResources(ctx)
	go s.sampleKafkaLag(ctx)
}

func (s *resourceSampler) sampleResources(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	containers := []string{"ohmf-api", "ohmf-messages-processor", "ohmf-delivery-processor", "ohmf-sms-processor", "ohmf-messages-processor-b", "ohmf-sync-fanout-worker-a", "ohmf-sync-fanout-worker-b", "ohmf-messages-projector"}
	for {
		select {
		case <-ctx.Done():
			s.resourceCSV.Flush()
			return
		case <-ticker.C:
			args := []string{"stats", "--no-stream", "--format", "{{.Name}},{{.CPUPerc}},{{.MemUsage}}"}
			args = append(args, containers...)
			output, err := runCommand(ctx, ".", "docker", args...)
			if err != nil {
				continue
			}
			now := time.Now().UTC()
			stackCPU := 0.0
			stackMemory := 0.0
			for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
				parts := strings.Split(line, ",")
				if len(parts) < 3 {
					continue
				}
				cpu := parsePercent(parts[1])
				memory := parseMemoryUsage(parts[2])
				stackCPU += cpu
				stackMemory += memory
				s.resourceMu.Lock()
				_ = s.resourceCSV.Write([]string{
					now.Format(time.RFC3339Nano),
					strings.TrimSpace(parts[0]),
					fmt.Sprintf("%.4f", cpu),
					fmt.Sprintf("%.0f", memory),
				})
				s.resourceCSV.Flush()
				s.resourceMu.Unlock()
			}
			s.reducer.updateResourcePeak(stackCPU, stackMemory, 0)
		}
	}
}

func (s *resourceSampler) sampleKafkaLag(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	groups := []string{"messages-processor-v1", "delivery-processor-v1", "sms-processor-v1"}
	for {
		select {
		case <-ctx.Done():
			s.lagCSV.Flush()
			return
		case <-ticker.C:
			now := time.Now().UTC()
			total := int64(0)
			for _, group := range groups {
				lag := queryConsumerGroupLag(ctx, group)
				total += lag
				s.lagMu.Lock()
				s.lagSamples = append(s.lagSamples, lagSample{
					Timestamp: now,
					Group:     group,
					TotalLag:  lag,
				})
				_ = s.lagCSV.Write([]string{now.Format(time.RFC3339Nano), group, strconv.FormatInt(lag, 10)})
				s.lagCSV.Flush()
				s.lagMu.Unlock()
			}
			s.reducer.updateResourcePeak(0, 0, total)
		}
	}
}

func (s *resourceSampler) currentLagTotal() int64 {
	s.lagMu.Lock()
	defer s.lagMu.Unlock()
	total := int64(0)
	if len(s.lagSamples) < 3 {
		for _, sample := range s.lagSamples {
			total += sample.TotalLag
		}
		return total
	}
	lastTimestamp := s.lagSamples[len(s.lagSamples)-1].Timestamp
	for i := len(s.lagSamples) - 1; i >= 0; i-- {
		if !s.lagSamples[i].Timestamp.Equal(lastTimestamp) {
			break
		}
		total += s.lagSamples[i].TotalLag
	}
	return total
}

func (s *resourceSampler) lagSamplesSince(start time.Time) []lagSample {
	s.lagMu.Lock()
	defer s.lagMu.Unlock()
	out := make([]lagSample, 0)
	grouped := map[time.Time]int64{}
	for _, sample := range s.lagSamples {
		if sample.Timestamp.Before(start) {
			continue
		}
		grouped[sample.Timestamp] += sample.TotalLag
	}
	for ts, total := range grouped {
		out = append(out, lagSample{Timestamp: ts, TotalLag: total})
	}
	sortLagSamples(out)
	return out
}

func sortLagSamples(samples []lagSample) {
	for i := 0; i < len(samples); i++ {
		for j := i + 1; j < len(samples); j++ {
			if samples[j].Timestamp.Before(samples[i].Timestamp) {
				samples[i], samples[j] = samples[j], samples[i]
			}
		}
	}
}

func queryConsumerGroupLag(ctx context.Context, group string) int64 {
	output, err := runCommand(ctx, ".", "docker", "exec", "ohmf-kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:9092", "--describe", "--group", group)
	if err != nil {
		return 0
	}
	var total int64
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		value, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
		if err == nil {
			total += value
		}
	}
	return total
}

func parsePercent(raw string) float64 {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, "%"))
	value, _ := strconv.ParseFloat(raw, 64)
	return value
}

func parseMemoryUsage(raw string) float64 {
	parts := strings.Split(raw, "/")
	if len(parts) == 0 {
		return 0
	}
	return parseByteString(parts[0])
}

func parseByteString(raw string) float64 {
	normalized := strings.ReplaceAll(strings.TrimSpace(raw), " ", "")
	if normalized == "" {
		return 0
	}
	split := strings.IndexFunc(normalized, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.' && r != '+' && r != '-'
	})
	if split <= 0 {
		return 0
	}
	value, err := strconv.ParseFloat(normalized[:split], 64)
	if err != nil {
		return 0
	}
	switch strings.ToUpper(normalized[split:]) {
	case "B":
		return value
	case "KIB":
		return value * 1024
	case "MIB":
		return value * 1024 * 1024
	case "GIB":
		return value * 1024 * 1024 * 1024
	case "TIB":
		return value * 1024 * 1024 * 1024 * 1024
	case "KB":
		return value * 1000
	case "MB":
		return value * 1000 * 1000
	case "GB":
		return value * 1000 * 1000 * 1000
	default:
		return value
	}
}

func queryPrometheusDuration(ctx context.Context, query string) time.Duration {
	u, _ := url.Parse("http://localhost:19090/api/v1/query")
	values := u.Query()
	values.Set("query", query)
	u.RawQuery = values.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0
	}
	if payload.Status != "success" || len(payload.Data.Result) == 0 || len(payload.Data.Result[0].Value) < 2 {
		return 0
	}
	valueText, _ := payload.Data.Result[0].Value[1].(string)
	value, _ := strconv.ParseFloat(valueText, 64)
	return time.Duration(value * float64(time.Second))
}

func maxPrometheusDuration(ctx context.Context, queries ...string) time.Duration {
	var out time.Duration
	for _, query := range queries {
		value := queryPrometheusDuration(ctx, query)
		if value > out {
			out = value
		}
	}
	return out
}

func (s *resourceSampler) resourcePeaks() (float64, float64, int64) {
	s.reducer.mu.Lock()
	defer s.reducer.mu.Unlock()
	return s.reducer.resourcePeakCPU, s.reducer.resourcePeakMemory, s.reducer.resourcePeakKafkaLag
}

func restartComposeService(ctx context.Context, composeFile string, service string) error {
	_, err := runCommand(ctx, ".", "docker", composeArgs(composeFile, "restart", service)...)
	return err
}

func waitForHTTP(ctx context.Context, target string, timeout time.Duration) error {
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for %s", target)
}

func postJSONRequest(ctx context.Context, client *http.Client, target string, bearerToken string, body any) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return nil, err
		}
		reader = buf
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(bearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s returned %d: %s", target, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func postJSONWithRetry(ctx context.Context, client *http.Client, target string, bearerToken string, body any) (map[string]any, error) {
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		payload, err := postJSONRequest(ctx, client, target, bearerToken, body)
		if err == nil {
			return payload, nil
		}
		if !isRetryableHTTPError(err) || attempt == 4 {
			return nil, err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, fmt.Errorf("unreachable retry state")
}

func patchJSONRequest(ctx context.Context, client *http.Client, target string, bearerToken string, body any) (map[string]any, error) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, target, buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s returned %d: %s", target, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return payload, nil
}

func getJSONRequest(ctx context.Context, client *http.Client, target string, bearerToken string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(bearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s returned %d: %s", target, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func isRetryableHTTPError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, " returned 429") ||
		strings.Contains(message, " returned 500") ||
		strings.Contains(message, " returned 502") ||
		strings.Contains(message, " returned 503") ||
		strings.Contains(message, " returned 504") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "deadline exceeded")
}

func isUnauthorizedError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, " returned 401") || strings.Contains(message, "unauthorized") || strings.Contains(message, "invalid token")
}

func writeRawEvent(writer *jsonlWriter, event rawEvent) {
	if writer == nil {
		return
	}
	_ = writer.write(event)
}

func recordPhaseFailure(writer *jsonlWriter, phase string, reason string, err error) {
	if err == nil {
		return
	}
	writeRawEvent(writer, rawEvent{
		Timestamp: time.Now().UTC(),
		Type:      "phase_failure",
		Phase:     phase,
		Outcome:   normalizeFailureReason(reason),
		Value:     err.Error(),
	})
}

func classifyFailureReason(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "bad handshake"):
		return "bad_handshake"
	case strings.Contains(message, " returned 429") || strings.Contains(message, "too many requests"):
		return "429_rate_limited"
	case strings.Contains(message, " returned 401") || strings.Contains(message, "unauthorized"):
		return "401_unauthorized"
	case strings.Contains(message, "resume_failed"):
		return "resume_failed"
	case strings.Contains(message, "timeout") || strings.Contains(message, "deadline exceeded"):
		return "http_timeout"
	default:
		return "other"
	}
}

func normalizeFailureReason(reason string) string {
	reason = strings.TrimSpace(strings.ToLower(reason))
	if reason == "" {
		return "other"
	}
	return reason
}
