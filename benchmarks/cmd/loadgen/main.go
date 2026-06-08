package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type scenarioConfig struct {
	Scenario               string            `json:"scenario"`
	RunID                  string            `json:"run_id,omitempty"`
	BaseURL                string            `json:"base_url"`
	DriverLocation         string            `json:"driver_location"`
	RatePerSecond          int               `json:"rate_per_second"`
	DurationSeconds        int               `json:"duration_seconds"`
	ActiveConversations    int               `json:"active_conversations"`
	MessageText            string            `json:"message_text"`
	AuthOTPCode            string            `json:"auth_otp_code"`
	Namespace              string            `json:"namespace"`
	PostgresResource       string            `json:"postgres_resource"`
	PostgresUser           string            `json:"postgres_user"`
	PostgresDB             string            `json:"postgres_db"`
	CassandraResource      string            `json:"cassandra_resource"`
	CassandraKeyspace      string            `json:"cassandra_keyspace"`
	KafkaResource          string            `json:"kafka_resource"`
	KafkaConsumerGroup     string            `json:"kafka_consumer_group"`
	KafkaIngressTopic      string            `json:"kafka_ingress_topic"`
	KafkaLagPollSeconds    int               `json:"kafka_lag_poll_seconds"`
	KafkaLagTimeoutSeconds int               `json:"kafka_lag_timeout_seconds"`
	RequestTimeoutSeconds  int               `json:"request_timeout_seconds"`
	DuplicateEvery         int               `json:"duplicate_every"`
	Metadata               map[string]string `json:"metadata,omitempty"`
}

type envCapture struct {
	TimestampUTC      string            `json:"timestamp_utc"`
	GitHead           string            `json:"git_head,omitempty"`
	WorkingTreeStatus string            `json:"working_tree_status,omitempty"`
	GitHeadNote       string            `json:"git_head_note,omitempty"`
	Kubectl           commandCapture    `json:"kubectl"`
	Kubernetes        commandCapture    `json:"kubernetes"`
	K3d               commandCapture    `json:"k3d"`
	Machine           machineCapture    `json:"machine"`
	Commands          map[string]string `json:"commands"`
}

type commandCapture struct {
	Command string `json:"command"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}

type machineCapture struct {
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUs     int    `json:"cpus"`
	Go       string `json:"go_version"`
}

type runArtifacts struct {
	Scenario      string                 `json:"scenario"`
	RunID         string                 `json:"run_id"`
	ResultDir     string                 `json:"result_dir"`
	DriverMode    string                 `json:"driver_mode"`
	CompletedUTC  string                 `json:"completed_utc"`
	Cancelled     bool                   `json:"cancelled"`
	Counts        countSummary           `json:"counts"`
	Latency       latencySummary         `json:"latency"`
	Reconcile     reconcileSummary       `json:"reconcile"`
	Conversations []conversationArtifact `json:"conversations"`
	Limitations   []string               `json:"limitations"`
	Supported     string                 `json:"supported_claim"`
}

type countSummary struct {
	Requested               int64            `json:"requested"`
	SentAttempts            int64            `json:"sent_attempts"`
	Accepted                int64            `json:"accepted"`
	AcceptedResponses       int64            `json:"accepted_responses"`
	SendFailures            map[string]int64 `json:"send_failures"`
	Duplicates              int64            `json:"duplicates"`
	Missing                 int64            `json:"missing"`
	UniqueIdempotencyKeys   int64            `json:"unique_idempotency_keys"`
	UniqueAcceptedMessageID int64            `json:"unique_accepted_message_ids"`
}

type latencySummary struct {
	SampleCount int     `json:"sample_count"`
	P50MS       float64 `json:"p50_ms"`
	P95MS       float64 `json:"p95_ms"`
	P99MS       float64 `json:"p99_ms"`
	Location    string  `json:"location"`
}

type reconcileSummary struct {
	PostgresBefore         int64    `json:"postgres_before"`
	PostgresAfter          int64    `json:"postgres_after"`
	PostgresDelta          int64    `json:"postgres_delta"`
	CassandraBefore        int64    `json:"cassandra_before"`
	CassandraAfter         int64    `json:"cassandra_after"`
	CassandraDelta         int64    `json:"cassandra_delta"`
	KafkaLagAtEnd          int64    `json:"kafka_lag_at_end"`
	KafkaLagSettledToZero  bool     `json:"kafka_lag_settled_to_zero"`
	KafkaLagZeroSeconds    float64  `json:"kafka_lag_zero_seconds"`
	RunScopedReconcileMode string   `json:"run_scoped_reconcile_mode"`
	Limitations            []string `json:"limitations"`
}

type conversationArtifact struct {
	ConversationID string `json:"conversation_id"`
	RecipientUser  string `json:"recipient_user_id"`
}

type authStartResponse struct {
	ChallengeID string `json:"challenge_id"`
}

type authVerifyResponse struct {
	User struct {
		UserID string `json:"user_id"`
	} `json:"user"`
	Tokens struct {
		AccessToken string `json:"access_token"`
	} `json:"tokens"`
}

type conversationResponse struct {
	ConversationID string `json:"conversation_id"`
}

type messageAck struct {
	MessageID   string `json:"message_id"`
	ServerOrder int64  `json:"server_order"`
}

type sendJob struct {
	Sequence       int64
	ConversationID string
	IdempotencyKey string
}

type sendResult struct {
	Job       sendJob
	LatencyMS float64
	MessageID string
	Accepted  bool
	Failure   string
}

type runState struct {
	config                scenarioConfig
	startedAt             time.Time
	usersToken            string
	conversations         []conversationArtifact
	scheduledUnique       int64
	sentAttempts          int64
	acceptedResponses     int64
	latencies             []float64
	sendFailures          map[string]int64
	acceptedByIdem        map[string]string
	acceptedMessageIDs    map[string]struct{}
	duplicateAcceptedKeys map[string]struct{}
	mu                    sync.Mutex
}

func main() {
	var configPath string
	var outputDir string
	flag.StringVar(&configPath, "config", "", "path to scenario config JSON")
	flag.StringVar(&outputDir, "output-dir", "", "override output directory")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		fail(err)
	}
	cfg.RunID = ensureRunID(cfg.RunID)
	if outputDir != "" {
		outputDir = filepath.Clean(outputDir)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	started := time.Now().UTC()
	resultDir := outputDir
	if resultDir == "" {
		resultDir = filepath.Join("benchmarks", "results", started.Format("2006-01-02T15-04-05Z")+"-"+sanitizeName(cfg.Scenario))
	}
	if err := os.MkdirAll(resultDir, 0o755); err != nil {
		fail(fmt.Errorf("create result dir: %w", err))
	}

	cfgPath := filepath.Join(resultDir, "config.json")
	envPath := filepath.Join(resultDir, "env.json")
	summaryJSONPath := filepath.Join(resultDir, "summary.json")
	summaryMDPath := filepath.Join(resultDir, "summary.md")

	if err := writeJSON(cfgPath, cfg); err != nil {
		fail(err)
	}
	env := captureEnv()
	if err := writeJSON(envPath, env); err != nil {
		fail(err)
	}

	client := &http.Client{
		Timeout: time.Duration(cfg.RequestTimeoutSeconds) * time.Second,
	}

	state := &runState{
		config:                cfg,
		startedAt:             started,
		sendFailures:          map[string]int64{},
		acceptedByIdem:        map[string]string{},
		acceptedMessageIDs:    map[string]struct{}{},
		duplicateAcceptedKeys: map[string]struct{}{},
	}

	limitNotes := []string{
		"Client latency is client-observed POST-to-ack latency only, labeled by driver execution location.",
		"Kafka lag reconciliation is not run-scoped in the current schema/tooling; it is consumer-group wide and should be treated as isolated-cluster evidence.",
	}

	cancelled := false
	if err := setupRun(ctx, client, state); err != nil {
		failWithArtifacts(summaryJSONPath, summaryMDPath, state, cancelled, nil, limitNotes, err)
	}

	pgBefore, pgMode, pgErr := postgresCount(ctx, cfg, state.conversations)
	if pgErr != nil {
		limitNotes = append(limitNotes, "Postgres pre-run reconciliation unavailable: "+pgErr.Error())
	}
	cassBefore, cassMode, cassErr := cassandraCount(ctx, cfg, state.conversations)
	if cassErr != nil {
		limitNotes = append(limitNotes, "Cassandra pre-run reconciliation unavailable: "+cassErr.Error())
	}

	if err := executeRun(ctx, client, state); err != nil {
		if errors.Is(err, context.Canceled) {
			cancelled = true
		} else {
			failWithArtifacts(summaryJSONPath, summaryMDPath, state, cancelled, nil, limitNotes, err)
		}
	}

	reconcile := reconcileSummary{
		RunScopedReconcileMode: strings.TrimSpace(strings.Join(filterEmpty([]string{pgMode, cassMode}), "; ")),
		Limitations:            append([]string{}, limitNotes...),
	}
	if reconcile.RunScopedReconcileMode == "" {
		reconcile.RunScopedReconcileMode = "documented limitation: no reconciler succeeded"
	}

	if pgErr == nil {
		reconcile.PostgresBefore = pgBefore
		pgAfter, _, err := postgresCount(ctx, cfg, state.conversations)
		if err != nil {
			reconcile.Limitations = append(reconcile.Limitations, "Postgres post-run reconciliation unavailable: "+err.Error())
		} else {
			reconcile.PostgresAfter = pgAfter
			reconcile.PostgresDelta = pgAfter - pgBefore
		}
	}
	if cassErr == nil {
		reconcile.CassandraBefore = cassBefore
		cassAfter, _, err := cassandraCount(ctx, cfg, state.conversations)
		if err != nil {
			reconcile.Limitations = append(reconcile.Limitations, "Cassandra post-run reconciliation unavailable: "+err.Error())
		} else {
			reconcile.CassandraAfter = cassAfter
			reconcile.CassandraDelta = cassAfter - cassBefore
		}
	}

	lagAtEnd, lagZero, lagSeconds, lagErr := waitForLag(ctx, cfg)
	if lagErr != nil {
		reconcile.Limitations = append(reconcile.Limitations, "Kafka lag reconciliation unavailable: "+lagErr.Error())
	} else {
		reconcile.KafkaLagAtEnd = lagAtEnd
		reconcile.KafkaLagSettledToZero = lagZero
		reconcile.KafkaLagZeroSeconds = lagSeconds
	}

	summary := buildSummary(state, cancelled, reconcile, env, limitNotes, resultDir)
	if err := writeJSON(summaryJSONPath, summary); err != nil {
		fail(err)
	}
	if err := os.WriteFile(summaryMDPath, []byte(renderMarkdown(summary, cfg, env)), 0o644); err != nil {
		fail(err)
	}

	fmt.Printf("results written to %s\n", resultDir)
}

func loadConfig(path string) (scenarioConfig, error) {
	cfg := scenarioConfig{
		Scenario:               "loadgen",
		BaseURL:                "http://127.0.0.1:18080",
		DriverLocation:         "host",
		RatePerSecond:          5,
		DurationSeconds:        10,
		ActiveConversations:    1,
		MessageText:            "hello from loadgen",
		AuthOTPCode:            "123456",
		Namespace:              "ohmf",
		PostgresResource:       "deploy/postgres",
		PostgresUser:           "ohmf",
		PostgresDB:             "ohmf",
		CassandraResource:      "deploy/cassandra",
		CassandraKeyspace:      "ohmf_messages",
		KafkaResource:          "deploy/kafka",
		KafkaConsumerGroup:     "messages-processor-v1",
		KafkaIngressTopic:      "msg.ingress.v1",
		KafkaLagPollSeconds:    2,
		KafkaLagTimeoutSeconds: 60,
		RequestTimeoutSeconds:  15,
	}
	if path == "" {
		return cfg, validateConfig(cfg)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, validateConfig(cfg)
}

func validateConfig(cfg scenarioConfig) error {
	if cfg.Scenario == "" {
		return errors.New("scenario is required")
	}
	if cfg.BaseURL == "" {
		return errors.New("base_url is required")
	}
	if cfg.DriverLocation == "" {
		return errors.New("driver_location is required")
	}
	if cfg.RatePerSecond <= 0 {
		return errors.New("rate_per_second must be > 0")
	}
	if cfg.DurationSeconds <= 0 {
		return errors.New("duration_seconds must be > 0")
	}
	if cfg.ActiveConversations <= 0 {
		return errors.New("active_conversations must be > 0")
	}
	return nil
}

func setupRun(ctx context.Context, client *http.Client, state *runState) error {
	senderPhone := buildPhone(state.config.RunID, 0)
	token, _, err := verifyUser(ctx, client, state.config, senderPhone, "loadgen-sender-"+state.config.RunID)
	if err != nil {
		return fmt.Errorf("verify sender: %w", err)
	}
	state.usersToken = token

	for i := 0; i < state.config.ActiveConversations; i++ {
		phone := buildPhone(state.config.RunID, i+1)
		_, userID, err := verifyUser(ctx, client, state.config, phone, fmt.Sprintf("loadgen-recipient-%02d", i+1))
		if err != nil {
			return fmt.Errorf("verify recipient %d: %w", i+1, err)
		}
		convID, err := createConversation(ctx, client, state.config, token, userID)
		if err != nil {
			return fmt.Errorf("create conversation %d: %w", i+1, err)
		}
		state.conversations = append(state.conversations, conversationArtifact{
			ConversationID: convID,
			RecipientUser:  userID,
		})
	}
	return nil
}

func executeRun(ctx context.Context, client *http.Client, state *runState) error {
	totalUnique := int64(state.config.RatePerSecond * state.config.DurationSeconds)
	state.scheduledUnique = totalUnique

	workers := state.config.RatePerSecond
	if workers < 4 {
		workers = 4
	}
	if workers > 256 {
		workers = 256
	}

	jobs := make(chan sendJob, workers*2)
	results := make(chan sendResult, workers*2)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- doSend(ctx, client, state, job)
			}
		}()
	}

	var collect sync.WaitGroup
	collect.Add(1)
	go func() {
		defer collect.Done()
		for result := range results {
			state.record(result)
		}
	}()

	start := time.Now()
	interval := time.Second / time.Duration(state.config.RatePerSecond)
	var lastIdem string
	for i := int64(0); i < totalUnique; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(results)
			collect.Wait()
			return ctx.Err()
		default:
		}
		target := start.Add(time.Duration(i) * interval)
		if sleep := time.Until(target); sleep > 0 {
			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				close(jobs)
				wg.Wait()
				close(results)
				collect.Wait()
				return ctx.Err()
			case <-timer.C:
			}
		}
		conversationID := state.conversations[i%int64(len(state.conversations))].ConversationID
		idem := fmt.Sprintf("m4-%s-%06d", state.config.RunID, i)
		lastIdem = idem
		jobs <- sendJob{
			Sequence:       i,
			ConversationID: conversationID,
			IdempotencyKey: idem,
		}
		if state.config.DuplicateEvery > 0 && (i+1)%int64(state.config.DuplicateEvery) == 0 {
			jobs <- sendJob{
				Sequence:       i,
				ConversationID: conversationID,
				IdempotencyKey: lastIdem,
			}
		}
	}

	close(jobs)
	wg.Wait()
	close(results)
	collect.Wait()
	return nil
}

func doSend(ctx context.Context, client *http.Client, state *runState, job sendJob) sendResult {
	body := map[string]any{
		"conversation_id":     job.ConversationID,
		"idempotency_key":     job.IdempotencyKey,
		"content_type":        "text",
		"client_generated_id": fmt.Sprintf("%s-%06d", state.config.RunID, job.Sequence),
		"content": map[string]any{
			"text": fmt.Sprintf("%s [%s #%06d]", state.config.MessageText, state.config.RunID, job.Sequence),
		},
	}
	start := time.Now()
	raw, status, err := apiRequest(ctx, client, state.config.BaseURL, http.MethodPost, "/v1/messages", state.usersToken, body)
	latencyMS := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return sendResult{Job: job, Failure: classifyError(err), LatencyMS: latencyMS}
	}
	if status < 200 || status > 299 {
		return sendResult{Job: job, Failure: "http_" + strconv.Itoa(status), LatencyMS: latencyMS}
	}
	var ack messageAck
	if err := json.Unmarshal(raw, &ack); err != nil {
		return sendResult{Job: job, Failure: "decode_ack", LatencyMS: latencyMS}
	}
	if ack.MessageID == "" {
		return sendResult{Job: job, Failure: "empty_message_id", LatencyMS: latencyMS}
	}
	return sendResult{
		Job:       job,
		LatencyMS: latencyMS,
		MessageID: ack.MessageID,
		Accepted:  true,
	}
}

func (s *runState) record(result sendResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sentAttempts++
	if result.Accepted {
		s.acceptedResponses++
		s.latencies = append(s.latencies, result.LatencyMS)
		if prior, ok := s.acceptedByIdem[result.Job.IdempotencyKey]; ok {
			s.duplicateAcceptedKeys[result.Job.IdempotencyKey] = struct{}{}
			if prior != result.MessageID {
				s.sendFailures["duplicate_idempotency_mismatch"]++
			}
			return
		}
		s.acceptedByIdem[result.Job.IdempotencyKey] = result.MessageID
		s.acceptedMessageIDs[result.MessageID] = struct{}{}
		return
	}
	s.sendFailures[result.Failure]++
}

func buildSummary(state *runState, cancelled bool, reconcile reconcileSummary, env envCapture, limitNotes []string, resultDir string) runArtifacts {
	state.mu.Lock()
	latencies := append([]float64(nil), state.latencies...)
	sendFailures := cloneMap(state.sendFailures)
	acceptedByIdem := len(state.acceptedByIdem)
	duplicateKeys := len(state.duplicateAcceptedKeys)
	acceptedMessageIDs := len(state.acceptedMessageIDs)
	conversations := append([]conversationArtifact(nil), state.conversations...)
	sentAttempts := state.sentAttempts
	acceptedResponses := state.acceptedResponses
	state.mu.Unlock()

	sort.Float64s(latencies)
	missing := int64(0)
	if reconcile.PostgresDelta > 0 || acceptedByIdem > 0 {
		missing = int64(acceptedByIdem) - reconcile.PostgresDelta
	}
	counts := countSummary{
		Requested:               state.scheduledUnique,
		SentAttempts:            sentAttempts,
		Accepted:                int64(acceptedByIdem),
		AcceptedResponses:       acceptedResponses,
		SendFailures:            sendFailures,
		Duplicates:              int64(duplicateKeys),
		Missing:                 missing,
		UniqueIdempotencyKeys:   int64(acceptedByIdem),
		UniqueAcceptedMessageID: int64(acceptedMessageIDs),
	}
	latency := latencySummary{
		SampleCount: len(latencies),
		P50MS:       percentile(latencies, 50),
		P95MS:       percentile(latencies, 95),
		P99MS:       percentile(latencies, 99),
		Location:    state.config.DriverLocation,
	}
	limitations := append([]string{}, reconcile.Limitations...)
	for _, note := range limitNotes {
		if !contains(limitations, note) {
			limitations = append(limitations, note)
		}
	}
	if strings.TrimSpace(reconcile.RunScopedReconcileMode) == "" {
		reconcile.RunScopedReconcileMode = "fresh-conversation counts fallback only"
	}
	return runArtifacts{
		Scenario:      state.config.Scenario,
		RunID:         state.config.RunID,
		ResultDir:     resultDir,
		DriverMode:    state.config.DriverLocation,
		CompletedUTC:  time.Now().UTC().Format(time.RFC3339),
		Cancelled:     cancelled,
		Counts:        counts,
		Latency:       latency,
		Reconcile:     reconcile,
		Conversations: conversations,
		Limitations:   limitations,
		Supported: fmt.Sprintf(
			"This artifact supports only a local %s benchmark claim for scenario %q at run_id %q, with client-observed accept latency and run-scoped Postgres/Cassandra reconciliation by fresh test conversation where available.",
			state.config.DriverLocation,
			state.config.Scenario,
			state.config.RunID,
		),
	}
}

func renderMarkdown(summary runArtifacts, cfg scenarioConfig, env envCapture) string {
	var b strings.Builder
	b.WriteString("# " + cfg.Scenario + "\n\n")
	b.WriteString("## Run metadata\n\n")
	b.WriteString("- Run ID: `" + summary.RunID + "`\n")
	b.WriteString("- Driver location: `" + summary.DriverMode + "`\n")
	b.WriteString("- Completed UTC: `" + summary.CompletedUTC + "`\n")
	b.WriteString("- Git HEAD: `" + env.GitHead + "`\n")
	if env.WorkingTreeStatus != "" {
		b.WriteString("- Working tree at generation: `" + env.WorkingTreeStatus + "`\n")
	}
	if env.GitHeadNote != "" {
		b.WriteString("- Git HEAD note: " + env.GitHeadNote + "\n")
	}
	b.WriteString("- Host: `" + env.Machine.Hostname + "` on `" + env.Machine.OS + "/" + env.Machine.Arch + "`\n")
	b.WriteString("\n## Counts\n\n")
	b.WriteString(fmt.Sprintf("- Requested unique sends: `%d`\n", summary.Counts.Requested))
	b.WriteString(fmt.Sprintf("- Sent attempts: `%d`\n", summary.Counts.SentAttempts))
	b.WriteString(fmt.Sprintf("- Accepted unique idempotency keys: `%d`\n", summary.Counts.Accepted))
	b.WriteString(fmt.Sprintf("- Accepted responses: `%d`\n", summary.Counts.AcceptedResponses))
	b.WriteString(fmt.Sprintf("- Duplicate accepted keys: `%d`\n", summary.Counts.Duplicates))
	b.WriteString(fmt.Sprintf("- Missing (`accepted - pg_delta`): `%d`\n", summary.Counts.Missing))
	if len(summary.Counts.SendFailures) == 0 {
		b.WriteString("- Send failures: none\n")
	} else {
		keys := sortedKeys(summary.Counts.SendFailures)
		for _, key := range keys {
			b.WriteString(fmt.Sprintf("- Send failure `%s`: `%d`\n", key, summary.Counts.SendFailures[key]))
		}
	}
	b.WriteString("\n## Latency\n\n")
	b.WriteString(fmt.Sprintf("- Samples: `%d`\n", summary.Latency.SampleCount))
	b.WriteString(fmt.Sprintf("- p50 accept latency: `%.2f ms`\n", summary.Latency.P50MS))
	b.WriteString(fmt.Sprintf("- p95 accept latency: `%.2f ms`\n", summary.Latency.P95MS))
	b.WriteString(fmt.Sprintf("- p99 accept latency: `%.2f ms`\n", summary.Latency.P99MS))
	b.WriteString("\n## Reconciliation\n\n")
	b.WriteString("- Mode: `" + summary.Reconcile.RunScopedReconcileMode + "`\n")
	b.WriteString(fmt.Sprintf("- Postgres delta: `%d`\n", summary.Reconcile.PostgresDelta))
	b.WriteString(fmt.Sprintf("- Cassandra delta: `%d`\n", summary.Reconcile.CassandraDelta))
	if summary.Reconcile.KafkaLagSettledToZero {
		b.WriteString(fmt.Sprintf("- Kafka lag reached zero in `%.2f s`\n", summary.Reconcile.KafkaLagZeroSeconds))
	} else {
		b.WriteString(fmt.Sprintf("- Kafka lag at timeout/end: `%d`\n", summary.Reconcile.KafkaLagAtEnd))
	}
	b.WriteString("\n## Limitations\n\n")
	for _, limitation := range summary.Limitations {
		b.WriteString("- " + limitation + "\n")
	}
	b.WriteString("\n## Supported claim\n\n")
	b.WriteString(summary.Supported + "\n")
	return b.String()
}

func verifyUser(ctx context.Context, client *http.Client, cfg scenarioConfig, phone, deviceName string) (string, string, error) {
	raw, status, err := apiRequest(ctx, client, cfg.BaseURL, http.MethodPost, "/v1/auth/phone/start", "", map[string]any{
		"phone_e164": phone,
		"channel":    "SMS",
	})
	if err != nil {
		return "", "", err
	}
	if status < 200 || status > 299 {
		return "", "", fmt.Errorf("auth start status %d", status)
	}
	var start authStartResponse
	if err := json.Unmarshal(raw, &start); err != nil {
		return "", "", err
	}
	raw, status, err = apiRequest(ctx, client, cfg.BaseURL, http.MethodPost, "/v1/auth/phone/verify", "", map[string]any{
		"challenge_id": start.ChallengeID,
		"otp_code":     cfg.AuthOTPCode,
		"device": map[string]any{
			"platform":    "WEB",
			"device_name": deviceName,
		},
	})
	if err != nil {
		return "", "", err
	}
	if status < 200 || status > 299 {
		return "", "", fmt.Errorf("auth verify status %d", status)
	}
	var verify authVerifyResponse
	if err := json.Unmarshal(raw, &verify); err != nil {
		return "", "", err
	}
	return verify.Tokens.AccessToken, verify.User.UserID, nil
}

func createConversation(ctx context.Context, client *http.Client, cfg scenarioConfig, token, participant string) (string, error) {
	raw, status, err := apiRequest(ctx, client, cfg.BaseURL, http.MethodPost, "/v1/conversations", token, map[string]any{
		"type":         "DM",
		"participants": []string{participant},
	})
	if err != nil {
		return "", err
	}
	if status < 200 || status > 299 {
		return "", fmt.Errorf("create conversation status %d", status)
	}
	var resp conversationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}
	return resp.ConversationID, nil
}

func apiRequest(ctx context.Context, client *http.Client, baseURL, method, path, token string, body any) ([]byte, int, error) {
	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, payload)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return raw, resp.StatusCode, err
}

func postgresCount(ctx context.Context, cfg scenarioConfig, conversations []conversationArtifact) (int64, string, error) {
	if len(conversations) == 0 {
		return 0, "", nil
	}
	var ids []string
	for _, conv := range conversations {
		ids = append(ids, "'"+conv.ConversationID+"'::uuid")
	}
	query := "SELECT COUNT(*) FROM messages WHERE conversation_id IN (" + strings.Join(ids, ",") + ");"
	cmd := []string{
		"-n", cfg.Namespace, "exec", cfg.PostgresResource, "--", "sh", "-lc",
		fmt.Sprintf("psql -U %s -d %s -t -A -c %q", cfg.PostgresUser, cfg.PostgresDB, query),
	}
	out, err := runCommand(ctx, "kubectl", cmd...)
	if err != nil {
		return 0, "", err
	}
	count, err := parseTrailingInt(out)
	if err != nil {
		return 0, "", err
	}
	return count, "Postgres reconciled by fresh test conversation ids derived from run_id", nil
}

func cassandraCount(ctx context.Context, cfg scenarioConfig, conversations []conversationArtifact) (int64, string, error) {
	if len(conversations) == 0 {
		return 0, "", nil
	}
	bucket := time.Now().UTC().Format("20060102")
	var total int64
	for _, conv := range conversations {
		query := fmt.Sprintf("CONSISTENCY ONE; SELECT COUNT(*) FROM %s.messages_by_conversation WHERE conversation_id = %s AND bucket_yyyymmdd = '%s';", cfg.CassandraKeyspace, conv.ConversationID, bucket)
		out, err := runCommand(ctx, "kubectl", "-n", cfg.Namespace, "exec", cfg.CassandraResource, "--", "sh", "-lc", "/opt/cassandra/bin/cqlsh -e "+shellQuote(query))
		if err != nil {
			return 0, "", err
		}
		count, err := parseTrailingInt(out)
		if err != nil {
			return 0, "", err
		}
		total += count
	}
	return total, "Cassandra reconciled by fresh test conversation ids plus UTC partition bucket; there is no dedicated persisted run_id column", nil
}

func waitForLag(ctx context.Context, cfg scenarioConfig) (int64, bool, float64, error) {
	start := time.Now()
	timeout := time.Duration(cfg.KafkaLagTimeoutSeconds) * time.Second
	poll := time.Duration(cfg.KafkaLagPollSeconds) * time.Second
	if poll <= 0 {
		poll = 2 * time.Second
	}
	for {
		lag, err := kafkaLag(ctx, cfg)
		if err != nil {
			return 0, false, 0, err
		}
		if lag == 0 {
			return 0, true, time.Since(start).Seconds(), nil
		}
		if time.Since(start) >= timeout {
			return lag, false, time.Since(start).Seconds(), nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lag, false, time.Since(start).Seconds(), ctx.Err()
		case <-timer.C:
		}
	}
}

func kafkaLag(ctx context.Context, cfg scenarioConfig) (int64, error) {
	command := fmt.Sprintf("/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group %s", cfg.KafkaConsumerGroup)
	out, err := runCommand(ctx, "kubectl", "-n", cfg.Namespace, "exec", cfg.KafkaResource, "--", "sh", "-lc", command)
	if err != nil {
		return 0, err
	}
	var total int64
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		if fields[0] != cfg.KafkaConsumerGroup || fields[1] != cfg.KafkaIngressTopic {
			continue
		}
		lagField := fields[5]
		lag, err := strconv.ParseInt(lagField, 10, 64)
		if err != nil {
			continue
		}
		total += lag
	}
	return total, nil
}

func captureEnv() envCapture {
	return envCapture{
		TimestampUTC:      time.Now().UTC().Format(time.RFC3339),
		GitHead:           gitHead(),
		WorkingTreeStatus: gitWorkingTreeStatus(),
		GitHeadNote:       "git_head is the base commit visible at artifact generation time; artifact changes may be uncommitted until the validation commit lands",
		Kubectl:           captureCommand("kubectl", "version", "--client"),
		Kubernetes:        captureCommand("kubectl", "version"),
		K3d:               captureCommand("k3d", "version"),
		Machine: machineCapture{
			Hostname: hostOrUnknown(),
			OS:       runtime.GOOS,
			Arch:     runtime.GOARCH,
			CPUs:     runtime.NumCPU(),
			Go:       runtime.Version(),
		},
		Commands: map[string]string{
			"git_head":           "git rev-parse HEAD",
			"git_status_short":   "git status --short",
			"kubectl_client":     "kubectl version --client",
			"kubernetes_version": "kubectl version",
			"k3d_version":        "k3d version",
		},
	}
}

func captureCommand(name string, args ...string) commandCapture {
	resolved := resolveCommandName(name)
	out, err := runCommandNoContext(resolved, args...)
	capture := commandCapture{
		Command: resolved + " " + strings.Join(args, " "),
	}
	if err != nil {
		capture.Error = err.Error()
		capture.Output = strings.TrimSpace(out)
		return capture
	}
	capture.Output = strings.TrimSpace(out)
	return capture
}

func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return string(raw), fmt.Errorf("%s %s: %w; output=%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(raw)))
	}
	return string(raw), nil
}

func runCommandNoContext(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	raw, err := cmd.CombinedOutput()
	return string(raw), err
}

func resolveCommandName(name string) string {
	if _, err := exec.LookPath(name); err == nil {
		return name
	}
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		if resolved := resolveCommandName(name + ".exe"); resolved != name+".exe" {
			return resolved
		}
	}
	if goBin := os.Getenv("GOBIN"); goBin != "" {
		candidate := filepath.Join(goBin, name)
		if fileExists(candidate) {
			return candidate
		}
	}
	if goPath := os.Getenv("GOPATH"); goPath != "" {
		candidate := filepath.Join(goPath, "bin", name)
		if fileExists(candidate) {
			return candidate
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidate := filepath.Join(home, "go", "bin", name)
		if fileExists(candidate) {
			return candidate
		}
	}
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		if goBin := os.Getenv("GOBIN"); goBin != "" {
			candidate := filepath.Join(goBin, name+".exe")
			if fileExists(candidate) {
				return candidate
			}
		}
		if goPath := os.Getenv("GOPATH"); goPath != "" {
			candidate := filepath.Join(goPath, "bin", name+".exe")
			if fileExists(candidate) {
				return candidate
			}
		}
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			candidate := filepath.Join(home, "go", "bin", name+".exe")
			if fileExists(candidate) {
				return candidate
			}
		}
	}
	return name
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func parseTrailingInt(raw string) (int64, error) {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		line = strings.Trim(line, "()")
		value, err := strconv.ParseInt(line, 10, 64)
		if err == nil {
			return value, nil
		}
	}
	return 0, fmt.Errorf("no integer found in output: %q", raw)
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	pos := (p / 100.0) * float64(len(values)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return values[lower]
	}
	fraction := pos - float64(lower)
	return values[lower] + fraction*(values[upper]-values[lower])
}

func classifyError(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "no such host"):
		return "dns"
	default:
		return "transport_error"
	}
}

func ensureRunID(input string) string {
	if input != "" {
		return input
	}
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return time.Now().UTC().Format("20060102t150405.000000000z")
	}
	return time.Now().UTC().Format("20060102t150405z") + "-" + hex.EncodeToString(buf[:])
}

func buildPhone(runID string, index int) string {
	digits := digitsOnly(runID)
	if len(digits) < 6 {
		digits += "000000"
	}
	return fmt.Sprintf("+1555%s%02d", digits[len(digits)-6:], index)
}

func digitsOnly(input string) string {
	var b strings.Builder
	for _, r := range input {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func sanitizeName(input string) string {
	input = strings.ToLower(strings.TrimSpace(input))
	input = strings.ReplaceAll(input, " ", "-")
	input = strings.ReplaceAll(input, "/", "-")
	input = strings.ReplaceAll(input, "\\", "-")
	return input
}

func shellQuote(input string) string {
	return "'" + strings.ReplaceAll(input, "'", "'\"'\"'") + "'"
}

func hostOrUnknown() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown"
	}
	return host
}

func cloneMap(input map[string]int64) map[string]int64 {
	if len(input) == 0 {
		return map[string]int64{}
	}
	out := make(map[string]int64, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func sortedKeys(input map[string]int64) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func filterEmpty(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			out = append(out, item)
		}
	}
	return out
}

func firstLine(raw string) string {
	if idx := strings.IndexByte(raw, '\n'); idx >= 0 {
		return raw[:idx]
	}
	return raw
}

func gitHead() string {
	out, err := runCommandNoContext("git", "rev-parse", "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(firstLine(out))
}

func gitWorkingTreeStatus() string {
	out, err := runCommandNoContext("git", "status", "--short")
	if err != nil {
		return ""
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return "clean"
	}
	return "dirty: " + strings.ReplaceAll(trimmed, "\n", "; ")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func failWithArtifacts(summaryJSONPath, summaryMDPath string, state *runState, cancelled bool, reconcile *reconcileSummary, limitNotes []string, root error) {
	rec := reconcileSummary{}
	if reconcile != nil {
		rec = *reconcile
	}
	rec.Limitations = append(rec.Limitations, "run terminated early: "+root.Error())
	summary := buildSummary(state, cancelled, rec, captureEnv(), limitNotes, filepath.Dir(summaryJSONPath))
	_ = writeJSON(summaryJSONPath, summary)
	_ = os.WriteFile(summaryMDPath, []byte(renderMarkdown(summary, state.config, captureEnv())), 0o644)
	fail(root)
}
