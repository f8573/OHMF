package main

import "time"

type config struct {
	mode            string
	profile         string
	baseURL         string
	hostBaseURL     string
	composeFile     string
	listenAddr      string
	controllerURL   string
	agentID         string
	sharedSecret    string
	agentShare      string
	initialDevices  int
	maxDevices      int
	proveDuration   time.Duration
	sustainDuration time.Duration
	warmupDuration  time.Duration
	fixtureDir      string
	connectWaveSize int
	connectWaveWait time.Duration
	readyThreshold  float64
	artifactRoot    string
	runID           string
	runDir          string
	seed            int64
}

type summary struct {
	Workload      workloadSummary      `json:"workload"`
	Performance   performanceSummary   `json:"performance"`
	Correctness   correctnessSummary   `json:"correctness"`
	Resilience    resilienceSummary    `json:"resilience"`
	ResourceUsage resourceUsageSummary `json:"resource_usage"`
}

type workloadSummary struct {
	ConcurrentUsers         int     `json:"concurrent users"`
	ConcurrentDevices       int     `json:"concurrent devices"`
	Conversations           int     `json:"conversations"`
	AverageRecipientsPerMsg float64 `json:"average recipients/message"`
	MessageRate             float64 `json:"message rate"`
	TestDuration            string  `json:"test duration"`
	ConnectReadinessPercent float64 `json:"connect readiness percentage"`
	ConnectPhaseDuration    string  `json:"connect phase duration"`
	FirstFailurePhase       string  `json:"first failure phase"`
	FirstFailureReason      string  `json:"first failure reason"`
}

type performanceSummary struct {
	MessagesPerSecond          float64 `json:"messages/sec"`
	DeliveriesPerSecond        float64 `json:"deliveries/sec"`
	P50SendToAckLatency        string  `json:"p50 send-to-ack latency"`
	P95SendToAckLatency        string  `json:"p95 send-to-ack latency"`
	P99SendToAckLatency        string  `json:"p99 send-to-ack latency"`
	P50EndToEndDeliveryLatency string  `json:"p50 end-to-end delivery latency"`
	P95EndToEndDeliveryLatency string  `json:"p95 end-to-end delivery latency"`
	P99EndToEndDeliveryLatency string  `json:"p99 end-to-end delivery latency"`
}

type correctnessSummary struct {
	ExpectedDeliveries         int64   `json:"expected deliveries"`
	SuccessfulDeliveries       int64   `json:"successful deliveries"`
	LostDeliveries             int64   `json:"lost deliveries"`
	DuplicateDeliveries        int64   `json:"duplicate deliveries"`
	OutOfOrderDeliveries       int64   `json:"out-of-order deliveries"`
	MultiDeviceConvergenceRate float64 `json:"multi-device convergence rate"`
}

type resilienceSummary struct {
	ReconnectSuccessRate float64 `json:"reconnect success rate"`
	RestartRecoveryTime  string  `json:"restart recovery time"`
	BacklogDrainTime     string  `json:"backlog drain time"`
}

type resourceUsageSummary struct {
	PeakCPU      string `json:"peak CPU"`
	PeakMemory   string `json:"peak memory"`
	DBP95Latency string `json:"DB p95 latency"`
	PeakKafkaLag int64  `json:"peak Kafka lag"`
}

type rawEvent struct {
	Timestamp      time.Time      `json:"timestamp"`
	Type           string         `json:"type"`
	Phase          string         `json:"phase,omitempty"`
	UserID         string         `json:"user_id,omitempty"`
	DeviceID       string         `json:"device_id,omitempty"`
	ConversationID string         `json:"conversation_id,omitempty"`
	MessageID      string         `json:"message_id,omitempty"`
	ServerOrder    int64          `json:"server_order,omitempty"`
	UserEventID    int64          `json:"user_event_id,omitempty"`
	Outcome        string         `json:"outcome,omitempty"`
	ReconnectID    string         `json:"reconnect_id,omitempty"`
	Value          any            `json:"value,omitempty"`
	Details        map[string]any `json:"details,omitempty"`
}

type sendRecord struct {
	MessageID       string
	ConversationID  string
	SenderUserID    string
	SenderDeviceID  string
	ClientSendAt    time.Time
	AckAt           time.Time
	ServerOrder     int64
	TargetDeviceIDs []string
	TargetCount     int
}

type receiveRecord struct {
	MessageID      string
	ConversationID string
	DeviceID       string
	ReceivedAt     time.Time
	ServerOrder    int64
}

type reconnectRecord struct {
	ID         string
	DeviceID   string
	StartedAt  time.Time
	ReadyAt    time.Time
	SyncAt     time.Time
	Successful bool
}

type convergenceResult struct {
	Successful int
	Total      int
}

type resourceSnapshot struct {
	Timestamp    time.Time
	StackCPU     float64
	StackMemory  float64
	PeakKafkaLag int64
}

type scaleResult struct {
	targetDevices int
	passed        bool
	summary       summary
}

type phaseFailure struct {
	Phase  string
	Reason string
	Err    error
}

type fixture struct {
	CreatedAt     time.Time             `json:"created_at"`
	TargetDevices int                   `json:"target_devices"`
	Users         []fixtureUser         `json:"users"`
	Conversations []fixtureConversation `json:"conversations"`
}

type fixtureUser struct {
	UserID    string          `json:"user_id"`
	PhoneE164 string          `json:"phone_e164"`
	Devices   []fixtureDevice `json:"devices"`
}

type fixtureDevice struct {
	UserID       string `json:"user_id"`
	DeviceID     string `json:"device_id"`
	PhoneE164    string `json:"phone_e164"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type fixtureConversation struct {
	ID                 string   `json:"id"`
	ParticipantUserIDs []string `json:"participant_user_ids"`
}

type connectResult struct {
	ReadyCount         int
	AttemptedCount     int
	ConnectDuration    time.Duration
	FirstFailurePhase  string
	FirstFailureReason string
}

type scenarioOutcome struct {
	Label         string
	Summary       summary
	Profile       string
	TargetDevices int
	Passed        bool
	Failure       phaseFailure
	RunDir        string
}

type campaignTierSummary struct {
	Label               string  `json:"label"`
	Profile             string  `json:"profile"`
	TargetDevices       int     `json:"target_devices"`
	ActualDevices       int     `json:"actual_devices"`
	Passed              bool    `json:"passed"`
	ConnectReadiness    float64 `json:"connect_readiness_percentage"`
	FirstFailurePhase   string  `json:"first_failure_phase"`
	FirstFailureReason  string  `json:"first_failure_reason"`
	MessagesPerSecond   float64 `json:"messages_per_second"`
	DeliveriesPerSecond float64 `json:"deliveries_per_second"`
	ReportPath          string  `json:"report_path"`
}

type campaignSummary struct {
	ActiveHeadline   *summary              `json:"active_headline,omitempty"`
	ConnectedCeiling *summary              `json:"connected_ceiling,omitempty"`
	Resilience       *summary              `json:"resilience,omitempty"`
	FirstBottleneck  string                `json:"first_bottleneck"`
	Tiers            []campaignTierSummary `json:"tiers"`
}

type agentConversationAssignment struct {
	ID                   string   `json:"id"`
	AllDeviceIDs         []string `json:"all_device_ids"`
	LocalSenderDeviceIDs []string `json:"local_sender_device_ids"`
}

type controllerAssignment struct {
	RunID                 string                        `json:"run_id"`
	Phase                 string                        `json:"phase"`
	Profile               string                        `json:"profile"`
	BaseURL               string                        `json:"base_url"`
	ConnectWaveSize       int                           `json:"connect_wave_size"`
	ConnectWaveIntervalMS int64                         `json:"connect_wave_interval_ms"`
	WarmupDurationMS      int64                         `json:"warmup_duration_ms"`
	HoldDurationMS        int64                         `json:"hold_duration_ms"`
	ReadyThreshold        float64                       `json:"ready_threshold"`
	Users                 []fixtureUser                 `json:"users"`
	Conversations         []agentConversationAssignment `json:"conversations"`
}

type agentRegisterRequest struct {
	AgentID      string `json:"agent_id"`
	SharedSecret string `json:"shared_secret"`
}

type agentRegisterResponse struct {
	AgentID        string `json:"agent_id"`
	ServerUnixNano int64  `json:"server_unix_nano"`
}

type agentEventsUpload struct {
	AgentID      string     `json:"agent_id"`
	SharedSecret string     `json:"shared_secret"`
	Events       []rawEvent `json:"events"`
}

type agentPhaseComplete struct {
	AgentID            string `json:"agent_id"`
	SharedSecret       string `json:"shared_secret"`
	Phase              string `json:"phase"`
	Succeeded          bool   `json:"succeeded"`
	AttemptedDevices   int    `json:"attempted_devices"`
	ReadyDevices       int    `json:"ready_devices"`
	ConnectDurationMS  int64  `json:"connect_duration_ms"`
	MeasurementStartNS int64  `json:"measurement_start_unix_nano,omitempty"`
	MeasurementEndNS   int64  `json:"measurement_end_unix_nano,omitempty"`
	FirstFailurePhase  string `json:"first_failure_phase,omitempty"`
	FirstFailureReason string `json:"first_failure_reason,omitempty"`
}
