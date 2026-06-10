package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const hostAgentID = "host"

type controllerState struct {
	cfg              config
	assignments      map[string]controllerAssignment
	selectedDevices  int
	selectedUsers    int
	selectedConvs    int
	reducer          *reducer
	eventsWriter     *jsonlWriter
	sampler          *resourceSampler
	mu               sync.Mutex
	phaseCompletions map[string]agentPhaseComplete
	done             chan struct{}
	closed           bool
}

func runController(ctx context.Context, cfg config) error {
	eventsWriter, err := newJSONLWriter(filepathJoin(cfg.runDir, "events.jsonl"))
	if err != nil {
		return err
	}
	defer eventsWriter.close()

	reducer := newReducer()
	sampler, err := newResourceSampler(cfg.runDir, reducer)
	if err != nil {
		return err
	}
	samplerCtx, samplerCancel := context.WithCancel(ctx)
	defer samplerCancel()
	sampler.start(samplerCtx)

	fixtureTarget := maxInt(cfg.initialDevices, cfg.maxDevices)
	if fixtureTarget <= 0 {
		fixtureTarget = cfg.initialDevices
	}
	fx, err := loadOrProvisionFixture(ctx, cfg, eventsWriter, fixtureTarget)
	if err != nil {
		return err
	}

	assignments, users, devices, conversations, err := buildControllerAssignments(cfg, fx)
	if err != nil {
		return err
	}
	state := &controllerState{
		cfg:              cfg,
		assignments:      assignments,
		selectedDevices:  devices,
		selectedUsers:    users,
		selectedConvs:    conversations,
		reducer:          reducer,
		eventsWriter:     eventsWriter,
		sampler:          sampler,
		phaseCompletions: make(map[string]agentPhaseComplete),
		done:             make(chan struct{}),
	}

	server := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           state.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- server.ListenAndServe()
	}()
	defer server.Shutdown(context.Background())

	if hostAssignment, ok := assignments[hostAgentID]; ok && len(hostAssignment.Users) > 0 {
		go state.runHostAssignment(ctx, hostAssignment)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-serverErrCh:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
	case <-state.done:
	}

	convergenceRate, err := measureConvergenceForAssignments(ctx, cfg, fx, assignments)
	if err != nil {
		return err
	}
	dbP95 := maxPrometheusDuration(
		ctx,
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_gateway_db_query_latency_seconds_bucket[5m])))",
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_messages_processor_db_query_latency_seconds_bucket[5m])))",
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_delivery_processor_db_query_latency_seconds_bucket[5m])))",
	)
	peakCPU, peakMemory, peakKafkaLag := sampler.resourcePeaks()
	connectReady, connectAttempted, connectDuration, measurementStart, measurementEnd, failurePhase, failureReason := state.aggregatePhaseResults()
	duration := time.Duration(0)
	if !measurementStart.IsZero() && !measurementEnd.IsZero() && measurementEnd.After(measurementStart) {
		duration = measurementEnd.Sub(measurementStart)
	}
	result := reducer.snapshotSummary(summaryInput{
		concurrentUsers:      users,
		concurrentDevices:    devices,
		conversations:        conversations,
		duration:             duration,
		measurementStart:     measurementStart,
		measurementEnd:       measurementEnd,
		messageRate:          scenarioMessageRate(scenarioPlan{enableTraffic: profileFromConfig(cfg) != profileConnectedIdle}, conversations),
		connectReadiness:     rate(connectReady, connectAttempted),
		connectPhaseDuration: connectDuration,
		firstFailurePhase:    failurePhase,
		firstFailureReason:   failureReason,
		convergenceRate:      convergenceRate,
		dbP95Latency:         dbP95,
		peakKafkaLag:         maxInt64(sampler.currentLagTotal(), peakKafkaLag),
		peakCPU:              peakCPU,
		peakMemory:           peakMemory,
	})
	return writeSummaryFiles(cfg.runDir, result)
}

func runAgent(ctx context.Context, cfg config) error {
	if strings.TrimSpace(cfg.controllerURL) == "" {
		return fmt.Errorf("controller-url is required in agent mode")
	}
	if strings.TrimSpace(cfg.agentID) == "" {
		return fmt.Errorf("agent-id is required in agent mode")
	}
	registerReq := agentRegisterRequest{AgentID: cfg.agentID, SharedSecret: cfg.sharedSecret}
	if err := postAgentJSON(ctx, cfg, http.MethodPost, "/agents/register", registerReq, nil); err != nil {
		return err
	}

	offset := sampleClockOffset(ctx, cfg)
	var assignment controllerAssignment
	if err := getAgentJSON(ctx, cfg, "/agents/"+cfg.agentID+"/assignment", &assignment); err != nil {
		return err
	}

	eventsWriter, err := newJSONLWriter(filepathJoin(cfg.runDir, "events.jsonl"))
	if err != nil {
		return err
	}
	defer eventsWriter.close()

	uploader := &agentEventUploader{
		cfg:    cfg,
		offset: offset,
	}
	eventsWriter.onRawEvent = uploader.enqueue
	defer uploader.flush(context.Background())

	tickerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go uploader.flushLoop(tickerCtx)
	go uploader.refreshOffsetLoop(tickerCtx)

	result, err := runDistributedAssignment(ctx, cfg, assignment, eventsWriter)
	if err != nil {
		return err
	}
	if err := uploader.flush(ctx); err != nil {
		return err
	}
	return postAgentJSON(ctx, cfg, http.MethodPost, "/agents/"+cfg.agentID+"/phase-complete", result, nil)
}

type agentEventUploader struct {
	cfg    config
	offset time.Duration
	mu     sync.Mutex
	events []rawEvent
}

func (u *agentEventUploader) enqueue(event rawEvent) {
	event.Timestamp = event.Timestamp.Add(u.offset)
	u.mu.Lock()
	u.events = append(u.events, event)
	u.mu.Unlock()
}

func (u *agentEventUploader) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = u.flush(ctx)
		}
	}
}

func (u *agentEventUploader) refreshOffsetLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			offset := sampleClockOffset(ctx, u.cfg)
			u.mu.Lock()
			u.offset = offset
			u.mu.Unlock()
		}
	}
}

func (u *agentEventUploader) flush(ctx context.Context) error {
	u.mu.Lock()
	pending := append([]rawEvent(nil), u.events...)
	u.events = nil
	u.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	if err := postAgentJSON(ctx, u.cfg, http.MethodPost, "/agents/"+u.cfg.agentID+"/events", agentEventsUpload{
		AgentID:      u.cfg.agentID,
		SharedSecret: u.cfg.sharedSecret,
		Events:       pending,
	}, nil); err != nil {
		u.mu.Lock()
		u.events = append(pending, u.events...)
		u.mu.Unlock()
		return err
	}
	return nil
}

func (c *controllerState) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/agents/register", c.handleRegister)
	mux.HandleFunc("/agents/", c.handleAgentRoutes)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (c *controllerState) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req agentRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !c.authorize(req.AgentID, req.SharedSecret) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	writeJSON(w, agentRegisterResponse{AgentID: req.AgentID, ServerUnixNano: time.Now().UTC().UnixNano()})
}

func (c *controllerState) handleAgentRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/agents/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	agentID := parts[0]
	action := parts[1]
	if !c.authorize(agentID, r.Header.Get("X-OHMF-Load-Secret")) && r.Method == http.MethodGet {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	switch action {
	case "assignment":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		assign, ok := c.assignments[agentID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, assign)
	case "timesync":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, agentRegisterResponse{AgentID: agentID, ServerUnixNano: time.Now().UTC().UnixNano()})
	case "events":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req agentEventsUpload
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !c.authorize(agentID, req.SharedSecret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		for _, event := range req.Events {
			writeRawEvent(c.eventsWriter, event)
			ingestRawEvent(c.reducer, event)
		}
		writeJSON(w, map[string]any{"accepted": len(req.Events)})
	case "phase-complete":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req agentPhaseComplete
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !c.authorize(agentID, req.SharedSecret) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		c.mu.Lock()
		c.phaseCompletions[agentID] = req
		allDone := true
		for expectedID, assign := range c.assignments {
			if expectedID == hostAgentID && len(assign.Users) == 0 {
				continue
			}
			if _, ok := c.phaseCompletions[expectedID]; !ok {
				allDone = false
				break
			}
		}
		if allDone && !c.closed {
			c.closed = true
			close(c.done)
		}
		c.mu.Unlock()
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.NotFound(w, r)
	}
}

func (c *controllerState) authorize(agentID, secret string) bool {
	assign, ok := c.assignments[agentID]
	if !ok {
		return false
	}
	if agentID == hostAgentID && len(assign.Users) > 0 {
		return true
	}
	expected := strings.TrimSpace(c.cfg.sharedSecret)
	if expected == "" {
		expected = strings.TrimSpace(secret)
	}
	return expected == "" || secret == expected
}

func (c *controllerState) runHostAssignment(ctx context.Context, assignment controllerAssignment) {
	writer, err := newJSONLWriter(filepathJoin(c.cfg.runDir, "host_events.jsonl"))
	if err != nil {
		return
	}
	defer writer.close()
	writer.onRawEvent = func(event rawEvent) {
		writeRawEvent(c.eventsWriter, event)
		ingestRawEvent(c.reducer, event)
	}
	result, err := runDistributedAssignment(ctx, c.cfg, assignment, writer)
	if err != nil {
		result = agentPhaseComplete{
			AgentID:            hostAgentID,
			Phase:              assignment.Phase,
			Succeeded:          false,
			AttemptedDevices:   len(assignment.Users),
			FirstFailurePhase:  "hold",
			FirstFailureReason: err.Error(),
		}
	}
	c.mu.Lock()
	c.phaseCompletions[hostAgentID] = result
	allDone := true
	for expectedID, assign := range c.assignments {
		if expectedID == hostAgentID && len(assign.Users) == 0 {
			continue
		}
		if _, ok := c.phaseCompletions[expectedID]; !ok {
			allDone = false
			break
		}
	}
	if allDone && !c.closed {
		c.closed = true
		close(c.done)
	}
	c.mu.Unlock()
}

func (c *controllerState) aggregatePhaseResults() (int, int, time.Duration, time.Time, time.Time, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ready := 0
	attempted := 0
	var connectDuration time.Duration
	var start time.Time
	var end time.Time
	failurePhase := ""
	failureReason := ""
	for _, result := range c.phaseCompletions {
		ready += result.ReadyDevices
		attempted += result.AttemptedDevices
		if d := time.Duration(result.ConnectDurationMS) * time.Millisecond; d > connectDuration {
			connectDuration = d
		}
		if result.MeasurementStartNS > 0 {
			ts := time.Unix(0, result.MeasurementStartNS).UTC()
			if start.IsZero() || ts.Before(start) {
				start = ts
			}
		}
		if result.MeasurementEndNS > 0 {
			ts := time.Unix(0, result.MeasurementEndNS).UTC()
			if end.IsZero() || ts.After(end) {
				end = ts
			}
		}
		if failureReason == "" && !result.Succeeded {
			failurePhase = result.FirstFailurePhase
			failureReason = result.FirstFailureReason
		}
	}
	return ready, attempted, connectDuration, start, end, failurePhase, failureReason
}

func buildControllerAssignments(cfg config, fx *fixture) (map[string]controllerAssignment, int, int, int, error) {
	shares := parseAgentShares(cfg.agentShare)
	if len(shares) == 0 {
		shares = map[string]float64{hostAgentID: 1}
	}
	indexes, actualDevices := selectConversationIndexesByDeviceTarget(fx, cfg.initialDevices)
	if len(indexes) == 0 {
		return nil, 0, 0, 0, fmt.Errorf("no fixture conversations for %d devices", cfg.initialDevices)
	}
	userSource := make(map[string]fixtureUser, len(fx.Users))
	for _, user := range fx.Users {
		userSource[user.UserID] = user
	}
	assignments := make(map[string]controllerAssignment, len(shares))
	remaining := make(map[string]float64, len(shares))
	agentIDs := make([]string, 0, len(shares))
	for agentID, fraction := range shares {
		assignments[agentID] = controllerAssignment{
			RunID:                 cfg.runID,
			Phase:                 profileFromConfig(cfg),
			Profile:               profileFromConfig(cfg),
			BaseURL:               cfg.hostBaseURL,
			ConnectWaveSize:       connectWaveSizeForTarget(cfg.connectWaveSize, actualDevices),
			ConnectWaveIntervalMS: cfg.connectWaveWait.Milliseconds(),
			WarmupDurationMS:      chooseWarmup(profileFromConfig(cfg), cfg.warmupDuration).Milliseconds(),
			HoldDurationMS:        cfg.sustainDuration.Milliseconds(),
			ReadyThreshold:        chooseConnectThreshold(profileFromConfig(cfg)),
			Users:                 []fixtureUser{},
			Conversations:         []agentConversationAssignment{},
		}
		remaining[agentID] = math.Max(0, fraction) * float64(actualDevices)
		agentIDs = append(agentIDs, agentID)
	}
	sort.Strings(agentIDs)
	placementAgentIDs := positiveShareAgentIDs(agentIDs, shares)
	if len(placementAgentIDs) == 0 {
		placementAgentIDs = append([]string(nil), agentIDs...)
	}
	ownerAgentIDs := append([]string(nil), placementAgentIDs...)

	userByAgent := make(map[string]map[string]*fixtureUser, len(assignments))
	for _, agentID := range agentIDs {
		userByAgent[agentID] = make(map[string]*fixtureUser)
	}

	selectedUsers := map[string]struct{}{}
	ownerCursor := 0
	for _, idx := range indexes {
		stored := fx.Conversations[idx]
		allDeviceIDs := make([]string, 0)
		type deviceOwner struct {
			agentID string
			userID  string
			device  fixtureDevice
		}
		owners := make([]deviceOwner, 0)
		ownerAgentID := ownerAgentIDs[ownerCursor%len(ownerAgentIDs)]
		ownerCursor++
		firstAssignedOwner := false
		localSenderIDs := []string{}
		for _, userID := range stored.ParticipantUserIDs {
			selectedUsers[userID] = struct{}{}
			source := userSource[userID]
			for _, device := range source.Devices {
				allDeviceIDs = append(allDeviceIDs, device.DeviceID)
				targetAgent := pickAgentByRemaining(placementAgentIDs, remaining)
				if !firstAssignedOwner {
					targetAgent = ownerAgentID
					firstAssignedOwner = true
				}
				remaining[targetAgent]--
				owners = append(owners, deviceOwner{agentID: targetAgent, userID: userID, device: device})
				if targetAgent == ownerAgentID {
					localSenderIDs = append(localSenderIDs, device.DeviceID)
				}
			}
		}
		for _, owner := range owners {
			items := userByAgent[owner.agentID]
			entry := items[owner.userID]
			if entry == nil {
				source := userSource[owner.userID]
				entry = &fixtureUser{
					UserID:    source.UserID,
					PhoneE164: source.PhoneE164,
					Devices:   []fixtureDevice{},
				}
				items[owner.userID] = entry
			}
			entry.Devices = append(entry.Devices, owner.device)
		}
		assign := assignments[ownerAgentID]
		assign.Conversations = append(assign.Conversations, agentConversationAssignment{
			ID:                   stored.ID,
			AllDeviceIDs:         allDeviceIDs,
			LocalSenderDeviceIDs: localSenderIDs,
		})
		assignments[ownerAgentID] = assign
	}

	for _, agentID := range agentIDs {
		assign := assignments[agentID]
		for _, user := range userByAgent[agentID] {
			assign.Users = append(assign.Users, *user)
		}
		sort.Slice(assign.Users, func(i, j int) bool { return assign.Users[i].UserID < assign.Users[j].UserID })
		assignments[agentID] = assign
	}
	return assignments, len(selectedUsers), actualDevices, len(indexes), nil
}

func pickAgentByRemaining(agentIDs []string, remaining map[string]float64) string {
	best := agentIDs[0]
	bestRemaining := remaining[best]
	for _, agentID := range agentIDs[1:] {
		if remaining[agentID] > bestRemaining {
			best = agentID
			bestRemaining = remaining[agentID]
		}
	}
	return best
}

func positiveShareAgentIDs(agentIDs []string, shares map[string]float64) []string {
	selected := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		if shares[agentID] > 0 {
			selected = append(selected, agentID)
		}
	}
	return selected
}

func parseAgentShares(raw string) map[string]float64 {
	result := make(map[string]float64)
	for _, part := range strings.Split(strings.TrimSpace(raw), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			continue
		}
		value := strings.TrimSpace(pieces[1])
		fraction, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		result[strings.TrimSpace(pieces[0])] = fraction
	}
	return result
}

func buildPopulationFromAssignment(assign controllerAssignment) *population {
	selected := &population{
		users:         make([]*loadUser, 0, len(assign.Users)),
		devices:       make([]*deviceClient, 0),
		conversations: make([]*conversationFixture, 0, len(assign.Conversations)),
	}
	deviceIndex := make(map[string]*deviceClient)
	for _, source := range assign.Users {
		user := &loadUser{
			UserID:    source.UserID,
			PhoneE164: source.PhoneE164,
			Devices:   make([]*deviceClient, 0, len(source.Devices)),
		}
		for _, device := range source.Devices {
			client := &deviceClient{
				userID:       source.UserID,
				deviceID:     device.DeviceID,
				phoneE164:    source.PhoneE164,
				accessToken:  device.AccessToken,
				refreshToken: device.RefreshToken,
				baseURL:      assign.BaseURL,
				httpClient:   &http.Client{Timeout: 15 * time.Second},
				phase:        assign.Profile,
			}
			user.Devices = append(user.Devices, client)
			selected.devices = append(selected.devices, client)
			deviceIndex[client.deviceID] = client
		}
		selected.users = append(selected.users, user)
	}
	for _, item := range assign.Conversations {
		conv := &conversationFixture{
			ID:           item.ID,
			AllDeviceIDs: append([]string(nil), item.AllDeviceIDs...),
		}
		for _, senderID := range item.LocalSenderDeviceIDs {
			if device := deviceIndex[senderID]; device != nil {
				conv.SenderDevices = append(conv.SenderDevices, device)
			}
		}
		selected.conversations = append(selected.conversations, conv)
	}
	return selected
}

func runDistributedAssignment(ctx context.Context, cfg config, assignment controllerAssignment, writer *jsonlWriter) (agentPhaseComplete, error) {
	pop := buildPopulationFromAssignment(assignment)
	reducer := newReducer()
	for _, device := range pop.devices {
		device.attach(reducer, writer, assignment.Profile)
	}
	runCfg := cfg
	runCfg.baseURL = assignment.BaseURL
	runCfg.connectWaveSize = assignment.ConnectWaveSize
	runCfg.connectWaveWait = time.Duration(assignment.ConnectWaveIntervalMS) * time.Millisecond
	plan := scenarioPlan{
		label:            assignment.Profile,
		profile:          assignment.Profile,
		targetDevices:    len(pop.devices),
		warmupDuration:   time.Duration(assignment.WarmupDurationMS) * time.Millisecond,
		holdDuration:     time.Duration(assignment.HoldDurationMS) * time.Millisecond,
		enableTraffic:    assignment.Profile != profileConnectedIdle,
		connectThreshold: assignment.ReadyThreshold,
	}
	connectResult := connectPopulation(ctx, runCfg, pop, writer, plan)
	agentID := strings.TrimSpace(cfg.agentID)
	if agentID == "" {
		agentID = hostAgentID
	}
	result := agentPhaseComplete{
		AgentID:           agentID,
		SharedSecret:      cfg.sharedSecret,
		Phase:             assignment.Phase,
		Succeeded:         true,
		AttemptedDevices:  connectResult.AttemptedCount,
		ReadyDevices:      connectResult.ReadyCount,
		ConnectDurationMS: connectResult.ConnectDuration.Milliseconds(),
	}
	if connectResult.ReadyCount == 0 || rate(connectResult.ReadyCount, connectResult.AttemptedCount) < assignment.ReadyThreshold {
		result.Succeeded = false
		result.FirstFailurePhase = "connect"
		result.FirstFailureReason = fallbackFailureReason(connectResult.FirstFailureReason, "insufficient_ready")
		for _, device := range pop.devices {
			device.close()
		}
		return result, nil
	}

	stopTraffic := func() {}
	var trafficErrCh <-chan error
	if plan.enableTraffic {
		stopTraffic, trafficErrCh = startConversationLoad(ctx, pop.conversations)
	}
	defer stopTraffic()

	if plan.warmupDuration > 0 {
		if err := waitForPhase(ctx, &phaseTracker{phase: "warmup"}, "warmup", plan.warmupDuration, trafficErrCh); err != nil {
			result.Succeeded = false
			result.FirstFailurePhase = "warmup"
			result.FirstFailureReason = classifyFailureReason(err)
			return result, nil
		}
	}
	measurementStart := time.Now().UTC()
	result.MeasurementStartNS = measurementStart.UnixNano()
	if err := waitForPhase(ctx, &phaseTracker{phase: "hold"}, "hold", plan.holdDuration, trafficErrCh); err != nil {
		result.Succeeded = false
		result.FirstFailurePhase = "hold"
		result.FirstFailureReason = classifyFailureReason(err)
	}
	result.MeasurementEndNS = time.Now().UTC().UnixNano()
	stopTraffic()
	time.Sleep(5 * time.Second)
	for _, device := range pop.devices {
		device.close()
	}
	return result, nil
}

func ingestRawEvent(reducer *reducer, event rawEvent) {
	if reducer == nil {
		return
	}
	switch event.Type {
	case "t_send_ack":
		sendAt := event.Timestamp
		targetDeviceIDs := stringSliceDetail(event.Details, "target_device_ids")
		if unixNano := int64Detail(event.Details, "client_send_at_unix_nano"); unixNano > 0 {
			sendAt = time.Unix(0, unixNano).UTC()
		}
		reducer.recordSend(sendRecord{
			MessageID:       event.MessageID,
			ConversationID:  event.ConversationID,
			SenderUserID:    event.UserID,
			SenderDeviceID:  event.DeviceID,
			ClientSendAt:    sendAt,
			AckAt:           event.Timestamp,
			ServerOrder:     event.ServerOrder,
			TargetDeviceIDs: targetDeviceIDs,
			TargetCount:     len(targetDeviceIDs),
		})
	case "t_recipient_receive":
		reducer.recordReceive(receiveRecord{
			MessageID:      event.MessageID,
			ConversationID: event.ConversationID,
			DeviceID:       event.DeviceID,
			ReceivedAt:     event.Timestamp,
			ServerOrder:    event.ServerOrder,
		})
	case "t_reconnect_start":
		reducer.startReconnect(event.ReconnectID, event.DeviceID, event.Timestamp)
	case "t_ws_ready":
		if strings.TrimSpace(event.ReconnectID) != "" {
			reducer.markReconnectReady(event.ReconnectID, event.Timestamp)
		}
	case "t_sync_complete":
		if strings.TrimSpace(event.ReconnectID) != "" {
			reducer.markReconnectSynced(event.ReconnectID, event.Timestamp, true)
		}
	}
}

func measureConvergenceForAssignments(ctx context.Context, cfg config, fx *fixture, assignments map[string]controllerAssignment) (float64, error) {
	targetDevices := 0
	for _, assign := range assignments {
		for _, user := range assign.Users {
			targetDevices += len(user.Devices)
		}
	}
	pop, err := populationFromFixture(fx, targetDevices, cfg.baseURL, cfg.profile)
	if err != nil {
		return 0, err
	}
	return measureConvergence(ctx, pop.conversations)
}

func sampleClockOffset(ctx context.Context, cfg config) time.Duration {
	bestRTT := time.Duration(1<<63 - 1)
	bestOffset := time.Duration(0)
	for i := 0; i < 8; i++ {
		startedAt := time.Now().UTC()
		var resp agentRegisterResponse
		if err := getAgentJSON(ctx, cfg, "/agents/"+cfg.agentID+"/timesync", &resp); err != nil {
			continue
		}
		finishedAt := time.Now().UTC()
		rtt := finishedAt.Sub(startedAt)
		midpoint := startedAt.Add(rtt / 2)
		offset := time.Unix(0, resp.ServerUnixNano).UTC().Sub(midpoint)
		if rtt < bestRTT {
			bestRTT = rtt
			bestOffset = offset
		}
		time.Sleep(50 * time.Millisecond)
	}
	return bestOffset
}

func postAgentJSON(ctx context.Context, cfg config, method, path string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(cfg.controllerURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(cfg.sharedSecret) != "" {
		req.Header.Set("X-OHMF-Load-Secret", cfg.sharedSecret)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller request failed: %d %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func getAgentJSON(ctx context.Context, cfg config, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cfg.controllerURL, "/")+path, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.sharedSecret) != "" {
		req.Header.Set("X-OHMF-Load-Secret", cfg.sharedSecret)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller request failed: %d %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func stringSliceDetail(details map[string]any, key string) []string {
	if details == nil {
		return nil
	}
	raw, ok := details[key]
	if !ok {
		return nil
	}
	switch items := raw.(type) {
	case []string:
		return append([]string(nil), items...)
	case []any:
		out := make([]string, 0, len(items))
		for _, item := range items {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func int64Detail(details map[string]any, key string) int64 {
	if details == nil {
		return 0
	}
	switch value := details[key].(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case float64:
		return int64(value)
	case json.Number:
		v, _ := value.Int64()
		return v
	default:
		return 0
	}
}
