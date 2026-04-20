package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	profileActiveSustain = "active_sustain"
	profileConnectedIdle = "connected_idle"
	profileResilience    = "resilience"
)

type scenarioPlan struct {
	label             string
	profile           string
	targetDevices     int
	warmupDuration    time.Duration
	holdDuration      time.Duration
	enableTraffic     bool
	connectThreshold  float64
	reconnectFraction float64
	reconnectWaves    int
	reconnectInterval time.Duration
	restartGateway    bool
	restartDelivery   bool
}

type phaseTracker struct {
	mu    sync.RWMutex
	phase string
}

func (p *phaseTracker) set(phase string) {
	p.mu.Lock()
	p.phase = phase
	p.mu.Unlock()
}

func (p *phaseTracker) get() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.phase
}

func runProvision(ctx context.Context, cfg config) error {
	provisionDir := filepath.Join(cfg.runDir, "provision")
	provisionCfg := cfg
	provisionCfg.runDir = provisionDir
	provisionCfg.fixtureDir = filepath.Join(cfg.runDir, "fixture")
	if err := prepareRunDirectory(provisionCfg); err != nil {
		return err
	}
	eventsWriter, err := newJSONLWriter(filepathJoin(provisionCfg.runDir, "events.jsonl"))
	if err != nil {
		return err
	}
	defer eventsWriter.close()
	targetDevices := maxInt(cfg.initialDevices, cfg.maxDevices)
	if _, err := loadOrProvisionFixture(ctx, provisionCfg, eventsWriter, targetDevices); err != nil {
		return err
	}
	log.Printf("fixture ready: %s", provisionCfg.fixtureDir)
	return nil
}

func runSingleScenario(ctx context.Context, cfg config) error {
	plan := defaultPlanForMode(cfg)
	outcome, err := executeScenario(ctx, cfg, plan, nil)
	if err != nil {
		return err
	}
	if !outcome.Passed {
		return fmt.Errorf("%s failed: %s/%s", outcome.Profile, fallbackText(outcome.Summary.Workload.FirstFailurePhase), fallbackText(outcome.Summary.Workload.FirstFailureReason))
	}
	return nil
}

func runCampaign(ctx context.Context, cfg config) error {
	fixtureDir := filepath.Join(cfg.runDir, "fixture")
	provisionCfg := cfg
	provisionCfg.fixtureDir = fixtureDir
	provisionCfg.runDir = filepath.Join(cfg.runDir, "provision")
	if err := prepareRunDirectory(provisionCfg); err != nil {
		return err
	}
	provisionEvents, err := newJSONLWriter(filepathJoin(provisionCfg.runDir, "events.jsonl"))
	if err != nil {
		return err
	}
	defer provisionEvents.close()
	fixtureTarget := 700
	fx, err := loadOrProvisionFixture(ctx, provisionCfg, provisionEvents, fixtureTarget)
	if err != nil {
		return err
	}

	campaign := campaignSummary{Tiers: make([]campaignTierSummary, 0)}
	activeTargets := []int{250, 300, 350, 400, 450, 500}
	lastFullPass := 0
	firstFail := 0
	for idx, target := range activeTargets {
		runCfg := childRunConfig(cfg, fixtureDir, fmt.Sprintf("%02d-active-%d", idx+1, target))
		outcome, err := executeScenario(ctx, runCfg, scenarioPlan{
			label:            fmt.Sprintf("active-%d", target),
			profile:          profileActiveSustain,
			targetDevices:    target,
			warmupDuration:   90 * time.Second,
			holdDuration:     10 * time.Minute,
			enableTraffic:    true,
			connectThreshold: 0.98,
		}, fx)
		if err != nil {
			return err
		}
		campaign.Tiers = append(campaign.Tiers, tierSummaryFromOutcome(outcome))
		if outcome.Passed {
			lastFullPass = target
			continue
		}
		firstFail = target
		break
	}

	if lastFullPass > 0 && firstFail > 0 {
		low := lastFullPass
		high := firstFail
		for probe := 0; probe < 3 && high-low > 25; probe++ {
			mid := low + ((high - low) / 2)
			runCfg := childRunConfig(cfg, fixtureDir, fmt.Sprintf("probe-active-%d", mid))
			outcome, err := executeScenario(ctx, runCfg, scenarioPlan{
				label:            fmt.Sprintf("active-probe-%d", mid),
				profile:          profileActiveSustain,
				targetDevices:    mid,
				warmupDuration:   45 * time.Second,
				holdDuration:     5 * time.Minute,
				enableTraffic:    true,
				connectThreshold: 0.98,
			}, fx)
			if err != nil {
				return err
			}
			campaign.Tiers = append(campaign.Tiers, tierSummaryFromOutcome(outcome))
			if outcome.Passed {
				low = mid
			} else {
				high = mid
			}
		}
	}

	if lastFullPass > 0 {
		headlineCfg := childRunConfig(cfg, fixtureDir, fmt.Sprintf("headline-active-%d", lastFullPass))
		headline, err := executeScenario(ctx, headlineCfg, scenarioPlan{
			label:            fmt.Sprintf("active-headline-%d", lastFullPass),
			profile:          profileActiveSustain,
			targetDevices:    lastFullPass,
			warmupDuration:   90 * time.Second,
			holdDuration:     15 * time.Minute,
			enableTraffic:    true,
			connectThreshold: 0.98,
		}, fx)
		if err != nil {
			return err
		}
		campaign.Tiers = append(campaign.Tiers, tierSummaryFromOutcome(headline))
		if headline.Passed {
			copySummary := headline.Summary
			campaign.ActiveHeadline = &copySummary
		}
	}

	connectedTargets := []int{400, 500, 600, 700}
	for idx, target := range connectedTargets {
		runCfg := childRunConfig(cfg, fixtureDir, fmt.Sprintf("%02d-idle-%d", idx+1, target))
		outcome, err := executeScenario(ctx, runCfg, scenarioPlan{
			label:            fmt.Sprintf("idle-%d", target),
			profile:          profileConnectedIdle,
			targetDevices:    target,
			holdDuration:     10 * time.Minute,
			connectThreshold: 0.99,
		}, fx)
		if err != nil {
			return err
		}
		campaign.Tiers = append(campaign.Tiers, tierSummaryFromOutcome(outcome))
		if outcome.Passed {
			copySummary := outcome.Summary
			campaign.ConnectedCeiling = &copySummary
			continue
		}
		break
	}

	if lastFullPass > 0 {
		resilienceTarget := minInt(200, int(math.Floor(float64(lastFullPass)*0.5)))
		runCfg := childRunConfig(cfg, fixtureDir, fmt.Sprintf("resilience-%d", resilienceTarget))
		outcome, err := executeScenario(ctx, runCfg, scenarioPlan{
			label:             fmt.Sprintf("resilience-%d", resilienceTarget),
			profile:           profileResilience,
			targetDevices:     resilienceTarget,
			warmupDuration:    2 * time.Minute,
			holdDuration:      cfg.proveDuration,
			enableTraffic:     true,
			connectThreshold:  0.98,
			reconnectFraction: 0.10,
			reconnectWaves:    3,
			reconnectInterval: 10 * time.Second,
			restartGateway:    true,
			restartDelivery:   true,
		}, fx)
		if err != nil {
			return err
		}
		campaign.Tiers = append(campaign.Tiers, tierSummaryFromOutcome(outcome))
		copySummary := outcome.Summary
		campaign.Resilience = &copySummary
	}

	campaign.FirstBottleneck = deriveFirstBottleneck(campaign.Tiers)
	if err := writeCampaignSummaryFiles(cfg.runDir, campaign); err != nil {
		return err
	}
	if campaign.ActiveHeadline == nil {
		return fmt.Errorf("campaign completed without a promoted active headline")
	}
	return nil
}

func executeScenario(ctx context.Context, cfg config, plan scenarioPlan, fx *fixture) (scenarioOutcome, error) {
	if err := prepareRunDirectory(cfg); err != nil {
		return scenarioOutcome{}, err
	}
	eventsWriter, err := newJSONLWriter(filepathJoin(cfg.runDir, "events.jsonl"))
	if err != nil {
		return scenarioOutcome{}, err
	}
	defer eventsWriter.close()

	reducer := newReducer()
	sampler, err := newResourceSampler(cfg.runDir, reducer)
	if err != nil {
		return scenarioOutcome{}, err
	}
	samplerCtx, samplerCancel := context.WithCancel(ctx)
	defer samplerCancel()
	sampler.start(samplerCtx)

	if fx == nil {
		fx, err = loadOrProvisionFixture(ctx, cfg, eventsWriter, plan.targetDevices)
		if err != nil {
			return scenarioOutcome{}, err
		}
	}

	pop, err := populationFromFixture(fx, plan.targetDevices, cfg.baseURL, plan.profile)
	if err != nil {
		return scenarioOutcome{}, err
	}
	persistFixtureTokens := func() error {
		return persistPopulationFixtureTokens(cfg, fx, pop)
	}
	for _, device := range pop.devices {
		device.attach(reducer, eventsWriter, plan.profile)
	}

	connectResult := connectPopulation(ctx, cfg, pop, eventsWriter, plan)
	failure := phaseFailure{Phase: connectResult.FirstFailurePhase, Reason: connectResult.FirstFailureReason}
	if connectResult.ReadyCount == 0 || rate(connectResult.ReadyCount, connectResult.AttemptedCount) < plan.connectThreshold {
		for _, device := range pop.devices {
			device.close()
		}
		result := reducer.snapshotSummary(summaryInput{
			concurrentUsers:      len(pop.users),
			concurrentDevices:    len(pop.devices),
			conversations:        len(pop.conversations),
			duration:             0,
			messageRate:          scenarioMessageRate(plan, len(pop.conversations)),
			connectReadiness:     rate(connectResult.ReadyCount, connectResult.AttemptedCount),
			connectPhaseDuration: connectResult.ConnectDuration,
			firstFailurePhase:    fallbackFailurePhase(failure.Phase, "connect"),
			firstFailureReason:   fallbackFailureReason(failure.Reason, "insufficient_ready"),
		})
		if err := persistFixtureTokens(); err != nil {
			return scenarioOutcome{}, err
		}
		if err := writeSummaryFiles(cfg.runDir, result); err != nil {
			return scenarioOutcome{}, err
		}
		return scenarioOutcome{
			Label:         plan.label,
			Summary:       result,
			Profile:       plan.profile,
			TargetDevices: plan.targetDevices,
			Passed:        false,
			Failure: phaseFailure{
				Phase:  result.Workload.FirstFailurePhase,
				Reason: result.Workload.FirstFailureReason,
			},
			RunDir: cfg.runDir,
		}, nil
	}

	phaseState := &phaseTracker{phase: "warmup"}
	stopTraffic := func() {}
	var trafficErrCh <-chan error
	if plan.enableTraffic {
		stopTraffic, trafficErrCh = startConversationLoad(ctx, pop.conversations)
	}
	defer stopTraffic()

	if plan.warmupDuration > 0 {
		if err := waitForPhase(ctx, phaseState, "warmup", plan.warmupDuration, trafficErrCh); err != nil {
			recordPhaseFailure(eventsWriter, "warmup", classifyFailureReason(err), err)
			failure = phaseFailure{Phase: "warmup", Reason: classifyFailureReason(err), Err: err}
		}
	}

	measurementStart := time.Now().UTC()
	measurementEnd := measurementStart.Add(plan.holdDuration)
	var gatewayRecovery time.Duration
	var deliveryRecovery time.Duration
	var backlogDrain time.Duration
	resilienceErrCh := make(chan error, 1)
	if plan.profile == profileResilience {
		phaseState.set("recovery")
		go func() {
			resilienceErrCh <- runResilienceActions(ctx, cfg, pop.devices, plan, sampler, &gatewayRecovery, &deliveryRecovery, &backlogDrain)
		}()
	} else {
		close(resilienceErrCh)
	}

	phaseState.set("hold")
	holdCtx, holdCancel := context.WithDeadline(ctx, measurementEnd)
	defer holdCancel()
	waitErr := waitForPhase(holdCtx, phaseState, phaseState.get(), plan.holdDuration, trafficErrCh)
	if waitErr != nil && failure.Reason == "" {
		failure = phaseFailure{Phase: phaseState.get(), Reason: classifyFailureReason(waitErr), Err: waitErr}
		recordPhaseFailure(eventsWriter, failure.Phase, failure.Reason, waitErr)
	}
	if plan.profile == profileResilience {
		if err := <-resilienceErrCh; err != nil && failure.Reason == "" {
			failure = phaseFailure{Phase: "recovery", Reason: classifyFailureReason(err), Err: err}
			recordPhaseFailure(eventsWriter, failure.Phase, failure.Reason, err)
		}
	}

	stopTraffic()
	time.Sleep(5 * time.Second)
	for _, device := range pop.devices {
		device.close()
	}

	convergenceRate, err := measureConvergence(ctx, pop.conversations)
	if err != nil {
		recordPhaseFailure(eventsWriter, "teardown", classifyFailureReason(err), err)
		failure = phaseFailure{Phase: "teardown", Reason: classifyFailureReason(err), Err: err}
		convergenceRate = 0
	}

	dbP95 := maxPrometheusDuration(
		ctx,
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_gateway_db_query_latency_seconds_bucket[5m])))",
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_messages_processor_db_query_latency_seconds_bucket[5m])))",
		"histogram_quantile(0.95, sum by (le) (increase(ohmf_delivery_processor_db_query_latency_seconds_bucket[5m])))",
		"histogram_quantile(0.95, sum by (le) (ohmf_gateway_db_query_latency_seconds_bucket))",
		"histogram_quantile(0.95, sum by (le) (ohmf_messages_processor_db_query_latency_seconds_bucket))",
		"histogram_quantile(0.95, sum by (le) (ohmf_delivery_processor_db_query_latency_seconds_bucket))",
	)
	peakCPU, peakMemory, peakKafkaLag := sampler.resourcePeaks()
	result := reducer.snapshotSummary(summaryInput{
		concurrentUsers:      len(pop.users),
		concurrentDevices:    len(pop.devices),
		conversations:        len(pop.conversations),
		duration:             plan.holdDuration,
		measurementStart:     measurementStart,
		measurementEnd:       measurementEnd,
		messageRate:          scenarioMessageRate(plan, len(pop.conversations)),
		connectReadiness:     rate(connectResult.ReadyCount, connectResult.AttemptedCount),
		connectPhaseDuration: connectResult.ConnectDuration,
		firstFailurePhase:    failure.Phase,
		firstFailureReason:   failure.Reason,
		convergenceRate:      convergenceRate,
		restartRecoveryTime:  maxDuration(gatewayRecovery, deliveryRecovery),
		backlogDrainTime:     backlogDrain,
		dbP95Latency:         dbP95,
		peakKafkaLag:         maxInt64(sampler.currentLagTotal(), peakKafkaLag),
		peakCPU:              peakCPU,
		peakMemory:           peakMemory,
	})

	passed, phase, reason := evaluateScenarioPass(plan.profile, result)
	if !passed && result.Workload.FirstFailurePhase == "" {
		result.Workload.FirstFailurePhase = phase
		result.Workload.FirstFailureReason = reason
		failure = phaseFailure{Phase: phase, Reason: reason}
	}
	if err := persistFixtureTokens(); err != nil {
		return scenarioOutcome{}, err
	}
	if err := writeSummaryFiles(cfg.runDir, result); err != nil {
		return scenarioOutcome{}, err
	}
	return scenarioOutcome{
		Label:         plan.label,
		Summary:       result,
		Profile:       plan.profile,
		TargetDevices: plan.targetDevices,
		Passed:        passed,
		Failure:       failure,
		RunDir:        cfg.runDir,
	}, nil
}

func connectPopulation(ctx context.Context, cfg config, pop *population, events *jsonlWriter, plan scenarioPlan) connectResult {
	if pop == nil || len(pop.devices) == 0 {
		return connectResult{}
	}
	waveSize := connectWaveSizeForTarget(cfg.connectWaveSize, plan.targetDevices)
	waveWait := cfg.connectWaveWait
	if waveWait <= 0 {
		waveWait = 15 * time.Second
	}
	startedAt := time.Now().UTC()
	totalReady := 0
	for offset := 0; offset < len(pop.devices); offset += waveSize {
		waveStartedAt := time.Now()
		end := offset + waveSize
		if end > len(pop.devices) {
			end = len(pop.devices)
		}
		wave := pop.devices[offset:end]
		var ready int
		var firstErr error
		var once sync.Once
		var readyMu sync.Mutex
		var wg sync.WaitGroup
		waveCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		for _, device := range wave {
			wg.Add(1)
			go func(device *deviceClient) {
				defer wg.Done()
				if err := device.connect(waveCtx, ""); err != nil {
					once.Do(func() {
						firstErr = err
					})
					recordPhaseFailure(events, "connect", classifyFailureReason(err), err)
					return
				}
				readyMu.Lock()
				ready++
				readyMu.Unlock()
			}(device)
		}
		wg.Wait()
		cancel()
		totalReady += ready
		if rate(ready, len(wave)) < plan.connectThreshold {
			return connectResult{
				ReadyCount:         totalReady,
				AttemptedCount:     len(pop.devices),
				ConnectDuration:    time.Since(startedAt),
				FirstFailurePhase:  "connect",
				FirstFailureReason: fallbackFailureReason(classifyFailureReason(firstErr), "insufficient_ready"),
			}
		}
		if wait := waveWait - time.Since(waveStartedAt); wait > 0 && end < len(pop.devices) {
			time.Sleep(wait)
		}
	}
	return connectResult{
		ReadyCount:      totalReady,
		AttemptedCount:  len(pop.devices),
		ConnectDuration: time.Since(startedAt),
	}
}

func startConversationLoad(ctx context.Context, conversations []*conversationFixture) (func(), <-chan error) {
	errCh := make(chan error, 1)
	loadCtx, cancel := context.WithCancel(ctx)
	for _, conv := range conversations {
		go func(conv *conversationFixture) {
			ticker := time.NewTicker(8 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-loadCtx.Done():
					return
				case tick := <-ticker.C:
					sender := conv.nextSenderDevice()
					if sender == nil {
						continue
					}
					text := fmt.Sprintf("[%s] %s %d", sender.phase, conv.ID, tick.UnixNano())
					if err := sender.sendText(loadCtx, conv.ID, text, conv.targetDeviceIDs(sender.deviceID)); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				}
			}
		}(conv)
	}
	return cancel, errCh
}

func waitForPhase(ctx context.Context, phaseState *phaseTracker, phase string, duration time.Duration, errCh <-chan error) error {
	if phaseState != nil && phase != "" {
		phaseState.set(phase)
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-errCh:
			if ok && err != nil {
				return err
			}
			errCh = nil
		case <-timer.C:
			return nil
		}
	}
}

func runResilienceActions(ctx context.Context, cfg config, devices []*deviceClient, plan scenarioPlan, sampler *resourceSampler, gatewayRecovery, deliveryRecovery, backlogDrain *time.Duration) error {
	offset := 0
	for wave := 0; wave < plan.reconnectWaves; wave++ {
		count := int(math.Ceil(float64(len(devices)) * plan.reconnectFraction))
		if count <= 0 {
			count = 1
		}
		if offset+count > len(devices) {
			offset = 0
		}
		if err := reconnectDevices(ctx, devices[offset:offset+count], fmt.Sprintf("storm-%d", wave+1)); err != nil {
			return err
		}
		offset += count
		time.Sleep(plan.reconnectInterval)
	}
	if plan.restartGateway {
		recoveryStart := time.Now().UTC()
		if err := restartComposeService(ctx, cfg.composeFile, "api"); err != nil {
			return err
		}
		if err := waitForHTTP(ctx, strings.TrimRight(cfg.baseURL, "/")+"/healthz", 90*time.Second); err != nil {
			return err
		}
		*gatewayRecovery = time.Since(recoveryStart)
		if err := reconnectDevices(ctx, devices, "gateway_restart"); err != nil {
			return err
		}
	}
	if plan.restartDelivery {
		baselineLag := sampler.currentLagTotal()
		restartAt := time.Now().UTC()
		if err := restartComposeService(ctx, cfg.composeFile, "delivery-processor"); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
		*deliveryRecovery = time.Since(restartAt)
		*backlogDrain = backlogDrainTime(sampler.lagSamplesSince(restartAt), baselineLag, 10)
	}
	return nil
}

func reconnectDevices(ctx context.Context, devices []*deviceClient, reason string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var once sync.Once
	for _, device := range devices {
		wg.Add(1)
		go func(device *deviceClient) {
			defer wg.Done()
			if err := device.reconnect(ctx, reason); err != nil {
				once.Do(func() {
					errCh <- err
				})
			}
		}(device)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func defaultPlanForMode(cfg config) scenarioPlan {
	switch strings.ToLower(cfg.mode) {
	case "smoke":
		profile := profileFromConfig(cfg)
		return scenarioPlan{
			label:            "smoke",
			profile:          profile,
			targetDevices:    maxInt(cfg.initialDevices, 20),
			warmupDuration:   minDuration(cfg.warmupDuration, 15*time.Second),
			holdDuration:     cfg.proveDuration,
			enableTraffic:    profile != profileConnectedIdle,
			connectThreshold: chooseConnectThreshold(profile),
		}
	case "resilience":
		return scenarioPlan{
			label:             "resilience",
			profile:           profileResilience,
			targetDevices:     cfg.initialDevices,
			warmupDuration:    maxDuration(cfg.warmupDuration, 2*time.Minute),
			holdDuration:      cfg.proveDuration,
			enableTraffic:     true,
			connectThreshold:  0.98,
			reconnectFraction: 0.10,
			reconnectWaves:    3,
			reconnectInterval: 10 * time.Second,
			restartGateway:    true,
			restartDelivery:   true,
		}
	default:
		profile := profileFromConfig(cfg)
		return scenarioPlan{
			label:            profile,
			profile:          profile,
			targetDevices:    cfg.initialDevices,
			warmupDuration:   chooseWarmup(profile, cfg.warmupDuration),
			holdDuration:     cfg.sustainDuration,
			enableTraffic:    profile != profileConnectedIdle,
			connectThreshold: chooseConnectThreshold(profile),
		}
	}
}

func profileFromConfig(cfg config) string {
	switch strings.ToLower(strings.TrimSpace(cfg.profile)) {
	case profileConnectedIdle:
		return profileConnectedIdle
	case profileResilience:
		return profileResilience
	default:
		return profileActiveSustain
	}
}

func chooseWarmup(profile string, configured time.Duration) time.Duration {
	if profile == profileConnectedIdle {
		return 0
	}
	if configured > 0 {
		return configured
	}
	return 90 * time.Second
}

func chooseConnectThreshold(profile string) float64 {
	if profile == profileConnectedIdle {
		return 0.99
	}
	return 0.98
}

func childRunConfig(parent config, fixtureDir string, label string) config {
	child := parent
	child.runDir = filepath.Join(parent.runDir, label)
	child.fixtureDir = fixtureDir
	return child
}

func tierSummaryFromOutcome(outcome scenarioOutcome) campaignTierSummary {
	return campaignTierSummary{
		Label:               outcome.Label,
		Profile:             outcome.Profile,
		TargetDevices:       outcome.TargetDevices,
		ActualDevices:       outcome.Summary.Workload.ConcurrentDevices,
		Passed:              outcome.Passed,
		ConnectReadiness:    outcome.Summary.Workload.ConnectReadinessPercent,
		FirstFailurePhase:   outcome.Summary.Workload.FirstFailurePhase,
		FirstFailureReason:  outcome.Summary.Workload.FirstFailureReason,
		MessagesPerSecond:   outcome.Summary.Performance.MessagesPerSecond,
		DeliveriesPerSecond: outcome.Summary.Performance.DeliveriesPerSecond,
		ReportPath:          filepathJoin(outcome.RunDir, "summary.md"),
	}
}

func deriveFirstBottleneck(rows []campaignTierSummary) string {
	for _, row := range rows {
		if row.Passed {
			continue
		}
		switch row.FirstFailureReason {
		case "bad_handshake", "http_timeout", "429_rate_limited", "401_unauthorized":
			return "websocket/bootstrap API saturation during connection and recovery ramps"
		case "latency_budget_exceeded":
			return "end-to-end latency budget exceeded under sustained active messaging"
		case "correctness_regression":
			return "delivery correctness regressed before infrastructure saturation"
		case "reconnect_success_below_slo":
			return "recovery path saturation during reconnect and restart handling"
		default:
			if row.FirstFailureReason != "" {
				return row.FirstFailureReason
			}
		}
	}
	return "no bottleneck isolated"
}

func evaluateScenarioPass(profile string, result summary) (bool, string, string) {
	switch profile {
	case profileConnectedIdle:
		if result.Workload.ConnectReadinessPercent < 99.0 {
			return false, "connect", "insufficient_ready"
		}
		if result.Workload.FirstFailureReason != "" {
			return false, fallbackFailurePhase(result.Workload.FirstFailurePhase, "connect"), result.Workload.FirstFailureReason
		}
		return true, "", ""
	case profileResilience:
		if result.Correctness.LostDeliveries > 0 || result.Correctness.DuplicateDeliveries > 0 || result.Correctness.OutOfOrderDeliveries > 0 || result.Correctness.MultiDeviceConvergenceRate < 1.0 {
			return false, "recovery", "correctness_regression"
		}
		if result.Resilience.ReconnectSuccessRate < 0.99 {
			return false, "recovery", "reconnect_success_below_slo"
		}
		if parseDuration(result.Resilience.RestartRecoveryTime) > 30*time.Second {
			return false, "recovery", "restart_recovery_exceeded"
		}
		return true, "", ""
	default:
		if result.Correctness.LostDeliveries > 0 || result.Correctness.DuplicateDeliveries > 0 || result.Correctness.OutOfOrderDeliveries > 0 || result.Correctness.MultiDeviceConvergenceRate < 1.0 {
			return false, "hold", "correctness_regression"
		}
		if result.Workload.ConnectReadinessPercent < 98.0 {
			return false, "connect", "insufficient_ready"
		}
		if parseDuration(result.Performance.P95SendToAckLatency) > 2500*time.Millisecond || parseDuration(result.Performance.P95EndToEndDeliveryLatency) > 3*time.Second {
			return false, "hold", "latency_budget_exceeded"
		}
		if result.ResourceUsage.PeakKafkaLag > 0 && parseDuration(result.Resilience.BacklogDrainTime) > 15*time.Second {
			return false, "hold", "kafka_lag_not_drained"
		}
		if result.Workload.FirstFailureReason != "" {
			return false, fallbackFailurePhase(result.Workload.FirstFailurePhase, "hold"), result.Workload.FirstFailureReason
		}
		return true, "", ""
	}
}

func scenarioMessageRate(plan scenarioPlan, conversations int) float64 {
	if !plan.enableTraffic {
		return 0
	}
	return roundTo(float64(conversations)/8.0, 2)
}

func fallbackFailurePhase(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func fallbackFailureReason(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func minDuration(values ...time.Duration) time.Duration {
	var out time.Duration
	for i, value := range values {
		if i == 0 || (value > 0 && value < out) {
			out = value
		}
	}
	return out
}

func minInt(values ...int) int {
	var out int
	for i, value := range values {
		if i == 0 || value < out {
			out = value
		}
	}
	return out
}

func maxInt(values ...int) int {
	var out int
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func connectWaveSizeForTarget(explicit, target int) int {
	if explicit > 0 {
		return explicit
	}
	if target <= 300 {
		return 25
	}
	return 15
}
