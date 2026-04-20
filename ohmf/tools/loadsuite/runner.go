package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

type scenarioSpec struct {
	phase                string
	targetDevices        int
	fixedUsers           int
	duration             time.Duration
	reconnectFraction    float64
	reconnectAfter       time.Duration
	restartGatewayAfter  time.Duration
	restartDeliveryAfter time.Duration
}

func run(ctx context.Context, cfg config) error {
	if err := prepareRunDirectory(cfg); err != nil {
		return err
	}
	switch strings.ToLower(cfg.mode) {
	case "agent":
		return runAgent(ctx, cfg)
	case "controller":
		if err := ensureServicesRunning(ctx, cfg); err != nil {
			return err
		}
		return runController(ctx, cfg)
	case "provision":
		if err := ensureServicesRunning(ctx, cfg); err != nil {
			return err
		}
		return runProvision(ctx, cfg)
	case "campaign", "full":
		if err := ensureServicesRunning(ctx, cfg); err != nil {
			return err
		}
		return runCampaign(ctx, cfg)
	case "sustain", "resilience", "smoke":
		if err := ensureServicesRunning(ctx, cfg); err != nil {
			return err
		}
		return runSingleScenario(ctx, cfg)
	}
	if err := ensureServicesRunning(ctx, cfg); err != nil {
		return err
	}

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

	var final summary
	switch strings.ToLower(cfg.mode) {
	case "smoke":
		final, err = runScenario(ctx, cfg, reducer, eventsWriter, sampler, scenarioSpec{
			phase:             "smoke",
			targetDevices:     10,
			fixedUsers:        8,
			duration:          cfg.proveDuration,
			reconnectFraction: 0.1,
			reconnectAfter:    20 * time.Second,
		})
	case "sweep":
		_, final, err = runScaleSearch(ctx, cfg, reducer, eventsWriter, sampler)
	case "sustain":
		final, err = runScenario(ctx, cfg, reducer, eventsWriter, sampler, scenarioSpec{
			phase:         "sustain",
			targetDevices: cfg.initialDevices,
			duration:      cfg.sustainDuration,
		})
	case "resilience":
		final, err = runScenario(ctx, cfg, reducer, eventsWriter, sampler, scenarioSpec{
			phase:                "resilience",
			targetDevices:        cfg.initialDevices,
			duration:             cfg.proveDuration,
			reconnectFraction:    0.30,
			reconnectAfter:       20 * time.Second,
			restartGatewayAfter:  40 * time.Second,
			restartDeliveryAfter: 60 * time.Second,
		})
	case "full":
		if _, err := runScenario(ctx, cfg, newReducer(), eventsWriter, sampler, scenarioSpec{
			phase:             "smoke",
			targetDevices:     10,
			fixedUsers:        8,
			duration:          45 * time.Second,
			reconnectFraction: 0.1,
			reconnectAfter:    15 * time.Second,
		}); err != nil {
			return err
		}
		maxPassing, _, err := runScaleSearch(ctx, cfg, reducer, eventsWriter, sampler)
		if err != nil {
			return err
		}
		sustainSummary, err := runScenario(ctx, cfg, reducer, eventsWriter, sampler, scenarioSpec{
			phase:         "sustain",
			targetDevices: maxPassing,
			duration:      cfg.sustainDuration,
		})
		if err != nil {
			return err
		}
		resilienceSummary, err := runScenario(ctx, cfg, reducer, eventsWriter, sampler, scenarioSpec{
			phase:                "resilience",
			targetDevices:        int(math.Max(1, math.Floor(float64(maxPassing)*0.7))),
			duration:             cfg.proveDuration,
			reconnectFraction:    0.30,
			reconnectAfter:       20 * time.Second,
			restartGatewayAfter:  40 * time.Second,
			restartDeliveryAfter: 60 * time.Second,
		})
		if err != nil {
			return err
		}
		final = sustainSummary
		final.Resilience = resilienceSummary.Resilience
		final.ResourceUsage = resilienceSummary.ResourceUsage
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
	if err != nil {
		return err
	}
	if err := writeSummaryFiles(cfg.runDir, final); err != nil {
		return err
	}
	log.Printf("load suite complete: %s", cfg.runDir)
	return nil
}

func runScaleSearch(ctx context.Context, cfg config, reducer *reducer, events *jsonlWriter, sampler *resourceSampler) (int, summary, error) {
	target := cfg.initialDevices
	lastPassing := cfg.initialDevices
	firstFail := 0
	var best summary
	for target <= cfg.maxDevices {
		result, err := runScenario(ctx, cfg, reducer, events, sampler, scenarioSpec{
			phase:         fmt.Sprintf("sweep-%d", target),
			targetDevices: target,
			duration:      cfg.proveDuration,
		})
		if err != nil {
			return 0, summary{}, err
		}
		if evaluateScalePass(result) {
			lastPassing = target
			best = result
			target *= 2
			continue
		}
		firstFail = target
		break
	}
	if firstFail == 0 {
		return lastPassing, best, nil
	}
	low := lastPassing
	high := firstFail
	for i := 0; i < 5; i++ {
		mid := low + ((high - low) / 2)
		if mid == low || mid == high {
			break
		}
		result, err := runScenario(ctx, cfg, reducer, events, sampler, scenarioSpec{
			phase:         fmt.Sprintf("binary-%d", mid),
			targetDevices: mid,
			duration:      cfg.proveDuration,
		})
		if err != nil {
			return 0, summary{}, err
		}
		if evaluateScalePass(result) {
			low = mid
			best = result
		} else {
			high = mid
		}
	}
	return low, best, nil
}

func runScenario(ctx context.Context, cfg config, reducer *reducer, events *jsonlWriter, sampler *resourceSampler, spec scenarioSpec) (summary, error) {
	localReducer := newReducer()
	pop, err := bootstrapPopulation(ctx, cfg, spec.phase, spec.targetDevices, spec.fixedUsers, events)
	if err != nil {
		return summary{}, err
	}
	for _, device := range pop.devices {
		device.attach(localReducer, events, spec.phase)
		if err := device.connect(ctx, ""); err != nil {
			return summary{}, err
		}
	}

	var gatewayRecovery time.Duration
	var deliveryRecovery time.Duration
	var backlogDrain time.Duration
	var onceReconnect sync.Once
	var onceGateway sync.Once
	var onceDelivery sync.Once
	startedAt := time.Now().UTC()
	stopAt := startedAt.Add(spec.duration)

	timersDone := make(chan struct{})
	go func() {
		defer close(timersDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			now := time.Now().UTC()
			if spec.reconnectAfter > 0 && now.After(startedAt.Add(spec.reconnectAfter)) {
				onceReconnect.Do(func() {
					triggerReconnectStorm(ctx, pop.devices, spec.reconnectFraction)
				})
			}
			if spec.restartGatewayAfter > 0 && now.After(startedAt.Add(spec.restartGatewayAfter)) {
				onceGateway.Do(func() {
					recoveryStart := time.Now().UTC()
					if err := restartComposeService(ctx, cfg.composeFile, "api"); err == nil {
						_ = waitForHTTP(ctx, strings.TrimRight(cfg.baseURL, "/")+"/healthz", 90*time.Second)
						gatewayRecovery = time.Since(recoveryStart)
						for _, device := range pop.devices {
							_ = device.reconnect(ctx, "gateway_restart")
						}
					}
				})
			}
			if spec.restartDeliveryAfter > 0 && now.After(startedAt.Add(spec.restartDeliveryAfter)) {
				onceDelivery.Do(func() {
					baselineLag := sampler.currentLagTotal()
					restartAt := time.Now().UTC()
					if err := restartComposeService(ctx, cfg.composeFile, "delivery-processor"); err == nil {
						time.Sleep(5 * time.Second)
						deliveryRecovery = time.Since(restartAt)
						backlogDrain = backlogDrainTime(sampler.lagSamplesSince(restartAt), baselineLag, 10)
					}
				})
			}
			if now.After(stopAt) {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	errCh := make(chan error, len(pop.conversations))
	runCtx, cancel := context.WithDeadline(ctx, stopAt)
	defer cancel()
	for _, conv := range pop.conversations {
		go func(conv *conversationFixture) {
			ticker := time.NewTicker(8 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case tick := <-ticker.C:
					sender := conv.nextSenderDevice()
					if sender == nil {
						continue
					}
					text := fmt.Sprintf("[%s] %s %d", spec.phase, conv.ID, tick.UnixNano())
					if err := sender.sendText(runCtx, conv.ID, text, conv.targetDeviceIDs(sender.deviceID)); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(conv)
	}

	select {
	case err := <-errCh:
		if err != nil {
			return summary{}, err
		}
	case <-runCtx.Done():
	}
	<-timersDone
	time.Sleep(5 * time.Second)

	for _, device := range pop.devices {
		device.close()
	}

	convergenceRate, err := measureConvergence(ctx, pop.conversations)
	if err != nil {
		return summary{}, err
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
	result := localReducer.snapshotSummary(summaryInput{
		concurrentUsers:     len(pop.users),
		concurrentDevices:   len(pop.devices),
		conversations:       len(pop.conversations),
		duration:            time.Since(startedAt),
		convergenceRate:     convergenceRate,
		restartRecoveryTime: maxDuration(gatewayRecovery, deliveryRecovery),
		backlogDrainTime:    backlogDrain,
		dbP95Latency:        dbP95,
		peakKafkaLag:        maxInt64(sampler.currentLagTotal(), peakKafkaLag),
		peakCPU:             peakCPU,
		peakMemory:          peakMemory,
	})
	_ = reducer
	return result, nil
}

func triggerReconnectStorm(ctx context.Context, devices []*deviceClient, fraction float64) {
	if fraction <= 0 {
		return
	}
	count := int(math.Ceil(float64(len(devices)) * fraction))
	if count <= 0 {
		count = 1
	}
	if count > len(devices) {
		count = len(devices)
	}
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(device *deviceClient) {
			defer wg.Done()
			_ = device.reconnect(ctx, "storm")
		}(devices[i])
	}
	wg.Wait()
}

func measureConvergence(ctx context.Context, conversations []*conversationFixture) (float64, error) {
	successful := 0
	total := 0
	for _, conversation := range conversations {
		hashes := map[string]struct{}{}
		conversationDevices := 0
		for _, participant := range conversation.Participants {
			for _, device := range participant.Devices {
				conversationDevices++
				target := fmt.Sprintf("%s/v1/conversations/%s/timeline?limit=500", strings.TrimRight(device.baseURL, "/"), conversation.ID)
				payload, err := device.authenticatedGetJSON(ctx, target)
				if err != nil {
					return 0, err
				}
				rawItems, _ := payload["items"].([]any)
				items := make([]map[string]any, 0, len(rawItems))
				for _, raw := range rawItems {
					if item, ok := raw.(map[string]any); ok {
						items = append(items, item)
					}
				}
				hash, err := convergenceHash(items)
				if err != nil {
					return 0, err
				}
				hashes[hash] = struct{}{}
				total++
			}
		}
		if len(hashes) == 1 {
			successful += conversationDevices
		}
	}
	if total == 0 {
		return 0, nil
	}
	return float64(successful) / float64(total), nil
}

func maxDuration(values ...time.Duration) time.Duration {
	var out time.Duration
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func filepathJoin(items ...string) string {
	return strings.ReplaceAll(strings.Join(items, "/"), "\\", "/")
}
