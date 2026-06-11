package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func writeSummaryFiles(runDir string, result summary) error {
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(runDir, "summary.json"), body, 0o644); err != nil {
		return err
	}

	var builder strings.Builder
	builder.WriteString("# OHMF Load Report\n\n")
	builder.WriteString("## Workload\n")
	builder.WriteString(fmt.Sprintf("- concurrent users: %d\n", result.Workload.ConcurrentUsers))
	builder.WriteString(fmt.Sprintf("- concurrent devices: %d\n", result.Workload.ConcurrentDevices))
	builder.WriteString(fmt.Sprintf("- conversations: %d\n", result.Workload.Conversations))
	builder.WriteString(fmt.Sprintf("- average recipients/message: %.2f\n", result.Workload.AverageRecipientsPerMsg))
	builder.WriteString(fmt.Sprintf("- message rate: %.2f\n", result.Workload.MessageRate))
	builder.WriteString(fmt.Sprintf("- test duration: %s\n", result.Workload.TestDuration))
	builder.WriteString(fmt.Sprintf("- connect readiness percentage: %.2f\n", result.Workload.ConnectReadinessPercent))
	builder.WriteString(fmt.Sprintf("- connect phase duration: %s\n", result.Workload.ConnectPhaseDuration))
	builder.WriteString(fmt.Sprintf("- first failure phase: %s\n", fallbackText(result.Workload.FirstFailurePhase)))
	builder.WriteString(fmt.Sprintf("- first failure reason: %s\n\n", fallbackText(result.Workload.FirstFailureReason)))

	builder.WriteString("## Performance\n")
	builder.WriteString(fmt.Sprintf("- messages/sec: %.2f\n", result.Performance.MessagesPerSecond))
	builder.WriteString(fmt.Sprintf("- deliveries/sec: %.2f\n", result.Performance.DeliveriesPerSecond))
	builder.WriteString(fmt.Sprintf("- p50/p95/p99 send-to-ack latency: %s / %s / %s\n", result.Performance.P50SendToAckLatency, result.Performance.P95SendToAckLatency, result.Performance.P99SendToAckLatency))
	builder.WriteString(fmt.Sprintf("- p50/p95/p99 end-to-end delivery latency: %s / %s / %s\n\n", result.Performance.P50EndToEndDeliveryLatency, result.Performance.P95EndToEndDeliveryLatency, result.Performance.P99EndToEndDeliveryLatency))

	builder.WriteString("## Correctness\n")
	builder.WriteString(fmt.Sprintf("- expected deliveries: %d\n", result.Correctness.ExpectedDeliveries))
	builder.WriteString(fmt.Sprintf("- successful deliveries: %d\n", result.Correctness.SuccessfulDeliveries))
	builder.WriteString(fmt.Sprintf("- lost deliveries: %d\n", result.Correctness.LostDeliveries))
	builder.WriteString(fmt.Sprintf("- duplicate deliveries: %d\n", result.Correctness.DuplicateDeliveries))
	builder.WriteString(fmt.Sprintf("- out-of-order deliveries: %d\n", result.Correctness.OutOfOrderDeliveries))
	builder.WriteString(fmt.Sprintf("- multi-device convergence rate: %.4f\n\n", result.Correctness.MultiDeviceConvergenceRate))

	builder.WriteString("## Resilience\n")
	builder.WriteString(fmt.Sprintf("- reconnect success rate: %.4f\n", result.Resilience.ReconnectSuccessRate))
	builder.WriteString(fmt.Sprintf("- restart recovery time: %s\n", result.Resilience.RestartRecoveryTime))
	builder.WriteString(fmt.Sprintf("- backlog drain time: %s\n\n", result.Resilience.BacklogDrainTime))

	builder.WriteString("## Resource Usage\n")
	builder.WriteString(fmt.Sprintf("- peak CPU: %s\n", result.ResourceUsage.PeakCPU))
	builder.WriteString(fmt.Sprintf("- peak memory: %s\n", result.ResourceUsage.PeakMemory))
	builder.WriteString(fmt.Sprintf("- DB p95 latency: %s\n", result.ResourceUsage.DBP95Latency))
	builder.WriteString(fmt.Sprintf("- peak Kafka lag: %d\n", result.ResourceUsage.PeakKafkaLag))

	return os.WriteFile(filepath.Join(runDir, "summary.md"), []byte(builder.String()), 0o644)
}

func writeCampaignSummaryFiles(runDir string, result campaignSummary) error {
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(runDir, "campaign_summary.json"), body, 0o644); err != nil {
		return err
	}

	var builder strings.Builder
	builder.WriteString("# OHMF Load Campaign\n\n")
	builder.WriteString(fmt.Sprintf("- first bottleneck: %s\n\n", fallbackText(result.FirstBottleneck)))
	if result.ActiveHeadline != nil {
		builder.WriteString("## Recruiter-Facing Active Summary\n")
		builder.WriteString(formatHeadlineSummary(*result.ActiveHeadline))
		builder.WriteString("\n")
	}
	if result.ConnectedCeiling != nil {
		builder.WriteString("## Connected Ceiling Summary\n")
		builder.WriteString(formatHeadlineSummary(*result.ConnectedCeiling))
		builder.WriteString("\n")
	}
	if result.Resilience != nil {
		builder.WriteString("## Resilience Summary\n")
		builder.WriteString(formatHeadlineSummary(*result.Resilience))
		builder.WriteString("\n")
	}
	builder.WriteString("## Tier Table\n")
	builder.WriteString("| label | profile | target | actual | passed | readiness% | failure phase | failure reason | msg/s | deliv/s |\n")
	builder.WriteString("| --- | --- | ---: | ---: | --- | ---: | --- | --- | ---: | ---: |\n")
	for _, row := range result.Tiers {
		builder.WriteString(fmt.Sprintf("| %s | %s | %d | %d | %t | %.2f | %s | %s | %.2f | %.2f |\n",
			row.Label,
			row.Profile,
			row.TargetDevices,
			row.ActualDevices,
			row.Passed,
			row.ConnectReadiness,
			fallbackText(row.FirstFailurePhase),
			fallbackText(row.FirstFailureReason),
			row.MessagesPerSecond,
			row.DeliveriesPerSecond,
		))
	}
	return os.WriteFile(filepath.Join(runDir, "campaign_summary.md"), []byte(builder.String()), 0o644)
}

func formatHeadlineSummary(result summary) string {
	return fmt.Sprintf(
		"- devices: %d\n- duration: %s\n- deliveries: %d/%d\n- p95 send-to-ack: %s\n- p95 end-to-end: %s\n- loss/duplicate/out-of-order: %d / %d / %d\n- convergence: %.4f\n",
		result.Workload.ConcurrentDevices,
		result.Workload.TestDuration,
		result.Correctness.SuccessfulDeliveries,
		result.Correctness.ExpectedDeliveries,
		result.Performance.P95SendToAckLatency,
		result.Performance.P95EndToEndDeliveryLatency,
		result.Correctness.LostDeliveries,
		result.Correctness.DuplicateDeliveries,
		result.Correctness.OutOfOrderDeliveries,
		result.Correctness.MultiDeviceConvergenceRate,
	)
}

func fallbackText(value string) string {
	if strings.TrimSpace(value) == "" {
		return "none"
	}
	return value
}
