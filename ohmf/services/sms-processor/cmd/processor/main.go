package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	brokers := splitCSV(getenv("APP_KAFKA_BROKERS", "localhost:9092"))
	smsTopic := getenv("APP_KAFKA_SMS_DISPATCH_TOPIC", "msg.sms.dispatch.v1")
	deliveryTopic := getenv("APP_KAFKA_DELIVERY_TOPIC", "msg.delivery.v1")
	dlqTopic := getenv("APP_KAFKA_SMS_DLQ_TOPIC", "msg.sms.dlq.v1")
	startMetricsServer(getenv("APP_METRICS_ADDR", ":9093"))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     getenv("APP_KAFKA_GROUP_ID", "sms-processor-v1"),
		Topic:       smsTopic,
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	deliveryWriter := writer(brokers, deliveryTopic)
	defer deliveryWriter.Close()
	dlqWriter := writer(brokers, dlqTopic)
	defer dlqWriter.Close()

	log.Printf("sms-processor started")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			recordProcessorRetry("fetch_failed")
			log.Printf("fetch failed: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		startedAt := time.Now()
		if err := process(ctx, deliveryWriter, msg); err != nil {
			recordProcessorResult("failure", time.Since(startedAt))
			log.Printf("process failed: %v", err)
			_ = publishDLQ(ctx, dlqWriter, msg, err)
		} else {
			recordProcessorResult("success", time.Since(startedAt))
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			recordProcessorRetry("commit_failed")
			log.Printf("commit failed: %v", err)
		}
	}
}

func process(ctx context.Context, deliveryWriter *kafka.Writer, msg kafka.Message) error {
	var event map[string]any
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}
	// Local/dev stub: SMS dispatch is modeled as successful enqueue.
	event["sms_status"] = "SENT"
	event["sms_status_updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	body, _ := json.Marshal(event)
	return deliveryWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: body,
		Time:  time.Now().UTC(),
	})
}

func publishDLQ(ctx context.Context, dlqWriter *kafka.Writer, msg kafka.Message, cause error) error {
	body, _ := json.Marshal(map[string]any{
		"cause":     cause.Error(),
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"value":     string(msg.Value),
		"at":        time.Now().UTC().Format(time.RFC3339Nano),
	})
	return dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: body,
		Time:  time.Now().UTC(),
	})
}

func writer(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}

func splitCSV(v string) []string {
	raw := strings.Split(v, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trim := strings.TrimSpace(item)
		if trim == "" {
			continue
		}
		out = append(out, trim)
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func getenv(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
