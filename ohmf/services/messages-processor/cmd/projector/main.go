package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
)

type config struct {
	KafkaBrokers        string
	KafkaPersistedTopic string
	KafkaGroupID        string
	CassandraHosts      string
	CassandraKeyspace   string
}

type persistedEvent struct {
	MessageID      string         `json:"message_id"`
	ConversationID string         `json:"conversation_id"`
	SenderUserID   string         `json:"sender_user_id"`
	ContentType    string         `json:"content_type"`
	Content        map[string]any `json:"content"`
	ServerOrder    int64          `json:"server_order"`
	Transport      string         `json:"transport"`
	PersistedAtMS  int64          `json:"persisted_at_ms"`
}

func main() {
	ctx := context.Background()
	cfg := loadConfig()

	cass, err := newCassandra(cfg)
	if err != nil {
		log.Fatalf("cassandra init failed: %v", err)
	}
	defer cass.Close()
	if err := ensureCassandraSchema(ctx, cass, cfg.CassandraKeyspace); err != nil {
		log.Fatalf("cassandra schema failed: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     splitCSV(cfg.KafkaBrokers),
		GroupID:     cfg.KafkaGroupID,
		Topic:       cfg.KafkaPersistedTopic,
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	log.Printf("messages-projector started")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch failed: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if err := projectMessage(ctx, cass, msg.Value); err != nil {
			log.Printf("project failed: %v", err)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}

func projectMessage(ctx context.Context, cass *gocql.Session, body []byte) error {
	var evt persistedEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return err
	}
	msgUUID, err := gocql.ParseUUID(evt.MessageID)
	if err != nil {
		return err
	}
	convUUID, err := gocql.ParseUUID(evt.ConversationID)
	if err != nil {
		return err
	}
	senderUUID, err := gocql.ParseUUID(evt.SenderUserID)
	if err != nil {
		return err
	}
	contentJSON, _ := json.Marshal(evt.Content)
	persistedAt := time.UnixMilli(evt.PersistedAtMS).UTC()
	bucket := persistedAt.Format("20060102")
	return cass.Query(`
		INSERT INTO messages_by_conversation
		(conversation_id, bucket_yyyymmdd, server_order, message_id, sender_user_id, content_type, content_json, transport, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, convUUID, bucket, evt.ServerOrder, msgUUID, senderUUID, evt.ContentType, string(contentJSON), evt.Transport, persistedAt).WithContext(ctx).Exec()
}

func loadConfig() config {
	return config{
		KafkaBrokers:        getenv("APP_KAFKA_BROKERS", "localhost:9092"),
		KafkaPersistedTopic: getenv("APP_KAFKA_PERSISTED_TOPIC", "msg.persisted.v1"),
		KafkaGroupID:        getenv("APP_KAFKA_GROUP_ID", "messages-projector-v1"),
		CassandraHosts:      getenv("APP_CASSANDRA_HOSTS", "localhost:9042"),
		CassandraKeyspace:   getenv("APP_CASSANDRA_KEYSPACE", "ohmf_messages"),
	}
}

func newCassandra(cfg config) (*gocql.Session, error) {
	cluster := gocql.NewCluster(splitCSV(cfg.CassandraHosts)...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second

	systemSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer systemSession.Close()
	createKeyspace := "CREATE KEYSPACE IF NOT EXISTS " + cfg.CassandraKeyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1}"
	if err := systemSession.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	cluster.Keyspace = cfg.CassandraKeyspace
	return cluster.CreateSession()
}

func ensureCassandraSchema(ctx context.Context, s *gocql.Session, keyspace string) error {
	_ = keyspace
	return s.Query(`
		CREATE TABLE IF NOT EXISTS messages_by_conversation (
			conversation_id uuid,
			bucket_yyyymmdd text,
			server_order bigint,
			message_id uuid,
			sender_user_id uuid,
			content_type text,
			content_json text,
			transport text,
			created_at timestamp,
			PRIMARY KEY ((conversation_id, bucket_yyyymmdd), server_order)
		) WITH CLUSTERING ORDER BY (server_order DESC)
		AND compaction = {'class': 'TimeWindowCompactionStrategy'}
		AND default_time_to_live = 31536000
	`).WithContext(ctx).Exec()
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
