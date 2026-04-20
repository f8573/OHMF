package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"ohmf/services/gateway/internal/observability"
	"ohmf/services/gateway/internal/replication"
)

type SyncFanoutWorker struct {
	store        *replication.Store
	dbDSN        string
	batchSize    int
	fallbackPoll time.Duration
	notifyChan   string
	stop         chan struct{}
	stopOnce     sync.Once
}

func NewSyncFanoutWorker(store *replication.Store, dbDSN string, batchSize int, fallbackPoll time.Duration, notifyChan string) *SyncFanoutWorker {
	return &SyncFanoutWorker{
		store:        store,
		dbDSN:        strings.TrimSpace(dbDSN),
		batchSize:    batchSize,
		fallbackPoll: fallbackPoll,
		notifyChan:   strings.TrimSpace(notifyChan),
		stop:         make(chan struct{}),
	}
}

func (w *SyncFanoutWorker) Name() string { return "sync_fanout" }

func (w *SyncFanoutWorker) Start(ctx context.Context) error {
	if w.store == nil {
		return nil
	}
	if w.batchSize <= 0 {
		w.batchSize = 100
	}
	if w.fallbackPoll <= 0 {
		w.fallbackPoll = time.Second
	}
	if w.notifyChan == "" {
		w.notifyChan = "ohmf_domain_events"
	}
	if err := w.drainUntilEmpty(ctx); err != nil {
		return err
	}
	if w.dbDSN == "" {
		return w.runPolling(ctx)
	}
	listener, err := pgx.Connect(ctx, w.dbDSN)
	if err != nil {
		return w.runPolling(ctx)
	}
	defer listener.Close(ctx)
	if _, err := listener.Exec(ctx, "listen "+quoteIdentifier(w.notifyChan)); err != nil {
		return w.runPolling(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.stop:
			return nil
		default:
		}
		wakeCtx, cancel := context.WithTimeout(ctx, w.fallbackPoll)
		_, err := listener.WaitForNotification(wakeCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if strings.Contains(strings.ToLower(err.Error()), "timeout") || err == context.DeadlineExceeded {
				observability.RecordReplicationWakeup("poll")
			} else {
				select {
				case <-ctx.Done():
					return nil
				case <-w.stop:
					return nil
				case <-time.After(750 * time.Millisecond):
				}
				continue
			}
		} else {
			observability.RecordReplicationWakeup("notify")
		}
		if err := w.drainUntilEmpty(ctx); err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-w.stop:
				return nil
			case <-time.After(750 * time.Millisecond):
			}
		}
	}
}

func (w *SyncFanoutWorker) Stop(ctx context.Context) error {
	w.stopOnce.Do(func() { close(w.stop) })
	return nil
}

func (w *SyncFanoutWorker) runPolling(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.stop:
			return nil
		default:
		}
		observability.RecordReplicationWakeup("poll")
		if err := w.drainUntilEmpty(ctx); err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-w.stop:
				return nil
			case <-time.After(750 * time.Millisecond):
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-w.stop:
			return nil
		case <-time.After(w.fallbackPoll):
		}
	}
}

func (w *SyncFanoutWorker) drainUntilEmpty(ctx context.Context) error {
	for {
		processed, err := w.store.ProcessBatch(ctx, w.batchSize)
		if err != nil {
			return err
		}
		if processed == 0 {
			return nil
		}
	}
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
