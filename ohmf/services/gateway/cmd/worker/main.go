package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto/sha256"
	"net/http"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/db"
	"ohmf/services/gateway/internal/deviceattestation"
	"ohmf/services/gateway/internal/devices"
	"ohmf/services/gateway/internal/notification"
	"ohmf/services/gateway/internal/observability"
	"ohmf/services/gateway/internal/push"
	"ohmf/services/gateway/internal/replication"
	wk "ohmf/services/gateway/internal/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	logger := observability.NewLogger(cfg.LogLevel)
	ctx := context.Background()
	startWorkerMetricsServer(os.Getenv("APP_METRICS_ADDR"))

	pool, err := db.NewPool(ctx, cfg.DBDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("db connection failed")
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal().Err(err).Msg("redis connection failed")
	}
	defer rdb.Close()

	replicationStore := replication.NewStore(pool, rdb)
	attestationVerifier := deviceattestation.NewVerifier(deviceattestation.Config{
		Secret:       cfg.DeviceAttestationSecret,
		AndroidAppID: cfg.AttestationAndroidAppID,
		IOSAppID:     cfg.AttestationIOSAppID,
		WebAppID:     cfg.AttestationWebAppID,
	})
	deviceSvc := devices.NewService(pool, func() []byte { sum := sha256.Sum256([]byte(cfg.PushSubscriptionKey)); return sum[:] }(), attestationVerifier, cfg.AttestationChallengeTTL)
	notificationHandler := notification.NewHandler(pool, deviceSvc, cfg)

	// Initialize push providers
	if cfg.FirebaseCredentialsPath != "" {
		fcmProv, err := push.NewFCMProvider(ctx, cfg.FirebaseCredentialsPath)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to initialize FCM provider")
		} else {
			notificationHandler.WithFCMProvider(fcmProv)
			defer fcmProv.Close()
		}
	}

	// Try token-based APNs first, fall back to certificate if needed
	if cfg.APNsKeyPath != "" && cfg.APNsKeyID != "" && cfg.APNsTeamID != "" {
		apnsProv, err := push.NewAPNsProviderWithToken(ctx, cfg.APNsKeyPath, cfg.APNsKeyID, cfg.APNsTeamID, cfg.APNsBundleID)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to initialize APNs token provider, trying certificate")
			// Fall back to certificate-based auth
			if cfg.APNsCertPath != "" && cfg.APNsKeyPath != "" {
				apnsProv, err = push.NewAPNsProvider(ctx, cfg.APNsCertPath, cfg.APNsKeyPath, cfg.APNsBundleID)
				if err != nil {
					logger.Warn().Err(err).Msg("failed to initialize APNs certificate provider")
				} else {
					notificationHandler.WithAPNsProvider(apnsProv)
					defer apnsProv.Close()
				}
			}
		} else {
			notificationHandler.WithAPNsProvider(apnsProv)
			defer apnsProv.Close()
		}
	}

	// create runner and workers
	runner := wk.NewRunner()
	enabledWorkers := workerSetFromEnv(os.Getenv("APP_ENABLED_WORKERS"))
	if workerEnabled(enabledWorkers, "media") {
		runner.Add(wk.NewMediaWorker(pool))
	}
	if workerEnabled(enabledWorkers, "notification") {
		runner.Add(wk.NewNotificationWorker(notificationHandler))
	}
	if workerEnabled(enabledWorkers, "abuse") {
		runner.Add(wk.NewAbuseAggregatorWorker(pool))
	}
	if workerEnabled(enabledWorkers, "relay_retry") {
		runner.Add(wk.NewRelayRetryWorker(pool))
	}
	if workerEnabled(enabledWorkers, "sync_fanout") {
		runner.Add(wk.NewSyncFanoutWorker(replicationStore, cfg.DBDSN, cfg.SyncFanoutBatchSize, cfg.SyncFanoutFallbackPoll, cfg.SyncFanoutNotifyChannel))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := runner.StartAll(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start workers")
		return
	}

	// wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info().Msg("shutting down workers")
	// allow short grace
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	_ = runner.StopAll(stopCtx)
}

func startWorkerMetricsServer(addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", observability.MetricsHandler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		server := &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		_ = server.ListenAndServe()
	}()
}

func workerSetFromEnv(value string) map[string]struct{} {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	out := make(map[string]struct{})
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(strings.ToLower(item))
		if item == "" {
			continue
		}
		out[item] = struct{}{}
	}
	return out
}

func workerEnabled(enabled map[string]struct{}, name string) bool {
	if len(enabled) == 0 {
		return true
	}
	_, ok := enabled[strings.TrimSpace(strings.ToLower(name))]
	return ok
}
