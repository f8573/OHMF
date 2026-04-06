package notification

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5"
	"ohmf/services/gateway/internal/push"
)

type Preferences struct {
	UserID                         string `json:"user_id"`
	DeviceID                       string `json:"device_id"`
	PushEnabled                    bool   `json:"push_enabled"`
	MuteUnknownSenders             bool   `json:"mute_unknown_senders"`
	ShowPreviews                   bool   `json:"show_previews"`
	MutedConversationNotifications bool   `json:"muted_conversation_notifications"`
	SendReadReceipts               bool   `json:"send_read_receipts"`
	SharePresence                  bool   `json:"share_presence"`
	ShareTyping                    bool   `json:"share_typing"`
}

type NotificationPayload struct {
	Title          string         `json:"title"`
	Body           string         `json:"body"`
	ConversationID string         `json:"conversation_id,omitempty"`
	Data           map[string]any `json:"data,omitempty"`
	Encrypted      bool           `json:"encrypted,omitempty"`
}

type subscriptionEnvelope struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		P256DH string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
}

type deliveryDeviceTarget struct {
	DeviceID                       string
	PushEnabled                    bool
	MutedConversationNotifications bool
}

func (h *Handler) sendNotification(ctx context.Context, userID string, p NotificationPayload) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = h.db.Exec(ctx, `
        INSERT INTO notifications (user_id, payload, status, created_at)
        VALUES ($1::uuid, $2::jsonb, 'pending', $3)
	`, userID, string(b), time.Now())
	return err
}

func (h *Handler) getPrefs(ctx context.Context, userID, deviceID string) (Preferences, error) {
	var prefs Preferences
	err := h.db.QueryRow(ctx, `
		SELECT user_id::text, device_id::text, push_enabled, mute_unknown_senders, show_previews, muted_conversation_notifications
		FROM notification_preferences
		WHERE user_id = $1::uuid AND device_id = $2::uuid
	`, userID, deviceID).Scan(&prefs.UserID, &prefs.DeviceID, &prefs.PushEnabled, &prefs.MuteUnknownSenders, &prefs.ShowPreviews, &prefs.MutedConversationNotifications)
	if err != nil {
		if err == pgx.ErrNoRows {
			prefs = Preferences{
				UserID:                         userID,
				DeviceID:                       deviceID,
				PushEnabled:                    true,
				ShowPreviews:                   true,
				MutedConversationNotifications: false,
			}
		} else {
			return Preferences{}, err
		}
	}
	privacy, err := h.getUserPrivacyPrefs(ctx, userID)
	if err != nil {
		return Preferences{}, err
	}
	prefs.SendReadReceipts = privacy.SendReadReceipts
	prefs.SharePresence = privacy.SharePresence
	prefs.ShareTyping = privacy.ShareTyping
	return prefs, nil
}

func (h *Handler) upsertPrefs(ctx context.Context, prefs Preferences) (Preferences, error) {
	if _, err := h.db.Exec(ctx, `
		INSERT INTO notification_preferences (
			user_id,
			device_id,
			push_enabled,
			mute_unknown_senders,
			show_previews,
			muted_conversation_notifications,
			created_at,
			updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, now(), now())
		ON CONFLICT (user_id, device_id)
		DO UPDATE SET
			push_enabled = EXCLUDED.push_enabled,
			mute_unknown_senders = EXCLUDED.mute_unknown_senders,
			show_previews = EXCLUDED.show_previews,
			muted_conversation_notifications = EXCLUDED.muted_conversation_notifications,
			updated_at = now()
	`, prefs.UserID, prefs.DeviceID, prefs.PushEnabled, prefs.MuteUnknownSenders, prefs.ShowPreviews, prefs.MutedConversationNotifications); err != nil {
		return Preferences{}, err
	}
	if err := h.upsertUserPrivacyPrefs(ctx, prefs); err != nil {
		return Preferences{}, err
	}
	return h.getPrefs(ctx, prefs.UserID, prefs.DeviceID)
}

type userPrivacyPreferences struct {
	SendReadReceipts bool
	SharePresence    bool
	ShareTyping      bool
}

func (h *Handler) getUserPrivacyPrefs(ctx context.Context, userID string) (userPrivacyPreferences, error) {
	var prefs userPrivacyPreferences
	err := h.db.QueryRow(ctx, `
		SELECT send_read_receipts, share_presence, share_typing
		FROM user_privacy_preferences
		WHERE user_id = $1::uuid
	`, userID).Scan(&prefs.SendReadReceipts, &prefs.SharePresence, &prefs.ShareTyping)
	if err != nil {
		if err == pgx.ErrNoRows {
			return userPrivacyPreferences{
				SendReadReceipts: true,
				SharePresence:    true,
				ShareTyping:      true,
			}, nil
		}
		return userPrivacyPreferences{}, err
	}
	return prefs, nil
}

func (h *Handler) upsertUserPrivacyPrefs(ctx context.Context, prefs Preferences) error {
	_, err := h.db.Exec(ctx, `
		INSERT INTO user_privacy_preferences (
			user_id,
			send_read_receipts,
			share_presence,
			share_typing,
			created_at,
			updated_at
		)
		VALUES ($1::uuid, $2, $3, $4, now(), now())
		ON CONFLICT (user_id)
		DO UPDATE SET
			send_read_receipts = EXCLUDED.send_read_receipts,
			share_presence = EXCLUDED.share_presence,
			share_typing = EXCLUDED.share_typing,
			updated_at = now()
	`, prefs.UserID, prefs.SendReadReceipts, prefs.SharePresence, prefs.ShareTyping)
	return err
}

func (h *Handler) DispatchPending(ctx context.Context, limit int) error {
	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id::text, user_id::text, payload, attempt_count
		FROM notifications
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	type pending struct {
		id           string
		userID       string
		payload      NotificationPayload
		attemptCount int
	}
	items := make([]pending, 0, limit)
	for rows.Next() {
		var id, userID string
		var raw []byte
		var attemptCount int
		if err := rows.Scan(&id, &userID, &raw, &attemptCount); err != nil {
			return err
		}
		var payload NotificationPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			payload = NotificationPayload{Title: "OHMF", Body: "New message"}
		}
		items = append(items, pending{id: id, userID: userID, payload: payload, attemptCount: attemptCount})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, item := range items {
		// Check if we should retry based on exponential backoff
		// Retry delays: 1s, 5s, 30s, 5m, then give up
		retryDelays := []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second, 5 * time.Minute}
		if item.attemptCount >= len(retryDelays) {
			// Max retries exceeded, mark as failed
			if _, err := tx.Exec(ctx, `
				UPDATE notifications
				SET status = 'failed',
				    updated_at = now()
				WHERE id = $1::uuid
			`, item.id); err != nil {
				return err
			}
			continue
		}

		if item.attemptCount > 0 {
			// Check if enough time has passed based on backoff schedule
			delayNeeded := retryDelays[item.attemptCount-1]
			var createdAt time.Time
			if err := tx.QueryRow(ctx, `
				SELECT created_at FROM notifications WHERE id = $1::uuid
			`, item.id).Scan(&createdAt); err != nil {
				return err
			}

			if time.Since(createdAt) < delayNeeded {
				// Not ready to retry yet, skip this one
				continue
			}
		}

		delivered := false
		lastErr := ""
		targets, err := h.eligibleDeviceTargets(ctx, item.userID, item.payload.ConversationID)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE notifications
				SET status = 'skipped',
				    attempt_count = attempt_count + 1,
				    updated_at = now()
				WHERE id = $1::uuid
			`, item.id); err != nil {
				return err
			}
			continue
		}
		deviceIDs := make([]string, 0, len(targets))
		for _, target := range targets {
			deviceIDs = append(deviceIDs, target.DeviceID)
		}

		// Try WebPush first (if enabled)
		if h.cfg.EnableWebPush {
			subscriptions, err := h.devices.ListWebPushSubscriptionsForDevices(ctx, item.userID, deviceIDs)
			if err == nil && len(subscriptions) > 0 {
				delivered, lastErr = h.dispatchWebPush(ctx, item.payload, subscriptions)
			}
		}

		// Try FCM for Android devices (if configured)
		if !delivered && h.fcmProv != nil {
			tokens, err := h.devices.ListPushTokensForProviderAndDevices(ctx, item.userID, "fcm", deviceIDs)
			if err == nil && len(tokens) > 0 {
				delivered, lastErr = h.dispatchFCM(ctx, item.userID, item.payload, tokens)
			} else if err != nil {
				lastErr = err.Error()
			}
		}

		// Try APNs for iOS devices (if configured)
		if !delivered && h.apnsProv != nil {
			tokens, err := h.devices.ListPushTokensForProviderAndDevices(ctx, item.userID, "apns", deviceIDs)
			if err == nil && len(tokens) > 0 {
				delivered, lastErr = h.dispatchAPNs(ctx, item.userID, item.payload, tokens)
			} else if err != nil {
				lastErr = err.Error()
			}
		}

		status := "skipped"
		if delivered {
			status = "delivered"
		} else if lastErr != "" {
			status = "pending" // Keep pending for retry on backoff
		}

		if _, err := tx.Exec(ctx, `
			UPDATE notifications
			SET status = $2,
			    attempt_count = attempt_count + 1,
			    delivered = $3,
			    delivered_at = CASE WHEN $3::bool THEN now() ELSE delivered_at END,
			    last_error = NULLIF($4, ''),
			    updated_at = now()
			WHERE id = $1::uuid
		`, item.id, status, delivered, lastErr); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (h *Handler) eligibleDeviceTargets(ctx context.Context, userID, conversationID string) ([]deliveryDeviceTarget, error) {
	conversationSuppressed := false
	if conversationID != "" {
		var archived bool
		var mutedUntil sql.NullTime
		err := h.db.QueryRow(ctx, `
			SELECT COALESCE(is_archived, false), muted_until
			FROM user_conversation_state
			WHERE user_id = $1::uuid AND conversation_id = $2::uuid
		`, userID, conversationID).Scan(&archived, &mutedUntil)
		if err == nil {
			conversationSuppressed = archived || (mutedUntil.Valid && mutedUntil.Time.After(time.Now().UTC()))
		} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}

	rows, err := h.db.Query(ctx, `
		SELECT
			d.id::text,
			COALESCE(np.push_enabled, true),
			COALESCE(np.muted_conversation_notifications, false)
		FROM devices d
		LEFT JOIN notification_preferences np
		  ON np.user_id = d.user_id
		 AND np.device_id = d.id
		WHERE d.user_id = $1::uuid
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	targets := make([]deliveryDeviceTarget, 0, 4)
	for rows.Next() {
		var target deliveryDeviceTarget
		if err := rows.Scan(&target.DeviceID, &target.PushEnabled, &target.MutedConversationNotifications); err != nil {
			return nil, err
		}
		if !target.PushEnabled {
			continue
		}
		if conversationSuppressed && !target.MutedConversationNotifications {
			continue
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

// dispatchWebPush sends via WebPush to browser subscriptions
func (h *Handler) dispatchWebPush(ctx context.Context, payload NotificationPayload, subscriptions []string) (bool, string) {
	delivered := false
	var lastErr string

	for _, raw := range subscriptions {
		env := subscriptionEnvelope{}
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			lastErr = err.Error()
			continue
		}
		sub := &webpush.Subscription{
			Endpoint: env.Endpoint,
			Keys: webpush.Keys{
				P256dh: env.Keys.P256DH,
				Auth:   env.Keys.Auth,
			},
		}
		body := payload.Body
		payloadBytes, _ := json.Marshal(map[string]any{
			"title":           payload.Title,
			"body":            body,
			"conversation_id": payload.ConversationID,
			"data":            payload.Data,
		})
		resp, err := webpush.SendNotification(payloadBytes, sub, &webpush.Options{
			Subscriber:      h.cfg.WebPushSubject,
			VAPIDPublicKey:  h.cfg.WebPushVAPIDPublicKey,
			VAPIDPrivateKey: h.cfg.WebPushVAPIDPrivateKey,
			TTL:             30,
			HTTPClient:      h.client,
		})
		if err != nil {
			lastErr = err.Error()
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			delivered = true
		} else {
			lastErr = resp.Status
		}
	}
	return delivered, lastErr
}

// dispatchFCM sends via Firebase Cloud Messaging to Android devices
func (h *Handler) dispatchFCM(ctx context.Context, userID string, payload NotificationPayload, tokens []string) (bool, string) {
	if h.fcmProv == nil {
		return false, "FCM provider not configured"
	}

	results, err := h.fcmProv.SendNotification(ctx, tokens, toPushPayload(payload))
	if err != nil {
		return false, err.Error()
	}

	delivered := false
	var lastErr string
	for _, result := range results {
		if result.Success {
			delivered = true
			continue
		}
		if result.Error != "" {
			lastErr = result.Error
		}
		if result.Permanent {
			_ = h.devices.RemovePushToken(ctx, userID, "fcm", result.Token)
		}
	}
	return delivered, lastErr
}

// dispatchAPNs sends via Apple Push Notifications to iOS devices
func (h *Handler) dispatchAPNs(ctx context.Context, userID string, payload NotificationPayload, tokens []string) (bool, string) {
	if h.apnsProv == nil {
		return false, "APNs provider not configured"
	}

	results, err := h.apnsProv.SendNotification(ctx, tokens, toPushPayload(payload))
	if err != nil {
		return false, err.Error()
	}

	delivered := false
	var lastErr string
	for _, result := range results {
		if result.Success {
			delivered = true
			continue
		}
		if result.Error != "" {
			lastErr = result.Error
		}
		if result.Permanent {
			_ = h.devices.RemovePushToken(ctx, userID, "apns", result.Token)
		}
	}
	return delivered, lastErr
}

// getDeviceTokensForProvider retrieves device tokens for a specific provider
func (h *Handler) HasUsableWebPush() bool {
	return h.cfg.EnableWebPush && h.cfg.WebPushVAPIDPublicKey != "" && h.cfg.WebPushVAPIDPrivateKey != ""
}

func (h *Handler) HasUsablePushProviders() bool {
	return h.HasUsableWebPush() || h.fcmProv != nil || h.apnsProv != nil
}

func IsMissingPrefs(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func toPushPayload(payload NotificationPayload) *push.NotificationPayload {
	return &push.NotificationPayload{
		Title:          payload.Title,
		Body:           payload.Body,
		ConversationID: payload.ConversationID,
		Data:           payload.Data,
	}
}
