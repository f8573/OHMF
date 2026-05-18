//go:build ignore
// +build ignore

// This file is an incomplete merge artifact kept only for reference.
// Exclude it from builds so module-wide CI does not try to compile it.

package auth

import (
	"encoding/json"
	"errors"
	"net/http"

	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/middleware"
)

type Handler struct {
	svc *Handler
}

func NewHandler(svc *Handler) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) StartPhone(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	resp, err := h.svc.StartPhoneVerification(r.Context(), req, r.RemoteAddr)
	if err != nil {
		handleError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) VerifyPhone(w http.ResponseWriter, r *http.Request) {
	var req VerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	resp, err := h.svc.VerifyPhone(r.Context(), req, r.RemoteAddr)
	if err != nil {
		handleError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) Refresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	resp, err := h.svc.Refresh(r.Context(), req)
	if err != nil {
		handleError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tokens": resp})
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	var req LogoutRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := h.svc.Logout(r.Context(), userID, req); err != nil {
		handleError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrChallengeNotFound):
		httpx.WriteError(w, r, http.StatusBadRequest, "challenge_not_found", err.Error(), nil)
	case errors.Is(err, ErrChallengeExpired):
		httpx.WriteError(w, r, http.StatusBadRequest, "challenge_expired", err.Error(), nil)
	case errors.Is(err, ErrInvalidOTP):
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_otp", err.Error(), nil)
	case errors.Is(err, ErrInvalidRefresh):
		httpx.WriteError(w, r, http.StatusUnauthorized, "invalid_refresh", err.Error(), nil)
	case errors.Is(err, ErrRateLimited):
		httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", err.Error(), nil)
	case errors.Is(err, ErrOTPDeliveryFailed):
		httpx.WriteError(w, r, http.StatusBadGateway, "otp_delivery_failed", err.Error(), nil)
	default:
		httpx.WriteError(w, r, http.StatusInternalServerError, "internal_error", err.Error(), nil)
	}
}


// --- Merged from service.go ---


type StartRequest struct {
	PhoneE164 string `json:"phone_e164"`
	Channel   string `json:"channel"`
}

type VerifyRequest struct {
	ChallengeID string `json:"challenge_id"`
	OTPCode     string `json:"otp_code"`
	Device      struct {
		Platform     string   `json:"platform"`
		DeviceName   string   `json:"device_name"`
		PushToken    string   `json:"push_token"`
		PublicKey    string   `json:"public_key"`
		Capabilities []string `json:"capabilities"`
	} `json:"device"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type LogoutRequest struct {
	DeviceID string `json:"device_id"`
}

var (
	ErrChallengeNotFound = errors.New("challenge_not_found")
	ErrChallengeExpired  = errors.New("challenge_expired")
	ErrInvalidOTP        = errors.New("invalid_otp")
	ErrInvalidRefresh    = errors.New("invalid_refresh")
	ErrRateLimited       = errors.New("rate_limited")
	ErrOTPDeliveryFailed = errors.New("otp_delivery_failed")
)

type Handler struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	tokens     *token.Service
	otp        otp.Provider
	accessTTL  time.Duration
	refreshTTL time.Duration
	cfg        config.Config
}

func NewHandler(db *pgxpool.Pool, redis *redis.Client, tokens *token.Service, otpProvider otp.Provider, accessTTL, refreshTTL time.Duration, cfg config.Config) *Handler {
	return &Service{db: db, redis: redis, tokens: tokens, otp: otpProvider, accessTTL: accessTTL, refreshTTL: refreshTTL, cfg: cfg}
}

func (s *Handler) StartPhoneVerification(ctx context.Context, req StartRequest, ip string) (map[string]any, error) {
	phoneE164 := phone.NormalizeE164(req.PhoneE164)
	if phoneE164 == "" {
		return nil, fmt.Errorf("phone_required")
	}
	allowed, err := h.allowRate(ctx, "otp:start:"+phoneE164, 5, 10*time.Minute)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrRateLimited
	}
	if ip != "" {
		allowedIP, err := h.allowRate(ctx, "otp:start:ip:"+ip, 20, 10*time.Minute)
		if err != nil {
			return nil, err
		}
		if !allowedIP {
			return nil, ErrRateLimited
		}
	}

	// rate limit by subnet (simple IPv4 /16 heuristic)
	if ip != "" {
		parts := strings.Split(ip, ".")
		if len(parts) >= 2 {
			subnet := parts[0] + "." + parts[1]
			allowedSubnet, err := h.allowRate(ctx, "otp:start:subnet:"+subnet, 100, 10*time.Minute)
			if err != nil {
				return nil, err
			}
			if !allowedSubnet {
				return nil, ErrRateLimited
			}
		}
	}

	id := uuid.New()
	code, err := h.generateOTPCode()
	if err != nil {
		return nil, err
	}

	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO phone_verification_challenges (id, phone_e164, otp_code_hash, channel, attempts_remaining, expires_at)
		VALUES ($1, $2, $3, $4, 5, now() + interval '5 minute')
	`, id, phoneE164, otp.Hash(code), req.Channel)
	if err != nil {
		return nil, err
	}
	if h.otp == nil {
		return nil, ErrOTPDeliveryFailed
	}
	if err := h.otp.SendCode(ctx, phoneE164, code); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOTPDeliveryFailed, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	// If the phone or IP has elevated activity, signal escalation to client
	escalated := false
	if n, _ := h.redis.Get(ctx, "otp:start:"+phoneE164).Int64(); n > 3 {
		escalated = true
	}
	if ip != "" {
		if n, _ := h.redis.Get(ctx, "otp:start:ip:"+ip).Int64(); n > 10 {
			escalated = true
		}
	}

	return map[string]any{
		"challenge_id":    id.String(),
		"expires_in_sec":  300,
		"retry_after_sec": 30,
		"otp_strategy":    "SMS",
		"escalated":       escalated,
		"provider":        h.otp.Name(),
	}, nil
}

func (s *Handler) VerifyPhone(ctx context.Context, req VerifyRequest, ip string) (map[string]any, error) {
	allowed, err := h.allowRate(ctx, "otp:verify:"+req.ChallengeID, 10, 10*time.Minute)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrRateLimited
	}
	if ip != "" {
		allowedIP, err := h.allowRate(ctx, "otp:verify:ip:"+ip, 50, 10*time.Minute)
		if err != nil {
			return nil, err
		}
		if !allowedIP {
			return nil, ErrRateLimited
		}
	}

	// device-based rate limiting (use public key or push token if present)
	deviceFingerprint := ""
	if req.Device.PublicKey != "" {
		deviceFingerprint = fmt.Sprintf("pk:%x", sha256.Sum256([]byte(req.Device.PublicKey)))
	} else if req.Device.PushToken != "" {
		deviceFingerprint = "pt:" + req.Device.PushToken
	}
	if deviceFingerprint != "" {
		allowedDevice, err := h.allowRate(ctx, "otp:verify:device:"+deviceFingerprint, 10, 10*time.Minute)
		if err != nil {
			return nil, err
		}
		if !allowedDevice {
			return nil, ErrRateLimited
		}
	}

	challengeID, err := uuid.Parse(req.ChallengeID)
	if err != nil {
		return nil, ErrChallengeNotFound
	}

	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var phoneE164, otpHash string
	var attemptsRemaining int
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT phone_e164, otp_code_hash, attempts_remaining, expires_at
		FROM phone_verification_challenges
		WHERE id = $1 AND consumed_at IS NULL
		FOR UPDATE
	`, challengeID).Scan(&phoneE164, &otpHash, &attemptsRemaining, &expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChallengeNotFound
		}
		return nil, err
	}
	if time.Now().After(expiresAt) {
		return nil, ErrChallengeExpired
	}
	if attemptsRemaining <= 0 || otp.Hash(req.OTPCode) != otpHash {
		_, _ = tx.Exec(ctx, `UPDATE phone_verification_challenges SET attempts_remaining = GREATEST(attempts_remaining - 1, 0) WHERE id = $1`, challengeID)
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, ErrInvalidOTP
	}

	_, err = tx.Exec(ctx, `UPDATE phone_verification_challenges SET consumed_at = now() WHERE id = $1`, challengeID)
	if err != nil {
		return nil, err
	}

	var userID string
	err = tx.QueryRow(ctx, `
		INSERT INTO users (primary_phone_e164, phone_verified_at)
		VALUES ($1, now())
		ON CONFLICT (primary_phone_e164)
		DO UPDATE SET phone_verified_at = EXCLUDED.phone_verified_at, updated_at = now()
		RETURNING id::text
	`, phoneE164).Scan(&userID)
	if err != nil {
		return nil, err
	}

	// Promote phone-based conversations into first-class user conversations so
	// newly verified users immediately receive existing messages.
	if _, err := tx.Exec(ctx, `
		WITH matched AS (
			SELECT DISTINCT cem.conversation_id
			FROM conversation_external_members cem
			JOIN external_contacts ec ON ec.id = cem.external_contact_id
			WHERE ec.phone_e164 = $1
		)
		INSERT INTO conversation_members (conversation_id, user_id, role)
		SELECT m.conversation_id, $2::uuid, 'MEMBER'
		FROM matched m
		ON CONFLICT (conversation_id, user_id) DO NOTHING
	`, phoneE164, userID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		WITH matched AS (
			SELECT DISTINCT cem.conversation_id
			FROM conversation_external_members cem
			JOIN external_contacts ec ON ec.id = cem.external_contact_id
			WHERE ec.phone_e164 = $1
		)
		UPDATE conversations c
		SET type = 'DM', transport_policy = 'AUTO', updated_at = now()
		WHERE c.id IN (SELECT conversation_id FROM matched)
	`, phoneE164); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM conversation_external_members cem
		USING external_contacts ec
		WHERE cem.external_contact_id = ec.id
		  AND ec.phone_e164 = $1
	`, phoneE164); err != nil {
		return nil, err
	}

	var deviceID string
	deviceCapabilities := normalizeDeviceCapabilities(req.Device.Platform, req.Device.Capabilities)
	err = tx.QueryRow(ctx, `
		INSERT INTO devices (user_id, platform, device_name, capabilities, push_token, public_key, last_seen_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING id::text
	`, userID, req.Device.Platform, req.Device.DeviceName, deviceCapabilities, nullable(req.Device.PushToken), nullable(req.Device.PublicKey)).Scan(&deviceID)
	if err != nil {
		return nil, err
	}

	refresh, err := randomToken()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
		VALUES ($1, $2, $3, now() + ($4 || ' seconds')::interval)
	`, userID, deviceID, hashToken(refresh), strconv.Itoa(int(h.refreshTTL.Seconds())))
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// determine feature profiles to include in the access token
	profiles := h.decideProfilesForPlatform(req.Device.Platform)
	access, err := h.tokens.IssueAccess(userID, deviceID, h.accessTTL, profiles)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"user": map[string]any{
			"user_id":            userID,
			"primary_phone_e164": phoneE164,
		},
		"device": map[string]any{
			"device_id": deviceID,
			"platform":  req.Device.Platform,
		},
		"tokens": map[string]any{
			"access_token":  access,
			"refresh_token": refresh,
		},
	}, nil
}

func (s *Handler) Refresh(ctx context.Context, req RefreshRequest) (map[string]any, error) {
	h := hashToken(req.RefreshToken)
	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var tokenID, userID, deviceID string
	err = tx.QueryRow(ctx, `
		SELECT id::text, user_id::text, COALESCE(device_id::text, '')
		FROM refresh_tokens
		WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > now()
		FOR UPDATE
	`, h).Scan(&tokenID, &userID, &deviceID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidRefresh
		}
		return nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1`, tokenID)
	if err != nil {
		return nil, err
	}
	newRefresh, err := randomToken()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
		VALUES ($1, NULLIF($2, '')::uuid, $3, now() + ($4 || ' seconds')::interval)
	`, userID, deviceID, hashToken(newRefresh), strconv.Itoa(int(h.refreshTTL.Seconds())))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	// determine profiles based on device platform if available
	var profiles []string
	if deviceID != "" {
		var platform string
		if err := h.db.QueryRow(ctx, `SELECT platform FROM devices WHERE id = $1`, deviceID).Scan(&platform); err == nil {
			profiles = h.decideProfilesForPlatform(platform)
		}
	}
	access, err := h.tokens.IssueAccess(userID, deviceID, h.accessTTL, profiles)
	if err != nil {
		return nil, err
	}
	return map[string]any{"access_token": access, "refresh_token": newRefresh}, nil
}

func (s *Handler) decideProfilesForPlatform(platform string) []string {
	profiles := []string{"CORE_OTT"}
	switch strings.ToUpper(platform) {
	case "ANDROID":
		profiles = append(profiles, "MINIAPP_RUNTIME")
		if h.cfg.ClaimAndroidCarrier {
			profiles = append(profiles, "ANDROID_CARRIER")
		}
	case "WEB":
		profiles = append(profiles, "WEB_RELAY")
	}
	return profiles
}

func normalizeDeviceCapabilities(platform string, requested []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(requested)+1)
	for _, capability := range requested {
		capability = strings.TrimSpace(strings.ToUpper(capability))
		if capability == "" {
			continue
		}
		if _, exists := seen[capability]; exists {
			continue
		}
		seen[capability] = struct{}{}
		out = append(out, capability)
	}
	if strings.EqualFold(platform, "WEB") {
		if _, exists := seen["MINI_APPS"]; !exists {
			out = append(out, "MINI_APPS")
		}
	}
	return out
}

func randomOTPCode() (string, error) {
	var value uint32
	if err := binaryReadRand(&value); err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", value%1000000), nil
}

func (s *Handler) generateOTPCode() (string, error) {
	if s == nil || h.otp == nil {
		return randomOTPCode()
	}
	if h.otp.Name() == "dev" {
		return "123456", nil
	}
	return randomOTPCode()
}

func binaryReadRand(dst *uint32) error {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return err
	}
	*dst = uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3])
	return nil
}

func (s *Handler) Logout(ctx context.Context, userID string, req LogoutRequest) error {
	if req.DeviceID != "" {
		_, err := h.db.Exec(ctx, `
			UPDATE refresh_tokens
			SET revoked_at = now()
			WHERE user_id = $1 AND device_id = $2::uuid AND revoked_at IS NULL
		`, userID, req.DeviceID)
		return err
	}
	_, err := h.db.Exec(ctx, `
		UPDATE refresh_tokens
		SET revoked_at = now()
		WHERE user_id = $1 AND revoked_at IS NULL
	`, userID)
	return err
}

func (s *Handler) allowRate(ctx context.Context, key string, limit int64, window time.Duration) (bool, error) {
	n, err := h.redis.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		if err := h.redis.Expire(ctx, key, window).Err(); err != nil {
			return false, err
		}
	}
	return n <= limit, nil
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func hashToken(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

