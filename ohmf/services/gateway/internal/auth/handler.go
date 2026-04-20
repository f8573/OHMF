package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/devices"
	"ohmf/services/gateway/internal/httpx"
	"ohmf/services/gateway/internal/middleware"
	"ohmf/services/gateway/internal/observability"
	"ohmf/services/gateway/internal/otp"
	"ohmf/services/gateway/internal/phone"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/securityaudit"
	"ohmf/services/gateway/internal/sqlutil"
	"ohmf/services/gateway/internal/token"
	"ohmf/services/gateway/internal/users"
)

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

// Service holds the auth business logic used by package tests and HTTP handlers.
// The HTTP handler below owns request decoding/encoding and delegates to these helpers.
type Service struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	tokens     *token.Service
	otp        otp.Provider
	accessTTL  time.Duration
	refreshTTL time.Duration
	cfg        config.Config
	userSvc    *users.Service
	deviceSvc  *devices.Service
}

func NewService(db *pgxpool.Pool, redis *redis.Client, tokens *token.Service, otpProvider otp.Provider, accessTTL, refreshTTL time.Duration, cfg config.Config) *Service {
	return &Service{
		db:         db,
		redis:      redis,
		tokens:     tokens,
		otp:        otpProvider,
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		cfg:        cfg,
	}
}

type Handler struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	tokens     *token.Service
	otp        otp.Provider
	accessTTL  time.Duration
	refreshTTL time.Duration
	cfg        config.Config
	userSvc    *users.Service
	deviceSvc  *devices.Service
}

func NewHandler(db *pgxpool.Pool, redis *redis.Client, tokens *token.Service, otpProvider otp.Provider, accessTTL, refreshTTL time.Duration, cfg config.Config, userSvc *users.Service, deviceSvc *devices.Service) *Handler {
	return &Handler{db: db, redis: redis, tokens: tokens, otp: otpProvider, accessTTL: accessTTL, refreshTTL: refreshTTL, cfg: cfg, userSvc: userSvc, deviceSvc: deviceSvc}
}

func (h *Handler) StartPhone(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	resp, err := h.StartPhoneVerification(r.Context(), req, r.RemoteAddr)
	if err != nil {
		handleError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) VerifyPhone(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	outcome := "success"
	defer func() {
		observability.RecordAuthVerify(outcome, time.Since(startedAt))
	}()
	var req VerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		outcome = "invalid_request"
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid body", nil)
		return
	}
	resp, err := h.VerifyPhoneMethod(r.Context(), req, r.RemoteAddr)
	if err != nil {
		outcome = classifyAuthOutcome(err)
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
	resp, err := h.RefreshMethod(r.Context(), req)
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
	if err := h.LogoutMethod(r.Context(), userID, req); err != nil {
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

func classifyAuthOutcome(err error) string {
	switch {
	case errors.Is(err, ErrChallengeNotFound):
		return "challenge_not_found"
	case errors.Is(err, ErrChallengeExpired):
		return "challenge_expired"
	case errors.Is(err, ErrInvalidOTP):
		return "invalid_otp"
	case errors.Is(err, ErrInvalidRefresh):
		return "invalid_refresh"
	case errors.Is(err, ErrRateLimited):
		return "rate_limited"
	case errors.Is(err, ErrOTPDeliveryFailed):
		return "otp_delivery_failed"
	default:
		return "internal_error"
	}
}

func (h *Handler) StartPhoneVerification(ctx context.Context, req StartRequest, ip string) (map[string]any, error) {
	phoneE164 := phone.NormalizeE164(req.PhoneE164)
	if phoneE164 == "" {
		return nil, fmt.Errorf("phone_required")
	}

	window := rateWindowOrDefault(h.cfg.OTPStartWindow, 10*time.Minute)
	phoneLimit := rateLimitOrDefault(h.cfg.OTPStartPerPhoneLimit, 5)
	ipLimit := rateLimitOrDefault(h.cfg.OTPStartPerIPLimit, 20)
	subnetLimit := rateLimitOrDefault(h.cfg.OTPStartPerSubnetLimit, 100)

	allowed, err := h.allowRate(ctx, "otp:start:phone:"+phoneE164, phoneLimit, window)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrRateLimited
	}

	ipKey := normalizeRemoteAddr(ip)
	if ipKey != "" {
		allowedIP, err := h.allowRate(ctx, "otp:start:ip:"+ipKey, ipLimit, window)
		if err != nil {
			return nil, err
		}
		if !allowedIP {
			return nil, ErrRateLimited
		}
	}

	if subnet := ipv4SubnetKey(ipKey); subnet != "" {
		allowedSubnet, err := h.allowRate(ctx, "otp:start:subnet:"+subnet, subnetLimit, window)
		if err != nil {
			return nil, err
		}
		if !allowedSubnet {
			return nil, ErrRateLimited
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

	escalated := false
	if n, _ := h.redis.Get(ctx, "otp:start:phone:"+phoneE164).Int64(); n > 3 {
		escalated = true
	}
	if ipKey != "" {
		if n, _ := h.redis.Get(ctx, "otp:start:ip:"+ipKey).Int64(); n > int64(ipLimit/2) {
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

func (h *Handler) VerifyPhoneMethod(ctx context.Context, req VerifyRequest, ip string) (map[string]any, error) {
	window := rateWindowOrDefault(h.cfg.OTPVerifyWindow, 10*time.Minute)
	challengeLimit := rateLimitOrDefault(h.cfg.OTPVerifyPerChallenge, 10)
	ipLimit := rateLimitOrDefault(h.cfg.OTPVerifyPerIP, 50)
	deviceLimit := rateLimitOrDefault(h.cfg.OTPVerifyPerDevice, 10)
	phoneLimit := rateLimitOrDefault(h.cfg.OTPVerifyPerPhone, 10)

	allowed, err := h.allowRate(ctx, "otp:verify:challenge:"+req.ChallengeID, challengeLimit, window)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrRateLimited
	}

	ipKey := normalizeRemoteAddr(ip)
	if ipKey != "" {
		allowedIP, err := h.allowRate(ctx, "otp:verify:ip:"+ipKey, ipLimit, window)
		if err != nil {
			return nil, err
		}
		if !allowedIP {
			return nil, ErrRateLimited
		}
	}

	deviceFingerprint := ""
	if req.Device.PublicKey != "" {
		deviceFingerprint = fmt.Sprintf("pk:%x", sha256.Sum256([]byte(req.Device.PublicKey)))
	} else if req.Device.PushToken != "" {
		deviceFingerprint = "pt:" + req.Device.PushToken
	}
	if deviceFingerprint != "" {
		allowedDevice, err := h.allowRate(ctx, "otp:verify:device:"+deviceFingerprint, deviceLimit, window)
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

	allowedPhone, err := h.allowRate(ctx, "otp:verify:phone:"+phoneE164, phoneLimit, window)
	if err != nil {
		return nil, err
	}
	if !allowedPhone {
		return nil, ErrRateLimited
	}

	if time.Now().After(expiresAt) {
		return nil, ErrChallengeExpired
	}
	if attemptsRemaining <= 0 || otp.Hash(req.OTPCode) != otpHash {
		_, _ = tx.Exec(ctx, `
UPDATE phone_verification_challenges SET attempts_remaining = GREATEST(attempts_remaining - 1, 0) WHERE id = $1
`, challengeID)
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, ErrInvalidOTP
	}

	_, err = tx.Exec(ctx, `
UPDATE phone_verification_challenges SET consumed_at = now() WHERE id = $1
`, challengeID)
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
	var deviceID string
	deviceCapabilities := normalizeDeviceCapabilities(req.Device.Platform, req.Device.Capabilities)
	err = tx.QueryRow(ctx, `
INSERT INTO devices (user_id, platform, device_name, capabilities, push_token, public_key, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
RETURNING id::text
`, userID, req.Device.Platform, req.Device.DeviceName, deviceCapabilities, sqlutil.Nullable(req.Device.PushToken), sqlutil.Nullable(req.Device.PublicKey)).Scan(&deviceID)
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

func (h *Handler) RefreshMethod(ctx context.Context, req RefreshRequest) (map[string]any, error) {
	return refreshTokens(ctx, h.db, h.tokens, h.accessTTL, h.refreshTTL, h.cfg, req)
}

func (s *Service) Refresh(ctx context.Context, req RefreshRequest) (map[string]any, error) {
	return refreshTokens(ctx, s.db, s.tokens, s.accessTTL, s.refreshTTL, s.cfg, req)
}

func (h *Handler) decideProfilesForPlatform(platform string) []string {
	return profilesForPlatform(h.cfg, platform)
}

func (s *Service) generateOTPCode() (string, error) {
	return generateOTPCodeForProvider(s.otp)
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

func (h *Handler) generateOTPCode() (string, error) {
	return generateOTPCodeForProvider(h.otp)
}

func binaryReadRand(dst *uint32) error {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return err
	}
	*dst = uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3])
	return nil
}

func (h *Handler) LogoutMethod(ctx context.Context, userID string, req LogoutRequest) error {
	return revokeRefreshTokens(ctx, h.db, userID, req)
}

func (s *Service) Logout(ctx context.Context, userID string, req LogoutRequest) error {
	return revokeRefreshTokens(ctx, s.db, userID, req)
}

func (h *Handler) allowRate(ctx context.Context, key string, limit int64, window time.Duration) (bool, error) {
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

func normalizeRemoteAddr(remote string) string {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(remote)
	if err == nil {
		remote = host
	}
	remote = strings.Trim(remote, "[]")
	if ip := net.ParseIP(remote); ip != nil {
		return ip.String()
	}
	return remote
}

func ipv4SubnetKey(ip string) string {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ""
	}
	v4 := parsed.To4()
	if v4 == nil {
		return ""
	}
	return fmt.Sprintf("%d.%d", v4[0], v4[1])
}

func rateLimitOrDefault(v, d int) int64 {
	if v > 0 {
		return int64(v)
	}
	return int64(d)
}

func rateWindowOrDefault(v, d time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return d
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

func generateOTPCodeForProvider(provider otp.Provider) (string, error) {
	if provider == nil {
		return randomOTPCode()
	}
	if strings.EqualFold(provider.Name(), "dev") {
		return "123456", nil
	}
	return randomOTPCode()
}

func (h *Handler) generateRefreshToken() (string, string, error) { return generateRefreshToken() }

func generateRefreshToken() (string, string, error) {
	refreshToken, err := randomToken()
	if err != nil {
		return "", "", err
	}
	return refreshToken, hashToken(refreshToken), nil
}

func (h *Handler) storeRefreshToken(ctx context.Context, db *pgxpool.Pool, userID, deviceID, tokenHash string, ttl time.Duration) error {
	return storeRefreshToken(ctx, db, userID, deviceID, tokenHash, ttl)
}

func storeRefreshToken(ctx context.Context, db *pgxpool.Pool, userID, deviceID, tokenHash string, ttl time.Duration) error {
	_, err := db.Exec(ctx, `
INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
VALUES ($1, NULLIF($2, '')::uuid, $3, now() + ($4 || ' seconds')::interval)
`, userID, deviceID, tokenHash, strconv.Itoa(int(ttl.Seconds())))
	return err
}

func refreshTokens(ctx context.Context, db *pgxpool.Pool, tokens *token.Service, accessTTL, refreshTTL time.Duration, cfg config.Config, req RefreshRequest) (map[string]any, error) {
	hsh := hashToken(req.RefreshToken)
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
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
`, hsh).Scan(&tokenID, &userID, &deviceID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidRefresh
		}
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1`, tokenID); err != nil {
		return nil, err
	}
	newRefresh, err := randomToken()
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
VALUES ($1, NULLIF($2, '')::uuid, $3, now() + ($4 || ' seconds')::interval)
`, userID, deviceID, hashToken(newRefresh), strconv.Itoa(int(refreshTTL.Seconds()))); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	var profiles []string
	if deviceID != "" {
		var platform string
		if err := db.QueryRow(ctx, `SELECT platform FROM devices WHERE id = $1`, deviceID).Scan(&platform); err == nil {
			profiles = profilesForPlatform(cfg, platform)
		}
	}
	access, err := tokens.IssueAccess(userID, deviceID, accessTTL, profiles)
	if err != nil {
		return nil, err
	}
	return map[string]any{"access_token": access, "refresh_token": newRefresh}, nil
}

func revokeRefreshTokens(ctx context.Context, db *pgxpool.Pool, userID string, req LogoutRequest) error {
	if req.DeviceID != "" {
		_, err := db.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = now()
WHERE user_id = $1 AND device_id = $2::uuid AND revoked_at IS NULL
`, userID, req.DeviceID)
		return err
	}
	_, err := db.Exec(ctx, `
UPDATE refresh_tokens
SET revoked_at = now()
WHERE user_id = $1 AND revoked_at IS NULL
`, userID)
	return err
}

func profilesForPlatform(cfg config.Config, platform string) []string {
	profiles := []string{"CORE_OTT"}
	switch strings.ToUpper(platform) {
	case "ANDROID":
		profiles = append(profiles, "MINIAPP_RUNTIME")
		if cfg.ClaimAndroidCarrier {
			profiles = append(profiles, "ANDROID_CARRIER")
		}
	case "WEB":
		profiles = append(profiles, "WEB_RELAY")
	}
	return profiles
}

// GetRecoveryCodes retrieves recovery codes for the authenticated user
func (h *Handler) GetRecoveryCodes(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	codes, err := h.userSvc.GenerateRecoveryCodes(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "failed_to_generate", err.Error(), nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"recovery_codes": codes,
		"expires_at":     time.Now().AddDate(0, 0, 90),
	})
}

// Setup2FA initiates 2FA setup for the authenticated user
func (h *Handler) Setup2FA(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	var req struct {
		Method string `json:"method"` // "sms" or "totp"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}

	if req.Method != "sms" && req.Method != "totp" {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_method", "method must be 'sms' or 'totp'", nil)
		return
	}

	secret, err := h.userSvc.Enable2FA(r.Context(), userID, req.Method)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "setup_failed", err.Error(), nil)
		return
	}

	response := map[string]any{
		"method": req.Method,
	}

	// For TOTP, return provisioning URL
	if req.Method == "totp" {
		// In a real implementation, generate a QR code or provisioning URL
		// For now, return the secret
		response["secret"] = secret
		response["provisioning_uri"] = fmt.Sprintf("otpauth://totp/OHMF:%s?secret=%s&issuer=OHMF", userID, secret)
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

// Verify2FA verifies and enables 2FA for the authenticated user
func (h *Handler) Verify2FA(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	var req struct {
		Method string `json:"method"`
		Code   string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}

	verified, err := h.userSvc.Verify2FA(r.Context(), userID, req.Method, req.Code)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "verify_failed", err.Error(), nil)
		return
	}

	if !verified {
		httpx.WriteError(w, r, http.StatusUnauthorized, "verify_failed", "code verification failed", nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"verified": true})
}

// List2FAMethods retrieves active 2FA methods for the authenticated user
func (h *Handler) List2FAMethods(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}

	methods, err := h.userSvc.List2FAMethods(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "list_failed", err.Error(), nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"methods": methods})
}

// StartPairing creates a short-lived pairing code that can be redeemed by a new device.
func (h *Handler) StartPairing(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing auth", nil)
		return
	}
	deviceID, _ := middleware.DeviceIDFromContext(r.Context())

	code, err := generatePairingCode()
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}

	var sessionID string
	var expiresAt time.Time
	if err := h.db.QueryRow(r.Context(), `
		INSERT INTO device_pairing_sessions (user_id, requested_by_device_id, pairing_code, expires_at)
		VALUES ($1::uuid, NULLIF($2, '')::uuid, $3, now() + interval '10 minute')
		RETURNING id::text, expires_at
	`, userID, deviceID, code).Scan(&sessionID, &expiresAt); err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	_ = securityaudit.Append(r.Context(), h.db, userID, userID, "device_pairing_started", map[string]any{
		"pairing_session_id":     sessionID,
		"requested_by_device_id": deviceID,
	})

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"pairing_session_id": sessionID,
		"pairing_code":       code,
		"expires_at":         expiresAt.UTC().Format(time.RFC3339Nano),
	})
}

// CompletePairing redeems a pairing code, registers a new device, and issues tokens.
func (h *Handler) CompletePairing(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	outcome := "success"
	defer func() {
		observability.RecordPairingComplete(outcome, time.Since(startedAt))
	}()
	var req struct {
		PairingCode string `json:"pairing_code"`
		Device      struct {
			Platform     string   `json:"platform"`
			DeviceName   string   `json:"device_name"`
			PublicKey    string   `json:"public_key"`
			Capabilities []string `json:"capabilities"`
			PushToken    string   `json:"push_token"`
		} `json:"device"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		outcome = "invalid_json"
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}
	if strings.TrimSpace(req.PairingCode) == "" || strings.TrimSpace(req.Device.Platform) == "" {
		outcome = "invalid_request"
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_request", "pairing_code and device.platform required", nil)
		return
	}

	tx, err := h.db.BeginTx(r.Context(), pgx.TxOptions{})
	if err != nil {
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	defer tx.Rollback(r.Context())

	var sessionID, userID, requestedByDeviceID string
	var expiresAt time.Time
	if err := tx.QueryRow(r.Context(), `
		SELECT id::text, user_id::text, COALESCE(requested_by_device_id::text, ''), expires_at
		FROM device_pairing_sessions
		WHERE pairing_code = $1 AND status = 'PENDING'
		FOR UPDATE
	`, strings.TrimSpace(req.PairingCode)).Scan(&sessionID, &userID, &requestedByDeviceID, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			outcome = "invalid_pairing_code"
			httpx.WriteError(w, r, http.StatusUnauthorized, "invalid_pairing_code", "pairing code invalid", nil)
			return
		}
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	if !expiresAt.After(time.Now().UTC()) {
		outcome = "pairing_expired"
		httpx.WriteError(w, r, http.StatusGone, "pairing_expired", "pairing code expired", nil)
		return
	}

	capabilities := normalizeDeviceCapabilities(req.Device.Platform, req.Device.Capabilities)
	var newDeviceID string
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO devices (user_id, platform, device_name, capabilities, push_token, public_key, last_seen_at)
		VALUES ($1::uuid, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), now())
		RETURNING id::text
	`, userID, req.Device.Platform, req.Device.DeviceName, capabilities, req.Device.PushToken, req.Device.PublicKey).Scan(&newDeviceID); err != nil {
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}

	refreshToken, tokenHash, err := h.generateRefreshToken()
	if err != nil {
		outcome = "token_generation_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_generation_failed", err.Error(), nil)
		return
	}
	if _, err := tx.Exec(r.Context(), `
		INSERT INTO refresh_tokens (user_id, device_id, token_hash, expires_at)
		VALUES ($1::uuid, $2::uuid, $3, now() + ($4 || ' seconds')::interval)
	`, userID, newDeviceID, tokenHash, strconv.Itoa(int(h.cfg.RefreshTTL.Seconds()))); err != nil {
		outcome = "token_storage_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_storage_failed", err.Error(), nil)
		return
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE device_pairing_sessions
		SET status = 'COMPLETED',
		    paired_device_id = $2::uuid,
		    completed_at = now()
		WHERE id = $1::uuid
	`, sessionID, newDeviceID); err != nil {
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	if err := securityaudit.Append(r.Context(), tx, userID, userID, "device_pairing_completed", map[string]any{
		"pairing_session_id": sessionID,
		"paired_device_id":   newDeviceID,
	}); err != nil {
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		outcome = "pairing_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "pairing_failed", err.Error(), nil)
		return
	}
	if h.redis != nil {
		_, _ = replication.NewStore(h.db, h.redis).EmitUserEvent(r.Context(), userID, "", replication.UserEventAccountDeviceLinked, map[string]any{
			"pairing_session_id":     sessionID,
			"paired_device_id":       newDeviceID,
			"requested_by_device_id": requestedByDeviceID,
		})
	}

	accessToken, err := h.tokens.IssueAccess(userID, newDeviceID, h.cfg.AccessTTL, h.decideProfilesForPlatform(req.Device.Platform))
	if err != nil {
		outcome = "token_generation_failed"
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_generation_failed", err.Error(), nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"user_id":       userID,
		"device_id":     newDeviceID,
		"access_token":  accessToken,
		"refresh_token": refreshToken,
		"expires_in":    int(h.cfg.AccessTTL.Seconds()),
	})
}

// UseRecoveryCode validates and uses a recovery code to authenticate
func (h *Handler) UseRecoveryCode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PhoneE164 string `json:"phone_e164"`
		Code      string `json:"code"`
		Device    struct {
			Platform     string   `json:"platform"`
			DeviceName   string   `json:"device_name"`
			PublicKey    string   `json:"public_key"`
			Capabilities []string `json:"capabilities"`
		} `json:"device"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_json", "invalid request body", nil)
		return
	}

	if req.PhoneE164 == "" || req.Code == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "missing_fields", "phone_e164 and code required", nil)
		return
	}

	// Find user by phone
	var userID string
	err := h.db.QueryRow(r.Context(), `
		SELECT id::text FROM users WHERE primary_phone_e164 = $1
	`, req.PhoneE164).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, http.StatusUnauthorized, "user_not_found", "user not found", nil)
			return
		}
		httpx.WriteError(w, r, http.StatusInternalServerError, "lookup_failed", err.Error(), nil)
		return
	}

	// Validate recovery code
	valid, err := h.userSvc.ValidateRecoveryCode(r.Context(), userID, req.Code)
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "validation_failed", err.Error(), nil)
		return
	}

	if !valid {
		httpx.WriteError(w, r, http.StatusUnauthorized, "invalid_code", "recovery code is invalid, used, or expired", nil)
		return
	}

	// Register device and generate tokens
	deviceID, err := h.deviceSvc.RegisterDevice(r.Context(), userID, devices.Device{
		Platform:     req.Device.Platform,
		DeviceName:   req.Device.DeviceName,
		PublicKey:    req.Device.PublicKey,
		Capabilities: req.Device.Capabilities,
	})
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "device_registration_failed", err.Error(), nil)
		return
	}

	refreshToken, tokenHash, err := h.generateRefreshToken()
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_generation_failed", err.Error(), nil)
		return
	}

	// Store refresh token alongside the recovery flow.
	if err := h.storeRefreshToken(r.Context(), h.db, userID, deviceID, tokenHash, h.cfg.RefreshTTL); err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_storage_failed", err.Error(), nil)
		return
	}

	accessToken, err := h.tokens.IssueAccess(userID, deviceID, h.cfg.AccessTTL, h.decideProfilesForPlatform(req.Device.Platform))
	if err != nil {
		httpx.WriteError(w, r, http.StatusInternalServerError, "token_generation_failed", err.Error(), nil)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
		"user_id":       userID,
		"device_id":     deviceID,
		"expires_in":    int(h.cfg.AccessTTL.Seconds()),
	})
}

func generatePairingCode() (string, error) {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	raw := make([]byte, 8)
	max := big.NewInt(int64(len(alphabet)))
	for i := range raw {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		raw[i] = alphabet[n.Int64()]
	}
	return string(raw[:4]) + "-" + string(raw[4:]), nil
}

// removed: duplicate nullable() helper - moved to sqlutil package
