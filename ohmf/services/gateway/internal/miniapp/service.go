// Package miniapp implements the gateway's mini-app session runtime plane.
// OWNERSHIP: Gateway is EXCLUSIVE owner of sessions, events, snapshots, joins, shares.
// Control plane (registry, releases, installs) is owned by the apps service.
// Gateway queries apps service for catalog/version/install data (read-only).
// See: docs/miniapp/ownership-boundaries.md
package miniapp

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/config"
	"ohmf/services/gateway/internal/replication"
	"ohmf/services/gateway/internal/securityaudit"
)

var (
	ErrManifestRequired          = errors.New("manifest_required")
	ErrManifestInvalid           = errors.New("manifest_invalid")
	ErrManifestSignatureRequired = errors.New("manifest_signature_required")
	ErrManifestSignatureInvalid  = errors.New("manifest_signature_invalid")
	ErrManifestNotFound          = errors.New("manifest_not_found")
	ErrAppInstallNotFound        = errors.New("miniapp_install_not_found")
	ErrSessionNotFound           = errors.New("session_not_found")
	ErrSessionEnded              = errors.New("session_ended")
	ErrStateVersionConflict      = errors.New("state_version_conflict")
	ErrBridgeMethodRequired      = errors.New("bridge_method_required")
	ErrBridgeMethodNotAllowed    = errors.New("bridge_method_not_allowed")
	ErrBridgeMethodRateLimited   = errors.New("bridge_method_rate_limited")
	ErrReleaseSuspended          = errors.New("release_suspended")
	ErrReleaseRevoked            = errors.New("release_revoked")
	ErrPreviewURLInvalid         = errors.New("preview_url_invalid")
	ErrIconURLInvalid            = errors.New("icon_url_invalid")
)

var semverPattern = regexp.MustCompile(`^\d+\.\d+\.\d+(-[A-Za-z0-9.-]+)?$`)

// P4.1 Event Model: Event type constants (append-only audit trail)
const (
	EventTypeSessionCreated   = "session_created"   // Session initialized with participants and permissions
	EventTypeSessionJoined    = "session_joined"    // New participant joined existing session
	EventTypeStorageUpdated   = "storage_updated"   // AppendEvent call recorded (bridge method invoked)
	EventTypeSnapshotWritten  = "snapshot_written"  // State snapshot persisted (SnapshotSession called)
	EventTypeMessageProjected = "message_projected" // Message projected into session (future: used when messages are synchronized)
)

// Allowed MIME types for preview and icon assets
var allowedImageMimeTypes = map[string]bool{
	"image/png":     true,
	"image/jpeg":    true,
	"image/webp":    true,
	"image/svg+xml": true,
	"image/gif":     true,
}

type Service struct {
	db          *pgxpool.Pool
	publicKey   any
	redis       *redis.Client
	replication *replication.Store
}

type SessionParticipant struct {
	UserID      string `json:"user_id"`
	Role        string `json:"role"`
	DisplayName string `json:"display_name,omitempty"`
}

type CreateSessionInput struct {
	ManifestID         string
	AppID              string
	ConversationID     string
	Viewer             SessionParticipant
	Participants       []SessionParticipant
	GrantedPermissions []string
	StateSnapshot      any
	TTL                time.Duration
	ResumeExisting     bool
	Reconsented        bool
}

type sessionState struct {
	Snapshot                  map[string]any   `json:"snapshot"`
	SessionStorage            map[string]any   `json:"session_storage"`
	SharedConversationStorage map[string]any   `json:"shared_conversation_storage"`
	ProjectedMessages         []map[string]any `json:"projected_messages"`
}

type sessionRecord struct {
	ID                     string
	ManifestID             string
	AppID                  string
	AppVersion             string
	ConversationID         string
	Participants           []SessionParticipant
	GlobalPermissions      []string
	ParticipantPermissions map[string][]string
	State                  sessionState
	StateVersion           int
	CreatedBy              string
	ExpiresAt              *time.Time
	CreatedAt              *time.Time
	EndedAt                *time.Time
}

type manifestSignature struct {
	Alg string
	KID string
	Sig string
}

type catalogInstall struct {
	Installed        bool       `json:"installed"`
	InstalledVersion string     `json:"installed_version,omitempty"`
	AutoUpdate       bool       `json:"auto_update"`
	Enabled          bool       `json:"enabled"`
	InstalledAt      *time.Time `json:"installed_at,omitempty"`
	UpdatedAt        *time.Time `json:"updated_at,omitempty"`
}

type catalogEntry struct {
	ID               string
	AppID            string
	Version          string
	OwnerUserID      string
	PublisherUserID  string
	Visibility       string
	SourceType       string
	ManifestHash     string
	EntrypointOrigin string
	PreviewOrigin    string
	Manifest         map[string]any
	CreatedAt        time.Time
	PublishedAt      time.Time
	Install          catalogInstall
}

func NewService(db *pgxpool.Pool, cfg config.Config, redisClient *redis.Client, store *replication.Store) *Service {
	s := &Service{db: db, redis: redisClient, replication: store}
	if cfg.MiniappPublicKeyPEM != "" {
		block, _ := pem.Decode([]byte(cfg.MiniappPublicKeyPEM))
		if block != nil {
			if pk, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
				switch typed := pk.(type) {
				case *rsa.PublicKey:
					s.publicKey = typed
				case ed25519.PublicKey:
					s.publicKey = typed
				}
			}
		}
	}
	return s
}

// RegisterManifest stores a mini-app manifest JSON and returns its id.
func (s *Service) RegisterManifest(ctx context.Context, ownerID string, manifest any) (string, error) {
	if manifest == nil {
		return "", ErrManifestRequired
	}
	var mmap map[string]any
	b, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("%w: marshal failed", ErrManifestInvalid)
	}
	if err := json.Unmarshal(b, &mmap); err != nil {
		return "", fmt.Errorf("%w: object required", ErrManifestInvalid)
	}
	if err := validateManifest(mmap); err != nil {
		return "", err
	}
	if s.publicKey != nil {
		if err := s.verifyManifestSignature(mmap); err != nil {
			return "", fmt.Errorf("%w: %v", ErrManifestSignatureInvalid, err)
		}
	}
	appID := stringField(mmap, "app_id")
	version := stringField(mmap, "version")
	manifestHash := manifestHashHex(b)
	if existingID, err := s.findPublishedManifestID(ctx, appID, version); err == nil {
		existingHash, hashErr := s.manifestHashByID(ctx, existingID)
		if hashErr != nil {
			return "", hashErr
		}
		if existingHash != manifestHash {
			return "", fmt.Errorf("%w: app_id/version already published with different manifest", ErrManifestInvalid)
		}
		return existingID, nil
	} else if !errors.Is(err, ErrManifestNotFound) {
		return "", err
	}
	id := uuid.New().String()
	_, err = s.db.Exec(ctx, `INSERT INTO miniapp_manifests (id, owner_user_id, manifest, created_at) VALUES ($1::uuid, $2::uuid, $3::jsonb, now())`, id, ownerID, string(b))
	if err != nil {
		return "", err
	}
	if err := s.publishRelease(ctx, ownerID, id, mmap, manifestHash); err != nil {
		return "", err
	}
	return id, nil
}

func validateManifest(mmap map[string]any) error {
	if len(mmap) == 0 {
		return ErrManifestRequired
	}
	if !nonEmptyStringField(mmap, "app_id") {
		return fmt.Errorf("%w: app_id required", ErrManifestInvalid)
	}
	if !nonEmptyStringField(mmap, "name") {
		return fmt.Errorf("%w: name required", ErrManifestInvalid)
	}
	if manifestVersion, ok := mmap["manifest_version"]; ok {
		if version, ok := manifestVersion.(string); !ok || version == "" {
			return fmt.Errorf("%w: manifest_version must be a string", ErrManifestInvalid)
		}
	}
	version, ok := mmap["version"].(string)
	if !ok || !semverPattern.MatchString(version) {
		return fmt.Errorf("%w: version must be semver", ErrManifestInvalid)
	}
	entrypoint, ok := mmap["entrypoint"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: entrypoint object required", ErrManifestInvalid)
	}
	entryType, ok := entrypoint["type"].(string)
	if !ok || (entryType != "url" && entryType != "inline" && entryType != "web_bundle") {
		return fmt.Errorf("%w: entrypoint.type invalid", ErrManifestInvalid)
	}
	if !nonEmptyStringField(entrypoint, "url") {
		return fmt.Errorf("%w: entrypoint.url required", ErrManifestInvalid)
	}
	messagePreview, ok := mmap["message_preview"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: message_preview object required", ErrManifestInvalid)
	}
	previewType := stringField(messagePreview, "type")
	if previewType != "static_image" && previewType != "live" {
		return fmt.Errorf("%w: message_preview.type invalid", ErrManifestInvalid)
	}
	if !nonEmptyStringField(messagePreview, "url") {
		return fmt.Errorf("%w: message_preview.url required", ErrManifestInvalid)
	}
	if fitMode := stringField(messagePreview, "fit_mode"); fitMode != "" && fitMode != "scale" && fitMode != "crop" {
		return fmt.Errorf("%w: message_preview.fit_mode invalid", ErrManifestInvalid)
	}

	// P2.4 Preview & Icon Security: Validate preview URL matches origin and is image type
	previewURL := stringField(messagePreview, "url")
	entrypointURL := stringField(entrypoint, "url")
	if err := validatePreviewURL(previewURL, entrypointURL, previewType); err != nil {
		return err
	}

	// P2.4 Preview & Icon Security: Validate icon URLs if present
	if iconArray, ok := mmap["icons"].([]any); ok {
		if err := validateIconURLs(iconArray, entrypointURL); err != nil {
			return err
		}
	}

	if !stringSliceFieldPresent(mmap, "permissions") {
		return fmt.Errorf("%w: permissions array required", ErrManifestInvalid)
	}
	if _, ok := mmap["capabilities"].(map[string]any); !ok {
		return fmt.Errorf("%w: capabilities object required", ErrManifestInvalid)
	}
	if rawSignature, ok := mmap["signature"]; ok && rawSignature != nil {
		if _, err := manifestSignatureFromMap(mmap); err != nil {
			return err
		}
	}
	return nil
}

func manifestSignatureFromMap(mmap map[string]any) (manifestSignature, error) {
	rawSignature, ok := mmap["signature"]
	if !ok || rawSignature == nil {
		return manifestSignature{}, ErrManifestSignatureRequired
	}
	sigMap, ok := rawSignature.(map[string]any)
	if !ok {
		return manifestSignature{}, fmt.Errorf("%w: signature object required", ErrManifestInvalid)
	}
	sig := manifestSignature{
		Alg: stringField(sigMap, "alg"),
		KID: stringField(sigMap, "kid"),
		Sig: stringField(sigMap, "sig"),
	}
	if sig.Alg == "" || sig.KID == "" || sig.Sig == "" {
		return manifestSignature{}, fmt.Errorf("%w: signature.alg, signature.kid, and signature.sig are required", ErrManifestInvalid)
	}
	return sig, nil
}

func nonEmptyStringField(m map[string]any, key string) bool {
	return stringField(m, key) != ""
}

func stringField(m map[string]any, key string) string {
	value, ok := m[key].(string)
	if !ok {
		return ""
	}
	return value
}

func stringSliceFieldPresent(m map[string]any, key string) bool {
	switch m[key].(type) {
	case []any, []string:
		return true
	default:
		return false
	}
}

// inferMimeType infers MIME type from URL file extension
func inferMimeType(rawURL string) string {
	lower := strings.ToLower(rawURL)
	if strings.HasSuffix(lower, ".png") {
		return "image/png"
	}
	if strings.HasSuffix(lower, ".jpg") || strings.HasSuffix(lower, ".jpeg") {
		return "image/jpeg"
	}
	if strings.HasSuffix(lower, ".webp") {
		return "image/webp"
	}
	if strings.HasSuffix(lower, ".gif") {
		return "image/gif"
	}
	if strings.HasSuffix(lower, ".svg") {
		return "image/svg+xml"
	}
	return ""
}

// isImageMimeType checks if a MIME type is in the allowed image whitelist
func isImageMimeType(mimeType string) bool {
	return allowedImageMimeTypes[strings.ToLower(mimeType)]
}

// isLocalhost checks if a hostname is localhost (for dev exception)
func isLocalhost(host string) bool {
	return host == "localhost" || strings.HasPrefix(host, "localhost:")
}

// validatePreviewURL validates that a preview URL is valid and matches the entrypoint origin
// For static_image previews, validates MIME type; for live previews, skips MIME check.
func validatePreviewURL(previewURL, entrypointURL, previewType string) error {
	// 1. Parse preview URL
	parsedPreview, err := url.Parse(previewURL)
	if err != nil {
		return fmt.Errorf("%w: invalid preview URL: %v", ErrPreviewURLInvalid, err)
	}

	// 2. Parse entrypoint URL
	parsedEntrypoint, err := url.Parse(entrypointURL)
	if err != nil {
		return fmt.Errorf("%w: invalid entrypoint URL: %v", ErrPreviewURLInvalid, err)
	}

	// 3. For static images, validate MIME type from extension
	if previewType == "static_image" {
		mimeType := inferMimeType(previewURL)
		if !isImageMimeType(mimeType) {
			return fmt.Errorf("%w: static_image preview URL must have image extension (.png, .jpg, .webp, .gif, .svg)", ErrPreviewURLInvalid)
		}
	}
	// Live previews are HTML/dynamic; skip MIME check (will be validated at fetch time)

	// 4. Check origin match (except for localhost dev)
	entrypointHost := parsedEntrypoint.Hostname()
	if !isLocalhost(entrypointHost) {
		previewHost := parsedPreview.Hostname()
		if previewHost != entrypointHost {
			return fmt.Errorf("%w: preview URL domain must match entrypoint domain (preview: %s, entrypoint: %s)", ErrPreviewURLInvalid, previewHost, entrypointHost)
		}
	}

	return nil
}

// validateIconURLs validates that all icon URLs are valid and match the entrypoint origin
func validateIconURLs(icons []any, entrypointURL string) error {
	if len(icons) == 0 {
		return nil // icons array is optional
	}

	// Parse entrypoint URL once
	parsedEntrypoint, err := url.Parse(entrypointURL)
	if err != nil {
		return fmt.Errorf("%w: invalid entrypoint URL: %v", ErrIconURLInvalid, err)
	}

	// Validate each icon in the array
	for i, icon := range icons {
		iconMap, ok := icon.(map[string]any)
		if !ok {
			continue // skip non-object entries
		}

		// Extract URL (required for icon)
		iconURL := stringField(iconMap, "url")
		if iconURL == "" {
			continue // skip icons without URL (though typically required)
		}

		// Parse icon URL
		parsedIcon, err := url.Parse(iconURL)
		if err != nil {
			return fmt.Errorf("%w: icon[%d].url is invalid: %v", ErrIconURLInvalid, i, err)
		}

		// Validate MIME type (from explicit type field or inferred from extension)
		mimeType := stringField(iconMap, "type")
		if mimeType == "" {
			mimeType = inferMimeType(iconURL)
		}
		if !isImageMimeType(mimeType) {
			return fmt.Errorf("%w: icon[%d] must have image MIME type, got %s", ErrIconURLInvalid, i, mimeType)
		}

		// Check origin match (except for localhost dev)
		entrypointHost := parsedEntrypoint.Hostname()
		if !isLocalhost(entrypointHost) {
			iconHost := parsedIcon.Hostname()
			if iconHost != entrypointHost {
				return fmt.Errorf("%w: icon[%d] domain must match entrypoint domain (icon: %s, entrypoint: %s)", ErrIconURLInvalid, i, iconHost, entrypointHost)
			}
		}
	}

	return nil
}

// verifyManifestSignature verifies a manifest map contains a signature over the
// manifest JSON with the `signature` field removed.
func (s *Service) verifyManifestSignature(mmap map[string]any) error {
	signature, err := manifestSignatureFromMap(mmap)
	if err != nil {
		return err
	}

	copyMap := make(map[string]any, len(mmap))
	for k, v := range mmap {
		if k == "signature" {
			continue
		}
		copyMap[k] = v
	}
	payload, err := json.Marshal(copyMap)
	if err != nil {
		return err
	}
	sigBytes, err := base64.StdEncoding.DecodeString(signature.Sig)
	if err != nil {
		return err
	}

	switch signature.Alg {
	case "RS256":
		rsaKey, ok := s.publicKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("configured public key does not support RS256")
		}
		h := sha256.Sum256(payload)
		return rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, h[:], sigBytes)
	case "Ed25519", "EdDSA":
		edKey, ok := s.publicKey.(ed25519.PublicKey)
		if !ok {
			return fmt.Errorf("configured public key does not support Ed25519")
		}
		if !ed25519.Verify(edKey, payload, sigBytes) {
			return fmt.Errorf("signature verification failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported signature algorithm %q", signature.Alg)
	}
}

func manifestHashHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:])
}

func manifestOriginFromURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

func manifestSourceType(manifest map[string]any) string {
	metadata, _ := manifest["metadata"].(map[string]any)
	if value, ok := metadata["registry_hosted"].(bool); ok && value {
		return "registry"
	}
	entrypoint, _ := manifest["entrypoint"].(map[string]any)
	origin := manifestOriginFromURL(stringField(entrypoint, "url"))
	if strings.HasPrefix(origin, "http://localhost:") || strings.HasPrefix(origin, "http://127.0.0.1:") || strings.HasPrefix(origin, "https://localhost:") || strings.HasPrefix(origin, "https://127.0.0.1:") {
		return "dev"
	}
	return "external"
}

func manifestVisibility(manifest map[string]any) string {
	metadata, _ := manifest["metadata"].(map[string]any)
	value := strings.ToLower(strings.TrimSpace(stringField(metadata, "visibility")))
	switch value {
	case "", "public":
		return "public"
	case "private":
		return "private"
	default:
		return "public"
	}
}

// findPublishedManifestID looks up a manifest by app_id and version from the gateway registry.
// DEPRECATED: Per P0.1 Ownership Boundaries, the apps service is the sole owner of release metadata.
// In production, this should not be called directly; use RegistryClient instead.
// This method persists for backward compatibility in dev mode only.
// See: docs/miniapp/ownership-boundaries.md
func (s *Service) findPublishedManifestID(ctx context.Context, appID, version string) (string, error) {
	var manifestID string
	err := s.db.QueryRow(
		ctx,
		`SELECT manifest_id::text
		   FROM miniapp_releases
		  WHERE app_id = $1 AND version = $2
		  LIMIT 1`,
		appID,
		version,
	).Scan(&manifestID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrManifestNotFound
		}
		return "", err
	}
	return manifestID, nil
}

func (s *Service) manifestHashByID(ctx context.Context, manifestID string) (string, error) {
	var manifestB []byte
	err := s.db.QueryRow(ctx, `SELECT manifest FROM miniapp_manifests WHERE id = $1::uuid`, manifestID).Scan(&manifestB)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrManifestNotFound
		}
		return "", err
	}
	var manifest any
	if err := json.Unmarshal(manifestB, &manifest); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return manifestHashHex(canonical), nil
}

// publishRelease records a mini-app release in the gateway registry.
// DEPRECATED: Per P0.1 Ownership Boundaries (docs/miniapp/ownership-boundaries.md),
// the apps service is the sole owner of releases and should be queried via RegistryClient.
// This method persists only for backward compatibility in dev mode when RegistryClient is not configured.
// In production, applications MUST configure RegistryClient; do not rely on this method.
// See also: Migration 000043_remove_miniapp_legacy_tables
func (s *Service) publishRelease(ctx context.Context, ownerID, manifestID string, manifest map[string]any, manifestHash string) error {
	entrypoint, _ := manifest["entrypoint"].(map[string]any)
	preview, _ := manifest["message_preview"].(map[string]any)
	_, err := s.db.Exec(
		ctx,
		`INSERT INTO miniapp_releases (
			app_id,
			version,
			manifest_id,
			publisher_user_id,
			visibility,
			source_type,
			entrypoint_origin,
			preview_origin,
			manifest_hash,
			created_at,
			published_at
		) VALUES ($1, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, now(), now())`,
		stringField(manifest, "app_id"),
		stringField(manifest, "version"),
		manifestID,
		ownerID,
		manifestVisibility(manifest),
		manifestSourceType(manifest),
		manifestOriginFromURL(stringField(entrypoint, "url")),
		manifestOriginFromURL(stringField(preview, "url")),
		manifestHash,
	)
	return err
}

func (s *Service) GetManifestByAppID(ctx context.Context, userID, appID string) (map[string]any, error) {
	if appID == "" {
		return nil, ErrManifestNotFound
	}
	entry, err := s.loadCatalogEntryByAppID(ctx, userID, appID)
	if err != nil {
		return nil, err
	}
	return catalogEntryToMap(entry), nil
}

func (s *Service) GetManifestByID(ctx context.Context, manifestID string) (map[string]any, error) {
	var id, owner string
	var manifestB []byte
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `SELECT id::text, owner_user_id::text, manifest, created_at FROM miniapp_manifests WHERE id = $1::uuid`, manifestID).Scan(&id, &owner, &manifestB, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}
	var manifest any
	_ = json.Unmarshal(manifestB, &manifest)
	return map[string]any{
		"id":            id,
		"owner_user_id": owner,
		"manifest":      manifest,
		"created_at":    createdAt,
	}, nil
}

// CreateSession creates or resumes a runtime session for an app bound to a conversation.
func (s *Service) CreateSession(ctx context.Context, input CreateSessionInput) (map[string]any, bool, error) {
	if input.ConversationID == "" {
		return nil, false, fmt.Errorf("conversation_id required")
	}
	if input.TTL <= 0 {
		input.TTL = 30 * time.Minute
	}

	manifestID, appID, manifestPermissions, err := s.resolveManifest(ctx, input.ManifestID, input.AppID)
	if err != nil {
		return nil, false, err
	}

	// Check if the release is suspended or revoked (kill switch enforcement)
	// This query will be optimized by the gateway caching layer if available
	catalogEntry, err := s.loadCatalogEntryByAppID(ctx, input.Viewer.UserID, appID)
	if err != nil {
		return nil, false, err
	}

	if catalogEntry.PublishedAt.IsZero() {
		// Release not published yet
		return nil, false, fmt.Errorf("release_not_available")
	}

	// Check release status from RegistryClient for authoritative status
	// This ensures we catch suspensions even if local cache is stale
	status, err := s.CheckReleaseStatus(ctx, appID, catalogEntry.Version)
	if err != nil && !errors.Is(err, ErrManifestNotFound) {
		// Log error but continue (graceful degradation if status check fails)
	} else if status != nil {
		if status.SuspendedAt != nil {
			return nil, false, fmt.Errorf("release_suspended: %s", status.SuspensionReason)
		}
		if status.RevokedAt != nil {
			return nil, false, fmt.Errorf("release_revoked")
		}
	}

	viewer := normalizeParticipant(input.Viewer)
	participants := normalizeParticipants(input.Participants, viewer)
	grantedPermissions := sanitizeGrantedPermissions(input.GrantedPermissions, manifestPermissions)
	if len(grantedPermissions) == 0 {
		grantedPermissions = append([]string(nil), manifestPermissions...)
	}

	if input.ResumeExisting {
		existing, err := s.findActiveSession(ctx, appID, input.ConversationID)
		if err != nil && !errors.Is(err, ErrSessionNotFound) {
			return nil, false, err
		}
		if err == nil {
			if err := s.refreshSession(ctx, existing.ID, participants, grantedPermissions, input.TTL); err != nil {
				return nil, false, err
			}
			record, err := s.GetSession(ctx, existing.ID)
			return record, false, err
		}
	}

	id := uuid.New().String()
	partsB, _ := json.Marshal(participants)
	permsB, _ := json.Marshal(grantedPermissions)
	participantPermissions := map[string][]string{}
	if viewer.UserID != "" {
		participantPermissions[viewer.UserID] = append([]string(nil), grantedPermissions...)
	}
	participantPermsB, _ := json.Marshal(participantPermissions)
	stateB, _ := json.Marshal(defaultSessionState(input.StateSnapshot))
	expires := time.Now().Add(input.TTL)
	createdBy := nullUUIDArg(viewer.UserID)
	_, err = s.db.Exec(
		ctx,
		`INSERT INTO miniapp_sessions (id, manifest_id, app_id, conversation_id, participants, granted_permissions, participant_permissions, state, state_version, created_by, expires_at, created_at)
		 VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, 1, $9::uuid, $10, now())`,
		id,
		manifestID,
		appID,
		input.ConversationID,
		string(partsB),
		string(permsB),
		string(participantPermsB),
		string(stateB),
		createdBy,
		expires,
	)
	if err != nil {
		return nil, true, err
	}

	// P4.1 Event Model: Log session_created event with initial participants and permissions
	_ = s.logSessionCreated(ctx, id, viewer.UserID, participants, grantedPermissions)

	record, err := s.GetSession(ctx, id)
	return record, true, err
}

// GetSession returns the session record and computed launch context.
func (s *Service) GetSession(ctx context.Context, sessionID string) (map[string]any, error) {
	record, err := s.loadSessionRecord(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return s.sessionRecordToMap(record), nil
}

// P4.1 Event Model: GetSessionEvents retrieves the event log for a session with optional pagination
type SessionEvent struct {
	EventSeq  int64          `json:"event_seq"`
	EventType string         `json:"event_type"`
	ActorID   *string        `json:"actor_id"`
	Body      map[string]any `json:"body"`
	CreatedAt string         `json:"created_at"` // RFC3339 timestamp
}

// GetSessionEvents retrieves events for a session, optionally filtered by type and paginated by event_seq
func (s *Service) GetSessionEvents(ctx context.Context, sessionID string, eventType *string, limit int, offset int, sinceSeq *int64) ([]SessionEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT event_seq, event_type, actor_user_id, body, created_at
		FROM miniapp_events
		WHERE app_session_id = $1::uuid
	`
	args := []any{sessionID}

	// Support cursor-based pagination with since_seq (preferred for polling)
	if sinceSeq != nil {
		query += ` AND event_seq > $` + fmt.Sprintf("%d", len(args)+1)
		args = append(args, *sinceSeq)
	} else if offset > 0 {
		// Fallback to offset-based pagination for backward compatibility
		query += ` AND event_seq > (
			SELECT COALESCE(MAX(event_seq) - $` + fmt.Sprintf("%d", len(args)+1) + `, 0)
			FROM miniapp_events
			WHERE app_session_id = $1::uuid
		)`
		args = append(args, offset)
	}

	if eventType != nil && *eventType != "" {
		query += ` AND event_type = $` + fmt.Sprintf("%d", len(args)+1)
		args = append(args, *eventType)
	}

	query += ` ORDER BY event_seq ASC LIMIT $` + fmt.Sprintf("%d", len(args)+1)
	args = append(args, limit)

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []SessionEvent
	for rows.Next() {
		var eventSeq int64
		var eventTypeStr string
		var actorID *string
		var body string
		var createdAt time.Time

		if err := rows.Scan(&eventSeq, &eventTypeStr, &actorID, &body, &createdAt); err != nil {
			return nil, err
		}

		var bodyMap map[string]any
		if err := json.Unmarshal([]byte(body), &bodyMap); err != nil {
			bodyMap = map[string]any{}
		}

		events = append(events, SessionEvent{
			EventSeq:  eventSeq,
			EventType: eventTypeStr,
			ActorID:   actorID,
			Body:      bodyMap,
			CreatedAt: createdAt.UTC().Format(time.RFC3339),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

// EndSession marks a session as ended.
func (s *Service) EndSession(ctx context.Context, sessionID string) error {
	tag, err := s.db.Exec(ctx, `UPDATE miniapp_sessions SET ended_at = now() WHERE id = $1::uuid AND ended_at IS NULL`, sessionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		record, err := s.loadSessionRecord(ctx, sessionID)
		if err != nil {
			return err
		}
		if record.EndedAt != nil {
			return ErrSessionEnded
		}
		return ErrSessionEnded
	}
	return nil
}

// P4.1 Event Model: Helper functions for logging typed events

// logSessionCreated logs that a session was created with initial participants and permissions.
func (s *Service) logSessionCreated(ctx context.Context, sessionID, actorID string, participants []SessionParticipant, permissions []string) error {
	metadata := map[string]any{
		"participant_count": len(participants),
		"permissions":       permissions,
		"participants":      participants,
	}
	_, err := s.AppendEvent(ctx, sessionID, actorID, EventTypeSessionCreated, "", metadata)
	return err
}

// logSessionJoined logs that a participant joined an existing session.
func (s *Service) logSessionJoined(ctx context.Context, sessionID, actorID string, participant SessionParticipant, permissions []string) error {
	metadata := map[string]any{
		"participant": participant,
		"permissions": permissions,
	}
	_, err := s.AppendEvent(ctx, sessionID, actorID, EventTypeSessionJoined, "", metadata)
	return err
}

// logStorageUpdated logs that the session storage was updated via an app bridge method call.
func (s *Service) logStorageUpdated(ctx context.Context, sessionID, actorID, bridgeMethod string, stateSize int) error {
	metadata := map[string]any{
		"bridge_method": bridgeMethod,
		"state_size":    stateSize,
	}
	_, err := s.AppendEvent(ctx, sessionID, actorID, EventTypeStorageUpdated, bridgeMethod, metadata)
	return err
}

// logSnapshotWritten logs that the session state was snapshotted and persisted.
func (s *Service) logSnapshotWritten(ctx context.Context, sessionID, actorID string, stateVersion int, stateSize int) error {
	metadata := map[string]any{
		"state_version": stateVersion,
		"state_size":    stateSize,
	}
	_, err := s.AppendEvent(ctx, sessionID, actorID, EventTypeSnapshotWritten, "", metadata)
	return err
}

// logMessageProjected logs that a message was projected into the session context.
// This is called when messages are synchronized to the session (future feature).
func (s *Service) logMessageProjected(ctx context.Context, sessionID, messageID string, messageMetadata map[string]any) error {
	metadata := map[string]any{
		"message_id": messageID,
	}
	if messageMetadata != nil {
		metadata["message_metadata"] = messageMetadata
	}
	_, err := s.AppendEvent(ctx, sessionID, "", EventTypeMessageProjected, "", metadata)
	return err
}

// AppendEvent appends an event to a mini-app session's event log.
// eventType must be one of the EventType* constants.
// eventName is optional (used for custom event identification, e.g., bridge method names).
func (s *Service) AppendEvent(ctx context.Context, sessionID, actorID, eventType, eventName string, body any) (int64, error) {
	if eventType == "" {
		return 0, fmt.Errorf("event_type required")
	}
	record, err := s.loadSessionRecord(ctx, sessionID)
	if err != nil {
		return 0, err
	}
	if record.EndedAt != nil {
		return 0, ErrSessionEnded
	}
	b, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	var seq int64
	err = s.db.QueryRow(
		ctx,
		`INSERT INTO miniapp_events (app_session_id, actor_user_id, event_type, event_name, body, created_at)
		 VALUES ($1::uuid, $2::uuid, $3::miniapp_event_type, $4, $5::jsonb, now())
		 RETURNING event_seq`,
		sessionID,
		nullUUIDArg(actorID),
		eventType,
		eventName,
		string(b),
	).Scan(&seq)
	if err != nil {
		return 0, err
	}

	// P4.3: Publish event to Redis for real-time WebSocket delivery (best-effort, async)
	// Spawn goroutine to avoid blocking hot path. Errors are silently dropped as this is fanout-only.
	if s.redis != nil {
		go func() {
			eventPayload := map[string]any{
				"session_id": sessionID,
				"event_seq":  seq,
				"event_type": eventType,
				"actor_id":   actorID,
				"body":       body,
				"created_at": time.Now().UTC().Format(time.RFC3339),
			}
			if eventJSON, err := json.Marshal(eventPayload); err == nil {
				channel := "miniapp:session:" + sessionID + ":events"
				_ = s.redis.Publish(context.Background(), channel, string(eventJSON)).Err()
			}
		}()
	}

	_, _ = s.db.Exec(ctx, `UPDATE miniapp_sessions SET expires_at = now() + interval '1 hour' WHERE id = $1::uuid`, sessionID)
	return seq, nil
}

// SnapshotSession replaces persisted state and advances the session state version.
func (s *Service) SnapshotSession(ctx context.Context, sessionID string, state any, version int, actorID string) (int, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var currentVersion int
	var appID string
	var endedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT app_id, state_version, ended_at FROM miniapp_sessions WHERE id = $1::uuid FOR UPDATE`, sessionID).Scan(&appID, &currentVersion, &endedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrSessionNotFound
		}
		return 0, err
	}
	if endedAt != nil {
		return 0, ErrSessionEnded
	}

	nextVersion := version
	if nextVersion <= 0 {
		nextVersion = currentVersion + 1
	}
	if nextVersion <= currentVersion {
		return 0, ErrStateVersionConflict
	}

	stateEnvelope := stateEnvelopeFromAny(state)
	stateB, err := json.Marshal(stateEnvelope)
	if err != nil {
		return 0, err
	}

	query := `UPDATE miniapp_sessions SET state = $1::jsonb, state_version = $2, expires_at = now() + interval '1 hour' WHERE id = $3::uuid`
	args := []any{string(stateB), nextVersion, sessionID}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	// P4.1 Event Model: Log snapshot_written event after successful commit
	_ = s.logSnapshotWritten(ctx, sessionID, actorID, nextVersion, len(stateB))

	return nextVersion, nil
}

// ListManifests returns the latest manifest for each public app.
// DEPRECATED: Per P0.1 Ownership Boundaries (docs/miniapp/ownership-boundaries.md),
// reads from the gateway's miniapp_releases and miniapp_installs tables should be delegated
// to the apps service via RegistryClient. This method persists for dev mode fallback only.
// In production, RegistryClient MUST be configured; this fallback should not be used.
// See: Migration 000043_remove_miniapp_legacy_tables
// ListManifests returns the latest manifest for each app id.
func (s *Service) ListManifests(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := s.db.Query(
		ctx,
		`SELECT DISTINCT ON (r.app_id)
		        m.id::text,
		        COALESCE(m.owner_user_id::text, ''),
		        m.manifest,
		        m.created_at,
		        r.app_id,
		        r.version,
		        r.publisher_user_id::text,
		        r.visibility,
		        r.source_type,
		        r.entrypoint_origin,
		        r.preview_origin,
		        r.manifest_hash,
		        r.published_at,
		        i.installed_version,
		        COALESCE(i.auto_update, false),
		        COALESCE(i.enabled, false),
		        i.installed_at,
		        i.updated_at
		   FROM miniapp_releases r
		   JOIN miniapp_manifests m ON m.id = r.manifest_id
		   LEFT JOIN miniapp_installs i
		     ON i.app_id = r.app_id
		    AND i.user_id = $1::uuid
		  WHERE r.visibility = 'public'
		  ORDER BY r.app_id, r.published_at DESC, r.created_at DESC`,
		nullUUIDArg(userID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []map[string]any{}
	for rows.Next() {
		var entry catalogEntry
		var manifestB []byte
		var installedVersion *string
		if err := rows.Scan(
			&entry.ID,
			&entry.OwnerUserID,
			&manifestB,
			&entry.CreatedAt,
			&entry.AppID,
			&entry.Version,
			&entry.PublisherUserID,
			&entry.Visibility,
			&entry.SourceType,
			&entry.EntrypointOrigin,
			&entry.PreviewOrigin,
			&entry.ManifestHash,
			&entry.PublishedAt,
			&installedVersion,
			&entry.Install.AutoUpdate,
			&entry.Install.Enabled,
			&entry.Install.InstalledAt,
			&entry.Install.UpdatedAt,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(manifestB, &entry.Manifest)
		if installedVersion != nil && *installedVersion != "" {
			entry.Install.Installed = true
			entry.Install.InstalledVersion = *installedVersion
		}
		out = append(out, catalogEntryToMap(entry))
	}
	return out, nil
}

// InstallApp marks an app as installed for a user in the gateway registry.
// DEPRECATED: Per P0.1 Ownership Boundaries (docs/miniapp/ownership-boundaries.md),
// the apps service is the sole owner of install state and should be queried via RegistryClient.
// This method persists for backward compatibility in dev mode when RegistryClient is not configured.
// In production, applications MUST configure RegistryClient; this fallback should not be used.
// See also: Migration 000043_remove_miniapp_legacy_tables
func (s *Service) InstallApp(ctx context.Context, userID, appID string) (map[string]any, error) {
	entry, err := s.loadCatalogEntryByAppID(ctx, userID, appID)
	if err != nil {
		return nil, err
	}
	_, err = s.db.Exec(
		ctx,
		`INSERT INTO miniapp_installs (user_id, app_id, installed_version, auto_update, enabled, installed_at, updated_at)
		 VALUES ($1::uuid, $2, $3, true, true, now(), now())
		 ON CONFLICT (user_id, app_id)
		 DO UPDATE SET installed_version = EXCLUDED.installed_version, enabled = true, updated_at = now()`,
		userID,
		appID,
		entry.Version,
	)
	if err != nil {
		return nil, err
	}
	return s.GetManifestByAppID(ctx, userID, appID)
}

// UninstallApp marks an app as uninstalled for a user in the gateway registry.
// DEPRECATED: Per P0.1 Ownership Boundaries (docs/miniapp/ownership-boundaries.md),
// the apps service is the sole owner of install state and should be queried via RegistryClient.
// This method persists for backward compatibility in dev mode when RegistryClient is not configured.
// In production, applications MUST configure RegistryClient; this fallback should not be used.
// See also: Migration 000043_remove_miniapp_legacy_tables
func (s *Service) UninstallApp(ctx context.Context, userID, appID string) error {
	tag, err := s.db.Exec(ctx, `DELETE FROM miniapp_installs WHERE user_id = $1::uuid AND app_id = $2`, userID, appID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAppInstallNotFound
	}
	return nil
}

func catalogEntryToMap(entry catalogEntry) map[string]any {
	return map[string]any{
		"id":                entry.ID,
		"app_id":            entry.AppID,
		"version":           entry.Version,
		"owner_user_id":     entry.OwnerUserID,
		"publisher_user_id": entry.PublisherUserID,
		"visibility":        entry.Visibility,
		"source_type":       entry.SourceType,
		"entrypoint_origin": entry.EntrypointOrigin,
		"preview_origin":    entry.PreviewOrigin,
		"manifest_hash":     entry.ManifestHash,
		"manifest":          entry.Manifest,
		"created_at":        entry.CreatedAt,
		"published_at":      entry.PublishedAt,
		"install":           entry.Install,
	}
}

// loadCatalogEntryByAppID fetches app metadata and user install state from the gateway registry.
// DEPRECATED: Per P0.1 Ownership Boundaries (docs/miniapp/ownership-boundaries.md),
// reads from miniapp_releases and miniapp_installs should be delegated to the apps service
// via RegistryClient. This method persists for dev mode fallback only.
// In production, RegistryClient MUST be configured.
// See: Migration 000043_remove_miniapp_legacy_tables
func (s *Service) loadCatalogEntryByAppID(ctx context.Context, userID, appID string) (catalogEntry, error) {
	var entry catalogEntry
	var manifestB []byte
	var installedVersion *string
	err := s.db.QueryRow(
		ctx,
		`SELECT m.id::text,
		        COALESCE(m.owner_user_id::text, ''),
		        m.manifest,
		        m.created_at,
		        r.app_id,
		        r.version,
		        r.publisher_user_id::text,
		        r.visibility,
		        r.source_type,
		        r.entrypoint_origin,
		        r.preview_origin,
		        r.manifest_hash,
		        r.published_at,
		        i.installed_version,
		        COALESCE(i.auto_update, false),
		        COALESCE(i.enabled, false),
		        i.installed_at,
		        i.updated_at
		   FROM miniapp_releases r
		   JOIN miniapp_manifests m ON m.id = r.manifest_id
		   LEFT JOIN miniapp_installs i
		     ON i.app_id = r.app_id
		    AND i.user_id = $2::uuid
		  WHERE r.app_id = $1
		    AND r.visibility = 'public'
		  ORDER BY r.published_at DESC, r.created_at DESC
		  LIMIT 1`,
		appID,
		nullUUIDArg(userID),
	).Scan(
		&entry.ID,
		&entry.OwnerUserID,
		&manifestB,
		&entry.CreatedAt,
		&entry.AppID,
		&entry.Version,
		&entry.PublisherUserID,
		&entry.Visibility,
		&entry.SourceType,
		&entry.EntrypointOrigin,
		&entry.PreviewOrigin,
		&entry.ManifestHash,
		&entry.PublishedAt,
		&installedVersion,
		&entry.Install.AutoUpdate,
		&entry.Install.Enabled,
		&entry.Install.InstalledAt,
		&entry.Install.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return catalogEntry{}, ErrManifestNotFound
		}
		return catalogEntry{}, err
	}
	_ = json.Unmarshal(manifestB, &entry.Manifest)
	if installedVersion != nil && *installedVersion != "" {
		entry.Install.Installed = true
		entry.Install.InstalledVersion = *installedVersion
	}
	return entry, nil
}

func (s *Service) resolveManifest(ctx context.Context, manifestID, appID string) (string, string, []string, error) {
	switch {
	case manifestID != "":
		row, err := s.GetManifestByID(ctx, manifestID)
		if err != nil {
			return "", "", nil, err
		}
		manifestMap, _ := row["manifest"].(map[string]any)
		return row["id"].(string), stringField(manifestMap, "app_id"), permissionsFromManifest(manifestMap), nil
	case appID != "":
		row, err := s.loadCatalogEntryByAppID(ctx, "", appID)
		if err != nil {
			return "", "", nil, err
		}
		return row.ID, row.AppID, permissionsFromManifest(row.Manifest), nil
	default:
		return "", "", nil, ErrManifestNotFound
	}
}

func (s *Service) manifestPermissionsForAppID(ctx context.Context, appID string) ([]string, error) {
	row, err := s.loadCatalogEntryByAppID(ctx, "", appID)
	if err != nil {
		return nil, err
	}
	return permissionsFromManifest(row.Manifest), nil
}

func permissionsFromManifest(manifest map[string]any) []string {
	rawPermissions, _ := manifest["permissions"].([]any)
	out := make([]string, 0, len(rawPermissions))
	for _, raw := range rawPermissions {
		if permission, ok := raw.(string); ok && permission != "" {
			out = append(out, permission)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func sanitizeGrantedPermissions(requested, allowed []string) []string {
	if len(allowed) == 0 {
		return nil
	}
	if len(requested) == 0 {
		return append([]string(nil), allowed...)
	}
	allowedSet := map[string]struct{}{}
	for _, permission := range allowed {
		allowedSet[permission] = struct{}{}
	}
	out := make([]string, 0, len(requested))
	for _, permission := range requested {
		if _, ok := allowedSet[permission]; ok {
			out = append(out, permission)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (s *Service) findActiveSession(ctx context.Context, appID, conversationID string) (*sessionRecord, error) {
	var sessionID string
	err := s.db.QueryRow(
		ctx,
		`SELECT id::text
		   FROM miniapp_sessions
		  WHERE app_id = $1 AND conversation_id = $2::uuid AND ended_at IS NULL
		  ORDER BY created_at DESC
		  LIMIT 1`,
		appID,
		conversationID,
	).Scan(&sessionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSessionNotFound
		}
		return nil, err
	}
	record, err := s.loadSessionRecord(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *Service) refreshSession(ctx context.Context, sessionID string, participants []SessionParticipant, _ []string, ttl time.Duration) error {
	partsB, _ := json.Marshal(participants)
	_, err := s.db.Exec(
		ctx,
		`UPDATE miniapp_sessions
		    SET participants = $1::jsonb,
		        expires_at = $2
		  WHERE id = $3::uuid`,
		string(partsB),
		time.Now().Add(ttl),
		sessionID,
	)
	return err
}

func (s *Service) loadSessionRecord(ctx context.Context, sessionID string) (sessionRecord, error) {
	var record sessionRecord
	var participantsB, permissionsB, participantPermissionsB, stateB []byte
	var createdBy *string
	err := s.db.QueryRow(
		ctx,
		`SELECT s.id::text,
		        s.manifest_id::text,
		        s.app_id,
		        COALESCE((SELECT r.version FROM miniapp_releases r WHERE r.manifest_id = s.manifest_id ORDER BY r.published_at DESC NULLS LAST, r.created_at DESC LIMIT 1), ''),
		        s.conversation_id::text,
		        s.participants,
		        s.granted_permissions,
		        s.participant_permissions,
		        s.state,
		        s.state_version,
		        s.created_by::text,
		        s.expires_at,
		        s.created_at,
		        s.ended_at
		   FROM miniapp_sessions s
		  WHERE s.id = $1::uuid`,
		sessionID,
	).Scan(
		&record.ID,
		&record.ManifestID,
		&record.AppID,
		&record.AppVersion,
		&record.ConversationID,
		&participantsB,
		&permissionsB,
		&participantPermissionsB,
		&stateB,
		&record.StateVersion,
		&createdBy,
		&record.ExpiresAt,
		&record.CreatedAt,
		&record.EndedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return sessionRecord{}, ErrSessionNotFound
		}
		return sessionRecord{}, err
	}
	if createdBy != nil {
		record.CreatedBy = *createdBy
	}
	record.Participants = decodeParticipants(participantsB)
	record.GlobalPermissions = decodeStringList(permissionsB)
	record.ParticipantPermissions = decodeParticipantPermissions(participantPermissionsB)
	record.State = decodeSessionState(stateB)
	return record, nil
}

func (s *Service) sessionRecordToMap(record sessionRecord) map[string]any {
	viewer := pickViewer(record.Participants, record.CreatedBy)

	// P3.2 Isolated Runtime Origins: Generate isolated origin for this app runtime
	originCfg := config.GenerateOriginConfig(config.OriginGenerationParams{
		AppID:        record.AppID,
		ReleaseID:    record.AppVersion,
		BaseDomain:   "miniapp.local",
		SubdomainLen: 8,
	})

	return map[string]any{
		"app_session_id":          record.ID,
		"manifest_id":             record.ManifestID,
		"app_id":                  record.AppID,
		"app_version":             record.AppVersion,
		"conversation_id":         record.ConversationID,
		"participants":            record.Participants,
		"capabilities_granted":    record.viewerGrantedPermissions(viewer.UserID),
		"participant_permissions": record.ParticipantPermissions,
		"state":                   record.State,
		"state_version":           record.StateVersion,
		"expires_at":              record.ExpiresAt,
		"created_at":              record.CreatedAt,
		"launch_context":          buildLaunchContext(record),
		"ended_at":                record.EndedAt,
		"app_origin":              originCfg.AppOrigin,
		"csp_header":              originCfg.CSPHeader,
	}
}

func buildLaunchContext(record sessionRecord) map[string]any {
	viewer := pickViewer(record.Participants, record.CreatedBy)

	// P3.2 Isolated Runtime Origins: Include origin in launch context for client-side iframe setup
	originCfg := config.GenerateOriginConfig(config.OriginGenerationParams{
		AppID:        record.AppID,
		ReleaseID:    record.AppVersion,
		BaseDomain:   "miniapp.local",
		SubdomainLen: 8,
	})

	return map[string]any{
		"bridge_version":       "1.0",
		"app_id":               record.AppID,
		"app_version":          record.AppVersion,
		"app_session_id":       record.ID,
		"conversation_id":      record.ConversationID,
		"viewer":               viewer,
		"participants":         record.Participants,
		"capabilities_granted": record.viewerGrantedPermissions(viewer.UserID),
		"host_capabilities": []string{
			"conversation.read_context",
			"conversation.send_message",
			"participants.read_basic",
			"storage.session",
			"storage.shared_conversation",
			"realtime.session",
			"media.pick_user",
			"notifications.in_app",
		},
		"state_snapshot": record.State.Snapshot,
		"state_version":  record.StateVersion,
		"joinable":       record.EndedAt == nil,
		"app_origin":     originCfg.AppOrigin,
	}
}

func defaultSessionState(snapshot any) sessionState {
	return sessionState{
		Snapshot:                  normalizeJSONObject(snapshot),
		SessionStorage:            map[string]any{},
		SharedConversationStorage: map[string]any{},
		ProjectedMessages:         []map[string]any{},
	}
}

func stateEnvelopeFromAny(state any) sessionState {
	if state == nil {
		return defaultSessionState(nil)
	}
	if envelope, ok := state.(map[string]any); ok {
		if _, hasSnapshot := envelope["snapshot"]; hasSnapshot {
			return sessionState{
				Snapshot:                  normalizeJSONObject(envelope["snapshot"]),
				SessionStorage:            normalizeJSONObject(envelope["session_storage"]),
				SharedConversationStorage: normalizeJSONObject(envelope["shared_conversation_storage"]),
				ProjectedMessages:         normalizeMessageList(envelope["projected_messages"]),
			}
		}
	}
	return defaultSessionState(state)
}

func decodeSessionState(raw []byte) sessionState {
	if len(raw) == 0 {
		return defaultSessionState(nil)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return defaultSessionState(nil)
	}
	return stateEnvelopeFromAny(value)
}

func decodeParticipants(raw []byte) []SessionParticipant {
	if len(raw) == 0 {
		return nil
	}
	var participants []SessionParticipant
	if err := json.Unmarshal(raw, &participants); err == nil {
		return normalizeParticipants(participants, SessionParticipant{})
	}
	var legacy []string
	if err := json.Unmarshal(raw, &legacy); err == nil {
		out := make([]SessionParticipant, 0, len(legacy))
		for _, userID := range legacy {
			if userID == "" {
				continue
			}
			out = append(out, SessionParticipant{UserID: userID, Role: "PLAYER"})
		}
		return out
	}
	return nil
}

func decodeStringList(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func normalizeParticipants(participants []SessionParticipant, viewer SessionParticipant) []SessionParticipant {
	out := make([]SessionParticipant, 0, len(participants)+1)
	seen := map[string]struct{}{}
	if viewer.UserID != "" {
		viewer = normalizeParticipant(viewer)
		out = append(out, viewer)
		seen[viewer.UserID] = struct{}{}
	}
	for _, participant := range participants {
		participant = normalizeParticipant(participant)
		if participant.UserID == "" {
			continue
		}
		if _, exists := seen[participant.UserID]; exists {
			continue
		}
		seen[participant.UserID] = struct{}{}
		out = append(out, participant)
	}
	return out
}

func normalizeParticipant(participant SessionParticipant) SessionParticipant {
	if participant.Role == "" {
		participant.Role = "PLAYER"
	}
	return participant
}

func pickViewer(participants []SessionParticipant, createdBy string) SessionParticipant {
	for _, participant := range participants {
		if participant.UserID == createdBy {
			return participant
		}
	}
	if len(participants) > 0 {
		return participants[0]
	}
	return SessionParticipant{}
}

func normalizeJSONObject(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	b, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return map[string]any{}
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}

func normalizeMessageList(value any) []map[string]any {
	b, err := json.Marshal(value)
	if err != nil {
		return []map[string]any{}
	}
	var out []map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return []map[string]any{}
	}
	if out == nil {
		return []map[string]any{}
	}
	return out
}

func nullUUIDArg(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// auditLogCapabilityAllowed logs successful capability enforcement.
// This is used for compliance, debugging, and forensic analysis.
func (s *Service) auditLogCapabilityAllowed(ctx context.Context, userID, sessionID, bridgeMethod string) error {
	payload := map[string]any{
		"session_id":    sessionID,
		"bridge_method": bridgeMethod,
		"allowed":       true,
	}
	return securityaudit.Append(ctx, s.db, userID, userID, "bridge_method_allowed", payload)
}

// auditLogCapabilityDenied logs denied capability enforcement with reason.
// This is used for compliance, debugging, and forensic analysis.
func (s *Service) auditLogCapabilityDenied(ctx context.Context, userID, sessionID, bridgeMethod, reason string) error {
	payload := map[string]any{
		"session_id":    sessionID,
		"bridge_method": bridgeMethod,
		"allowed":       false,
		"deny_reason":   reason,
	}
	return securityaudit.Append(ctx, s.db, userID, userID, "bridge_method_denied", payload)
}

// ReleaseStatus represents the suspension/revocation status of a release
type ReleaseStatus struct {
	AppID            string     `json:"app_id"`
	Version          string     `json:"version"`
	ReviewStatus     string     `json:"review_status"`
	RevokedAt        *time.Time `json:"revoked_at,omitempty"`
	SuspendedAt      *time.Time `json:"suspended_at,omitempty"`
	SuspensionReason string     `json:"suspension_reason,omitempty"`
}

// CheckReleaseStatus checks if a release is suspended or revoked
func (s *Service) CheckReleaseStatus(ctx context.Context, appID, version string) (*ReleaseStatus, error) {
	if appID == "" || version == "" {
		return nil, ErrManifestNotFound
	}

	var status ReleaseStatus
	var suspendedAt, revokedAt *time.Time

	err := s.db.QueryRow(
		ctx,
		`SELECT review_status, suspended_at, revoked_at
		   FROM miniapp_releases
		  WHERE app_id = $1 AND version = $2
		  LIMIT 1`,
		appID,
		version,
	).Scan(&status.ReviewStatus, &suspendedAt, &revokedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}

	status.AppID = appID
	status.Version = version
	status.SuspendedAt = suspendedAt
	status.RevokedAt = revokedAt
	return &status, nil
}

// IsReleaseAvailable checks if a release is available (not suspended or revoked)
func (s *Service) IsReleaseAvailable(ctx context.Context, appID, version string) (bool, error) {
	status, err := s.CheckReleaseStatus(ctx, appID, version)
	if err != nil {
		return false, err
	}
	if status == nil {
		return false, nil
	}
	// Release is NOT available if suspended or revoked
	if status.SuspendedAt != nil || status.RevokedAt != nil {
		return false, nil
	}
	return true, nil
}

// TerminateSessionsForRelease gracefully terminates all active sessions for a suspended/revoked release
// Returns the number of affected sessions
func (s *Service) TerminateSessionsForRelease(ctx context.Context, appID, version, reason string) (int64, error) {
	// Find all active sessions for this app
	rows, err := s.db.Query(
		ctx,
		`SELECT id::text FROM miniapp_sessions
		  WHERE app_id = $1 AND ended_at IS NULL`,
		appID,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var sessionIDs []string
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return 0, err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// Send suspension event to each session and end it
	for _, sessionID := range sessionIDs {
		// End the session gracefully
		_ = s.EndSession(ctx, sessionID)
	}

	return int64(len(sessionIDs)), nil
}

// StartCacheInvalidationListener starts listening for release suspension/revocation events
// Returns a channel for communication and a cancel function
func (s *Service) StartCacheInvalidationListener(ctx context.Context) (<-chan string, context.CancelFunc) {
	if s.redis == nil {
		// Redis not available; return dummy channel that never receives
		cancel := func() {}
		return make(chan string), cancel
	}

	msgChan := make(chan string, 100) // Buffered to avoid blocking
	cancelCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(msgChan)

		pubsub := s.redis.Subscribe(cancelCtx, "miniapp:release:invalidation")
		defer pubsub.Close()

		// Use Channel() to get a channel from pubsub
		for {
			select {
			case <-cancelCtx.Done():
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}
				if msg != nil && msg.Payload != "" {
					select {
					case msgChan <- msg.Payload:
					case <-cancelCtx.Done():
						return
					}
				}
			}
		}
	}()

	return msgChan, cancel
}

// PublishReleaseInvalidation publishes a cache invalidation event for a suspended/revoked release
func (s *Service) PublishReleaseInvalidation(ctx context.Context, appID, version, reason string) error {
	if s.redis == nil {
		return nil // Redis not available; skip publication
	}

	payload := map[string]any{
		"event_type": "release_invalidation",
		"app_id":     appID,
		"version":    version,
		"reason":     reason,
		"timestamp":  time.Now().UTC().Unix(),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return s.redis.Publish(ctx, "miniapp:release:invalidation", string(data)).Err()
}
