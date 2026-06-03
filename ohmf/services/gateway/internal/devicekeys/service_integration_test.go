package devicekeys

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"ohmf/services/gateway/internal/testutil"
)

func TestPublishAndClaimBundles(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping DB integration test; set TEST_DATABASE_URL to run")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	applyAllMigrations(t, ctx, pool)

	userID := insertTestUser(t, ctx, pool)
	var deviceID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO devices (user_id, platform, device_name, capabilities)
		VALUES ($1::uuid, 'WEB', 'Browser', ARRAY['MINI_APPS', 'E2EE_OTT_V2'])
		RETURNING id::text
	`, userID).Scan(&deviceID); err != nil {
		t.Fatalf("insert device: %v", err)
	}

	identityPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate identity key: %v", err)
	}
	signedPrekeyPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signed prekey: %v", err)
	}
	signingPublic, signingPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	signedPrekeyPublic := base64.StdEncoding.EncodeToString(signedPrekeyPrivate.PublicKey().Bytes())
	signature := ed25519.Sign(signingPrivate, []byte(signedPrekeyPayload(BundleVersionSignalV1, 7, signedPrekeyPublic)))
	prekeys := make([]OneTimePrekey, 0, minInitialOneTimePrekeys)
	for index := 0; index < minInitialOneTimePrekeys; index += 1 {
		privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate prekey %d: %v", index, err)
		}
		prekeys = append(prekeys, OneTimePrekey{
			PrekeyID:  int64(101 + index),
			PublicKey: base64.StdEncoding.EncodeToString(privateKey.PublicKey().Bytes()),
		})
	}

	svc := NewService(pool)
	bundle, err := svc.PublishBundle(ctx, userID, deviceID, PublishRequest{
		BundleVersion:              BundleVersionSignalV1,
		IdentityKeyAlg:             "X25519",
		IdentityPublicKey:          base64.StdEncoding.EncodeToString(identityPrivate.PublicKey().Bytes()),
		AgreementIdentityPublicKey: base64.StdEncoding.EncodeToString(identityPrivate.PublicKey().Bytes()),
		SigningKeyAlg:              "Ed25519",
		SigningPublicKey:           base64.StdEncoding.EncodeToString(signingPublic),
		SignedPrekeyID:             7,
		SignedPrekeyPublicKey:      signedPrekeyPublic,
		SignedPrekeySignature:      base64.StdEncoding.EncodeToString(signature),
		KeyVersion:                 1,
		OneTimePrekeys:             prekeys,
	})
	if err != nil {
		t.Fatalf("publish bundle: %v", err)
	}
	if bundle.BundleVersion != BundleVersionSignalV1 {
		t.Fatalf("expected signal bundle version, got %q", bundle.BundleVersion)
	}
	if bundle.AvailableOneTimePrekeys != int64(minInitialOneTimePrekeys) {
		t.Fatalf("expected %d available prekeys, got %d", minInitialOneTimePrekeys, bundle.AvailableOneTimePrekeys)
	}
	if bundle.SignedPrekey.PublicKey != signedPrekeyPublic {
		t.Fatalf("unexpected signed prekey public key: %q", bundle.SignedPrekey.PublicKey)
	}
	if bundle.SigningPublicKey != base64.StdEncoding.EncodeToString(signingPublic) {
		t.Fatalf("unexpected signing key: %q", bundle.SigningPublicKey)
	}
	if bundle.Fingerprint == "" {
		t.Fatal("expected fingerprint to be populated")
	}

	listed, err := svc.ListBundlesForUser(ctx, userID)
	if err != nil {
		t.Fatalf("list bundles: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected 1 listed bundle, got %d", len(listed))
	}

	firstClaim, err := svc.ClaimBundles(ctx, userID)
	if err != nil {
		t.Fatalf("claim bundles first: %v", err)
	}
	if len(firstClaim) != 1 || firstClaim[0].ClaimedOneTimePrekey == nil {
		t.Fatalf("expected claimed one-time prekey, got %#v", firstClaim)
	}
	if firstClaim[0].ClaimedOneTimePrekey.PrekeyID != 101 {
		t.Fatalf("expected first claimed prekey 101, got %d", firstClaim[0].ClaimedOneTimePrekey.PrekeyID)
	}

	secondClaim, err := svc.ClaimBundles(ctx, userID)
	if err != nil {
		t.Fatalf("claim bundles second: %v", err)
	}
	if len(secondClaim) != 1 || secondClaim[0].ClaimedOneTimePrekey == nil {
		t.Fatalf("expected second claimed one-time prekey, got %#v", secondClaim)
	}
	if secondClaim[0].ClaimedOneTimePrekey.PrekeyID != 102 {
		t.Fatalf("expected second claimed prekey 102, got %d", secondClaim[0].ClaimedOneTimePrekey.PrekeyID)
	}
}

func applyAllMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	testutil.ResetAndMigrateGateway(t, ctx, pool)
}

func insertTestUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()

	var userID string
	phone := "+test-" + uuid.NewString()
	if err := pool.QueryRow(ctx, `INSERT INTO users (primary_phone_e164) VALUES ($1) RETURNING id::text`, phone).Scan(&userID); err != nil {
		t.Fatalf("insert user %q: %v", phone, err)
	}
	return userID
}
