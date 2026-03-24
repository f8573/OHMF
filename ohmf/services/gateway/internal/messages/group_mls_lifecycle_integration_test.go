package messages

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"ohmf/services/gateway/internal/conversations"
	"ohmf/services/gateway/internal/e2ee"
	"ohmf/services/gateway/internal/replication"
)

func TestGroupMLSE2EEConversationLifecycle(t *testing.T) {
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

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	store := replication.NewStore(pool, rdb)
	mlsStore := e2ee.NewMLSSessionStore(pool)
	convSvc := conversations.NewService(pool, store, mlsStore)
	msgSvc := NewService(pool, Options{Redis: rdb, Replication: store})

	aliceID := insertTestUser(t, ctx, pool)
	bobID := insertTestUser(t, ctx, pool)
	carolID := insertTestUser(t, ctx, pool)

	aliceDeviceID, aliceSigningKey := enableMLSTestDevice(t, ctx, pool, aliceID)
	bobDeviceID, bobSigningKey := enableMLSTestDevice(t, ctx, pool, bobID)
	carolDeviceID, _ := enableMLSTestDevice(t, ctx, pool, carolID)

	conversation, err := convSvc.CreateConversation(ctx, aliceID, conversations.CreateRequest{
		Type:         "GROUP",
		Participants: []string{bobID, carolID},
	})
	if err != nil {
		t.Fatalf("create encrypted group: %v", err)
	}
	if conversation.EncryptionState != "ENCRYPTED" {
		t.Fatalf("expected ENCRYPTED group, got %q", conversation.EncryptionState)
	}
	if !conversation.MLSEnabled {
		t.Fatal("expected MLS to be enabled for encrypted group")
	}
	if conversation.MLSEpoch != 1 || conversation.EncryptionEpoch != 1 {
		t.Fatalf("expected initial epochs to be 1, got encryption=%d mls=%d", conversation.EncryptionEpoch, conversation.MLSEpoch)
	}
	if !conversation.E2EEReady {
		t.Fatalf("expected encrypted group to be E2EE ready, blocked=%v", conversation.E2EEBlockedMemberIDs)
	}
	if conversation.MLSTreeHash == "" {
		t.Fatal("expected MLS tree hash on encrypted group")
	}

	recipients := []RecipientKeyInfo{
		{UserID: aliceID, DeviceID: aliceDeviceID},
		{UserID: bobID, DeviceID: bobDeviceID},
		{UserID: carolID, DeviceID: carolDeviceID},
	}
	epochDigest := "epoch-digest-v1"

	originalContent := buildMLSContent(t, aliceSigningKey, conversation, aliceID, aliceDeviceID, "ciphertext-original", epochDigest, recipients, true)
	originalSend, err := msgSvc.Send(ctx, aliceID, aliceDeviceID, conversation.ConversationID, uuid.NewString(), "encrypted", originalContent, "client-original", "trace-mls-original", "127.0.0.1")
	if err != nil {
		t.Fatalf("send original MLS message: %v", err)
	}
	if originalSend.Message.Status != "SENT" {
		t.Fatalf("expected initial status SENT, got %q", originalSend.Message.Status)
	}
	if !originalSend.Message.IsEncrypted || originalSend.Message.EncryptionScheme != MLSEncryptionScheme {
		t.Fatalf("expected MLS encrypted send result, got encrypted=%v scheme=%q", originalSend.Message.IsEncrypted, originalSend.Message.EncryptionScheme)
	}

	processAllDomainEvents(t, ctx, store)
	assertStoredMLSEpochMaterial(t, ctx, pool, conversation.ConversationID, int64(conversation.MLSEpoch), epochDigest, len(recipients))

	if err := store.AdvanceDeliveredCheckpoint(ctx, bobID, "", conversation.ConversationID, originalSend.Message.ServerOrder); err != nil {
		t.Fatalf("advance bob delivered checkpoint: %v", err)
	}
	if err := store.AdvanceDeliveredCheckpoint(ctx, carolID, "", conversation.ConversationID, originalSend.Message.ServerOrder); err != nil {
		t.Fatalf("advance carol delivered checkpoint: %v", err)
	}
	if err := msgSvc.MarkRead(ctx, bobID, conversation.ConversationID, originalSend.Message.ServerOrder); err != nil {
		t.Fatalf("mark original as read for bob: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	originalView, err := msgSvc.loadMessageViewByID(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("load original message for alice: %v", err)
	}
	if originalView.Status != "READ" {
		t.Fatalf("expected alice to see original as READ, got %q", originalView.Status)
	}
	if originalView.DeliveredAt == "" || originalView.ReadAt == "" {
		t.Fatalf("expected delivery/read timestamps on original message, got delivered=%q read=%q", originalView.DeliveredAt, originalView.ReadAt)
	}

	readStatus, err := msgSvc.GetConversationReadStatus(ctx, aliceID, conversation.ConversationID)
	if err != nil {
		t.Fatalf("get conversation read status: %v", err)
	}
	assertMemberCheckpoint(t, readStatus, bobID, "last_read_server_order", originalSend.Message.ServerOrder)
	assertMemberCheckpoint(t, readStatus, carolID, "last_delivered_server_order", originalSend.Message.ServerOrder)

	replyContent := buildMLSContent(t, bobSigningKey, conversation, bobID, bobDeviceID, "ciphertext-reply", epochDigest, recipients, false)
	replyContent["reply_to_message_id"] = originalSend.Message.MessageID
	replySend, err := msgSvc.Send(ctx, bobID, bobDeviceID, conversation.ConversationID, uuid.NewString(), "encrypted", replyContent, "client-reply", "trace-mls-reply", "127.0.0.1")
	if err != nil {
		t.Fatalf("send encrypted reply: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	originalWithReply, err := msgSvc.loadMessageViewByID(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("reload original message after reply: %v", err)
	}
	if originalWithReply.ReplyCount != 1 {
		t.Fatalf("expected reply_count 1 after reply, got %d", originalWithReply.ReplyCount)
	}

	repliesBeforeDelete, err := msgSvc.ListReplies(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("list replies before delete: %v", err)
	}
	if len(repliesBeforeDelete) != 1 {
		t.Fatalf("expected 1 reply before delete, got %d", len(repliesBeforeDelete))
	}
	if repliesBeforeDelete[0].MessageID != replySend.Message.MessageID || repliesBeforeDelete[0].ReplyToMessageID != originalSend.Message.MessageID {
		t.Fatalf("unexpected reply projection: %#v", repliesBeforeDelete[0])
	}

	if err := store.AdvanceDeliveredCheckpoint(ctx, aliceID, "", conversation.ConversationID, replySend.Message.ServerOrder); err != nil {
		t.Fatalf("advance alice delivered checkpoint for reply: %v", err)
	}
	if err := msgSvc.MarkRead(ctx, aliceID, conversation.ConversationID, replySend.Message.ServerOrder); err != nil {
		t.Fatalf("mark reply as read for alice: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	replyView, err := msgSvc.loadMessageViewByID(ctx, bobID, replySend.Message.MessageID)
	if err != nil {
		t.Fatalf("load reply message for bob: %v", err)
	}
	if replyView.Status != "READ" || replyView.DeliveredAt == "" || replyView.ReadAt == "" {
		t.Fatalf("expected bob to see reply as READ with timestamps, got status=%q delivered=%q read=%q", replyView.Status, replyView.DeliveredAt, replyView.ReadAt)
	}

	editedContent := buildMLSContent(t, aliceSigningKey, conversation, aliceID, aliceDeviceID, "ciphertext-original-edited", epochDigest, recipients, false)
	if err := msgSvc.EditMessage(ctx, aliceID, aliceDeviceID, originalSend.Message.MessageID, editedContent); err != nil {
		t.Fatalf("edit original MLS message: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	editHistory, err := msgSvc.GetMessageEditHistory(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("get edit history: %v", err)
	}
	if len(editHistory) != 1 {
		t.Fatalf("expected 1 edit history row, got %d", len(editHistory))
	}
	if editHistory[0].PreviousContent["ciphertext"] != originalContent["ciphertext"] {
		t.Fatalf("expected original ciphertext in edit history, got %#v", editHistory[0].PreviousContent["ciphertext"])
	}
	if editHistory[0].NewContent["ciphertext"] != editedContent["ciphertext"] {
		t.Fatalf("expected edited ciphertext in edit history, got %#v", editHistory[0].NewContent["ciphertext"])
	}
	if editHistory[0].SentAt == "" || editHistory[0].DeliveredAt == "" || editHistory[0].ReadAt == "" {
		t.Fatalf("expected sent/delivered/read timestamps in edit history, got sent=%q delivered=%q read=%q", editHistory[0].SentAt, editHistory[0].DeliveredAt, editHistory[0].ReadAt)
	}

	if err := msgSvc.AddReaction(ctx, carolID, originalSend.Message.MessageID, "thumbs_up"); err != nil {
		t.Fatalf("add reaction to original message: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	reactionHistory, err := msgSvc.GetMessageReactionHistory(ctx, carolID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("get reaction history: %v", err)
	}
	if len(reactionHistory) != 1 {
		t.Fatalf("expected 1 reaction history row, got %d", len(reactionHistory))
	}
	if reactionHistory[0].Action != "added" || reactionHistory[0].Emoji != "thumbs_up" {
		t.Fatalf("unexpected reaction history row: %#v", reactionHistory[0])
	}
	if reactionHistory[0].SentAt == "" || reactionHistory[0].DeliveredAt == "" || reactionHistory[0].ReadAt == "" {
		t.Fatalf("expected sent/delivered/read timestamps in reaction history, got sent=%q delivered=%q read=%q", reactionHistory[0].SentAt, reactionHistory[0].DeliveredAt, reactionHistory[0].ReadAt)
	}

	originalAfterReaction, err := msgSvc.loadMessageViewByID(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("load original after reaction: %v", err)
	}
	if originalAfterReaction.Reactions["thumbs_up"] != 1 {
		t.Fatalf("expected thumbs_up reaction count 1, got %#v", originalAfterReaction.Reactions["thumbs_up"])
	}
	if originalAfterReaction.EditedAt == "" {
		t.Fatalf("expected edited_at on original message after edit")
	}
	if originalAfterReaction.Content["ciphertext"] != editedContent["ciphertext"] {
		t.Fatalf("expected edited ciphertext on original message, got %#v", originalAfterReaction.Content["ciphertext"])
	}

	if err := msgSvc.DeleteMessage(ctx, bobID, replySend.Message.MessageID); err != nil {
		t.Fatalf("delete reply message: %v", err)
	}
	processAllDomainEvents(t, ctx, store)

	replyAfterDelete, err := msgSvc.loadMessageViewByID(ctx, aliceID, replySend.Message.MessageID)
	if err != nil {
		t.Fatalf("load deleted reply: %v", err)
	}
	if !replyAfterDelete.Deleted || replyAfterDelete.VisibilityState != "SOFT_DELETED" || len(replyAfterDelete.Content) != 0 {
		t.Fatalf("expected tombstoned deleted reply, got deleted=%v visibility=%q content=%#v", replyAfterDelete.Deleted, replyAfterDelete.VisibilityState, replyAfterDelete.Content)
	}

	repliesAfterDelete, err := msgSvc.ListReplies(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("list replies after delete: %v", err)
	}
	if len(repliesAfterDelete) != 1 || !repliesAfterDelete[0].Deleted {
		t.Fatalf("expected deleted reply to remain visible as tombstone, got %#v", repliesAfterDelete)
	}

	originalAfterDelete, err := msgSvc.loadMessageViewByID(ctx, aliceID, originalSend.Message.MessageID)
	if err != nil {
		t.Fatalf("reload original after reply delete: %v", err)
	}
	if originalAfterDelete.ReplyCount != 0 {
		t.Fatalf("expected reply_count 0 after reply delete, got %d", originalAfterDelete.ReplyCount)
	}

	aliceEvents, err := store.ListEvents(ctx, aliceID, 0, 100)
	if err != nil {
		t.Fatalf("list alice user events: %v", err)
	}
	assertEventTypesPresent(t, aliceEvents.Events,
		replication.UserEventConversationMessageAppended,
		replication.UserEventConversationMessageEdited,
		replication.UserEventConversationMessageDeleted,
		replication.UserEventConversationMessageReactionsUpdated,
		replication.UserEventConversationReceiptUpdated,
	)

	bobEvents, err := store.ListEvents(ctx, bobID, 0, 100)
	if err != nil {
		t.Fatalf("list bob user events: %v", err)
	}
	assertEventTypesPresent(t, bobEvents.Events,
		replication.UserEventConversationMessageAppended,
		replication.UserEventConversationMessageEdited,
		replication.UserEventConversationMessageDeleted,
		replication.UserEventConversationMessageReactionsUpdated,
		replication.UserEventConversationReceiptUpdated,
	)
}

func enableMLSTestDevice(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID string) (string, ed25519.PrivateKey) {
	t.Helper()

	deviceID := uuid.NewString()
	publicKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	signingPublicKey := base64.StdEncoding.EncodeToString(publicKey)
	agreementPublicKey := base64.StdEncoding.EncodeToString([]byte("agreement-" + deviceID))
	identityPublicKey := base64.StdEncoding.EncodeToString([]byte("identity-" + deviceID))
	signedPrekeyPublicKey := base64.StdEncoding.EncodeToString([]byte("signed-prekey-" + deviceID))
	signedPrekeySignature := base64.StdEncoding.EncodeToString(ed25519.Sign(signingKey, []byte(signedPrekeyPublicKey)))

	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (id, user_id, platform, device_name, capabilities)
		VALUES ($1::uuid, $2::uuid, 'web', 'test mls device', ARRAY['E2EE_OTT_V2']::text[])
	`, deviceID, userID); err != nil {
		t.Fatalf("insert MLS device %s: %v", deviceID, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO device_identity_keys (
			device_id,
			user_id,
			identity_public_key,
			signed_prekey_id,
			signed_prekey_public_key,
			signed_prekey_signature,
			agreement_identity_public_key,
			signing_key_alg,
			signing_public_key,
			bundle_version
		)
		VALUES ($1::uuid, $2::uuid, $3, 1, $4, $5, $6, 'ED25519', $7, 'OHMF_SIGNAL_V1')
	`, deviceID, userID, identityPublicKey, signedPrekeyPublicKey, signedPrekeySignature, agreementPublicKey, signingPublicKey); err != nil {
		t.Fatalf("insert MLS device identity %s: %v", deviceID, err)
	}

	return deviceID, signingKey
}

func buildMLSContent(t *testing.T, signingKey ed25519.PrivateKey, conversation conversations.Conversation, senderUserID, senderDeviceID, ciphertextSeed, epochDigest string, recipients []RecipientKeyInfo, includeBoxes bool) map[string]any {
	t.Helper()

	ciphertext := base64.StdEncoding.EncodeToString([]byte(ciphertextSeed))
	nonce := base64.StdEncoding.EncodeToString([]byte("nonce-" + ciphertextSeed))

	boxesRaw := make([]any, 0, len(recipients))
	if includeBoxes {
		for _, recipient := range recipients {
			boxesRaw = append(boxesRaw, map[string]any{
				"user_id":     recipient.UserID,
				"device_id":   recipient.DeviceID,
				"wrapped_key": base64.StdEncoding.EncodeToString([]byte("wrapped-" + recipient.DeviceID)),
				"wrap_nonce":  base64.StdEncoding.EncodeToString([]byte("wrapnonce-" + recipient.DeviceID)),
			})
		}
	}

	signaturePayload := mlsEnvelopeSignaturePayload(
		MLSEncryptionScheme,
		int64(conversation.EncryptionEpoch),
		int64(conversation.MLSEpoch),
		conversation.MLSTreeHash,
		epochDigest,
		nonce,
		ciphertext,
		boxesRaw,
	)
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(signingKey, []byte(signaturePayload)))

	encryption := map[string]any{
		"scheme":              MLSEncryptionScheme,
		"sender_user_id":      senderUserID,
		"sender_device_id":    senderDeviceID,
		"sender_signature":    signature,
		"conversation_epoch":  conversation.EncryptionEpoch,
		"mls_epoch":           conversation.MLSEpoch,
		"tree_hash":           conversation.MLSTreeHash,
		"epoch_secret_digest": epochDigest,
	}
	if includeBoxes {
		encryption["epoch_secret_boxes"] = boxesRaw
	}

	return map[string]any{
		"ciphertext": ciphertext,
		"nonce":      nonce,
		"encryption": encryption,
	}
}

func processAllDomainEvents(t *testing.T, ctx context.Context, store *replication.Store) {
	t.Helper()

	for i := 0; i < 10; i++ {
		processed, err := store.ProcessBatch(ctx, 100)
		if err != nil {
			t.Fatalf("process domain events: %v", err)
		}
		if processed == 0 {
			return
		}
	}
	t.Fatal("domain events did not drain after 10 batches")
}

func assertStoredMLSEpochMaterial(t *testing.T, ctx context.Context, pool *pgxpool.Pool, conversationID string, epoch int64, digest string, wantSessions int) {
	t.Helper()

	var storedDigest []byte
	if err := pool.QueryRow(ctx, `
		SELECT group_secret
		FROM group_epochs
		WHERE group_id = $1::uuid AND epoch = $2
	`, conversationID, epoch).Scan(&storedDigest); err != nil {
		t.Fatalf("load stored MLS epoch secret: %v", err)
	}
	if string(storedDigest) != digest {
		t.Fatalf("expected stored epoch digest %q, got %q", digest, string(storedDigest))
	}

	var sessionCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM group_sessions
		WHERE group_id = $1::uuid AND epoch = $2
	`, conversationID, epoch).Scan(&sessionCount); err != nil {
		t.Fatalf("count stored MLS sessions: %v", err)
	}
	if sessionCount != wantSessions {
		t.Fatalf("expected %d stored MLS sessions, got %d", wantSessions, sessionCount)
	}
}

func assertMemberCheckpoint(t *testing.T, status map[string]any, userID, key string, want int64) {
	t.Helper()

	members, ok := status["members"].([]map[string]any)
	if !ok {
		t.Fatalf("unexpected members payload: %#v", status["members"])
	}
	for _, member := range members {
		if member["user_id"] != userID {
			continue
		}
		got, _ := member[key].(int64)
		if got != want {
			t.Fatalf("expected %s %d for user %s, got %#v", key, want, userID, member[key])
		}
		return
	}
	t.Fatalf("missing status row for user %s", userID)
}

func assertEventTypesPresent(t *testing.T, events []replication.Event, wantTypes ...string) {
	t.Helper()

	seen := make(map[string]bool, len(events))
	for _, event := range events {
		seen[event.Type] = true
	}
	for _, want := range wantTypes {
		if !seen[want] {
			t.Fatalf("expected user events to include %q, saw %#v", want, seen)
		}
	}
}
