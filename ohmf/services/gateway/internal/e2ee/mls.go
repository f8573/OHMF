package e2ee

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MLSRatchetTree represents a binary tree for group key derivation
type MLSRatchetTree struct {
	GroupID    string
	Generation int64
	Epoch      int64
	TreeBytes  []byte
	Leaves     map[int]TreeLeaf
}

// TreeLeaf represents a leaf node in the ratchet tree (a group member device)
type TreeLeaf struct {
	Index     int
	UserID    string
	DeviceID  string
	PublicKey []byte
}

// TreeNode represents an internal node in the ratchet tree
type TreeNode struct {
	Index    int
	LeftIdx  int
	RightIdx int
	KeyBytes []byte
}

// NewMLSRatchetTree creates a new ratchet tree for a group
func NewMLSRatchetTree(groupID string) *MLSRatchetTree {
	return &MLSRatchetTree{
		GroupID:    groupID,
		Generation: 0,
		Epoch:      0,
		Leaves:     make(map[int]TreeLeaf),
	}
}

// AddMember inserts a new device as a leaf in the tree, returns assigned leaf index
func (t *MLSRatchetTree) AddMember(leaf TreeLeaf) (int, error) {
	var maxIndex int
	for idx := range t.Leaves {
		if idx > maxIndex {
			maxIndex = idx
		}
	}
	leaf.Index = maxIndex + 1

	t.Leaves[leaf.Index] = leaf
	t.Generation++
	return leaf.Index, nil
}

// RemoveMember blanks leaf node, bumps generation and epoch (forward secrecy)
func (t *MLSRatchetTree) RemoveMember(userID, deviceID string) error {
	for idx, leaf := range t.Leaves {
		if leaf.UserID == userID && leaf.DeviceID == deviceID {
			delete(t.Leaves, idx)
			t.Generation++
			t.Epoch++
			return nil
		}
	}
	return fmt.Errorf("member not found: %s/%s", userID, deviceID)
}

// GetGroupMembers returns all current members sorted by leaf index
func (t *MLSRatchetTree) GetGroupMembers() []TreeLeaf {
	members := make([]TreeLeaf, 0, len(t.Leaves))
	for _, leaf := range t.Leaves {
		members = append(members, leaf)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].Index < members[j].Index
	})
	return members
}

// DeriveGroupKey generates group encryption key for epoch
func (t *MLSRatchetTree) DeriveGroupKey(salt []byte) []byte {
	h := sha256.New()
	h.Write(t.TreeBytes)
	h.Write([]byte(fmt.Sprintf("%d:%d", t.Generation, t.Epoch)))
	h.Write(salt)
	return h.Sum(nil)
}

// ComputeTreeHash computes deterministic hash of current tree state
func (t *MLSRatchetTree) ComputeTreeHash() string {
	h := sha256.New()
	for _, leaf := range t.GetGroupMembers() {
		h.Write([]byte(leaf.DeviceID))
		h.Write(leaf.PublicKey)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// MLSSessionStore manages MLS group sessions in database
type MLSSessionStore struct {
	db *pgxpool.Pool
}

type treeQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// NewMLSSessionStore creates a store for MLS operations
func NewMLSSessionStore(db *pgxpool.Pool) *MLSSessionStore {
	return &MLSSessionStore{db: db}
}

func decodeMLSPublicKey(value string) []byte {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		return decoded
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(trimmed); err == nil {
		return decoded
	}
	return []byte(trimmed)
}

func encodeTreeBytes(leaves []TreeLeaf) []byte {
	parts := make([]string, 0, len(leaves))
	for _, leaf := range leaves {
		parts = append(parts, strings.Join([]string{
			fmt.Sprintf("%d", leaf.Index),
			leaf.UserID,
			leaf.DeviceID,
			base64.StdEncoding.EncodeToString(leaf.PublicKey),
		}, "|"))
	}
	return []byte(strings.Join(parts, "\n"))
}

func BuildMLSTree(groupID string, epoch int64, leaves []TreeLeaf) *MLSRatchetTree {
	cloned := make([]TreeLeaf, 0, len(leaves))
	for _, leaf := range leaves {
		cloned = append(cloned, TreeLeaf{
			UserID:    strings.TrimSpace(leaf.UserID),
			DeviceID:  strings.TrimSpace(leaf.DeviceID),
			PublicKey: append([]byte(nil), leaf.PublicKey...),
		})
	}
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].UserID == cloned[j].UserID {
			return cloned[i].DeviceID < cloned[j].DeviceID
		}
		return cloned[i].UserID < cloned[j].UserID
	})
	tree := NewMLSRatchetTree(groupID)
	tree.Epoch = epoch
	tree.Generation = epoch
	if tree.Generation <= 0 {
		tree.Generation = 1
	}
	tree.Leaves = make(map[int]TreeLeaf, len(cloned))
	for index, leaf := range cloned {
		leaf.Index = index
		tree.Leaves[index] = leaf
	}
	tree.TreeBytes = encodeTreeBytes(tree.GetGroupMembers())
	return tree
}

func BuildConversationMLSTree(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, groupID string, epoch int64) (*MLSRatchetTree, error) {
	rows, err := q.Query(ctx, `
		SELECT cm.user_id::text, d.id::text, COALESCE(dik.agreement_identity_public_key, '')
		FROM conversation_members cm
		JOIN devices d
		  ON d.user_id = cm.user_id
		JOIN device_identity_keys dik
		  ON dik.user_id = d.user_id
		 AND dik.device_id = d.id
		WHERE cm.conversation_id = $1::uuid
		  AND dik.bundle_version = 'OHMF_SIGNAL_V1'
		  AND d.capabilities @> ARRAY['E2EE_OTT_V2']::text[]
		ORDER BY cm.user_id, d.id
	`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	leaves := make([]TreeLeaf, 0, 8)
	for rows.Next() {
		var userID, deviceID, publicKey string
		if err := rows.Scan(&userID, &deviceID, &publicKey); err != nil {
			return nil, err
		}
		leaves = append(leaves, TreeLeaf{
			UserID:    userID,
			DeviceID:  deviceID,
			PublicKey: decodeMLSPublicKey(publicKey),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return BuildMLSTree(groupID, epoch, leaves), nil
}

func PersistConversationMLSTree(ctx context.Context, q treeQuerier, tree *MLSRatchetTree) error {
	if tree == nil {
		return nil
	}
	if tree.TreeBytes == nil {
		tree.TreeBytes = encodeTreeBytes(tree.GetGroupMembers())
	}
	if _, err := q.Exec(ctx, `DELETE FROM group_member_tree_leaves WHERE group_id = $1::uuid`, tree.GroupID); err != nil {
		return err
	}
	for _, leaf := range tree.GetGroupMembers() {
		if _, err := q.Exec(ctx, `
			INSERT INTO group_member_tree_leaves (group_id, user_id, device_id, leaf_index, generation)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5)
		`, tree.GroupID, leaf.UserID, leaf.DeviceID, leaf.Index, tree.Generation); err != nil {
			return err
		}
	}
	_, err := q.Exec(ctx, `
		INSERT INTO group_ratchet_trees (group_id, generation, tree_bytes, epoch)
		VALUES ($1::uuid, $2, $3, $4)
		ON CONFLICT (group_id) DO UPDATE
		SET generation = EXCLUDED.generation,
		    tree_bytes = EXCLUDED.tree_bytes,
		    epoch = EXCLUDED.epoch,
		    updated_at = NOW()
	`, tree.GroupID, tree.Generation, tree.TreeBytes, tree.Epoch)
	return err
}

func ConversationMLSTreeHash(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, groupID string, epoch int64) (string, error) {
	tree, err := BuildConversationMLSTree(ctx, q, groupID, epoch)
	if err != nil {
		return "", err
	}
	return tree.ComputeTreeHash(), nil
}

// SaveRatchetTree persists tree state to database
func (s *MLSSessionStore) SaveRatchetTree(ctx context.Context, tree *MLSRatchetTree) error {
	query := `INSERT INTO group_ratchet_trees (group_id, generation, tree_bytes, epoch)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (group_id) DO UPDATE SET generation = EXCLUDED.generation,
			tree_bytes = EXCLUDED.tree_bytes, epoch = EXCLUDED.epoch, updated_at = NOW()`
	_, err := s.db.Exec(ctx, query, tree.GroupID, tree.Generation, tree.TreeBytes, tree.Epoch)
	return err
}

// LoadRatchetTree retrieves tree state from database
func (s *MLSSessionStore) LoadRatchetTree(ctx context.Context, groupID string) (*MLSRatchetTree, error) {
	query := `SELECT group_id, generation, tree_bytes, epoch FROM group_ratchet_trees WHERE group_id = $1`
	var tree MLSRatchetTree
	err := s.db.QueryRow(ctx, query, groupID).Scan(&tree.GroupID, &tree.Generation, &tree.TreeBytes, &tree.Epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	tree.Leaves = make(map[int]TreeLeaf)
	return &tree, nil
}

// SaveMemberLeaves stores member-to-leaf mappings
func (s *MLSSessionStore) SaveMemberLeaves(ctx context.Context, groupID string, leaves []TreeLeaf, generation int64) error {
	const query = `INSERT INTO group_member_tree_leaves (group_id, user_id, device_id, leaf_index, generation)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`
	for _, leaf := range leaves {
		if _, err := s.db.Exec(ctx, query, groupID, leaf.UserID, leaf.DeviceID, leaf.Index, generation); err != nil {
			return err
		}
	}
	return nil
}

// LoadMemberLeaves retrieves member-to-leaf mappings for current generation
func (s *MLSSessionStore) LoadMemberLeaves(ctx context.Context, groupID string) ([]TreeLeaf, error) {
	query := `SELECT leaf_index, user_id, device_id FROM group_member_tree_leaves
		WHERE group_id = $1 ORDER BY leaf_index ASC`
	rows, err := s.db.Query(ctx, query, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var leaves []TreeLeaf
	for rows.Next() {
		var idx int
		var userID, deviceID string
		if err := rows.Scan(&idx, &userID, &deviceID); err != nil {
			return nil, err
		}
		leaves = append(leaves, TreeLeaf{Index: idx, UserID: userID, DeviceID: deviceID})
	}
	return leaves, rows.Err()
}

// SaveGroupEpoch stores group secret for an epoch
func (s *MLSSessionStore) SaveGroupEpoch(ctx context.Context, groupID string, epoch int64, secret []byte) error {
	query := `INSERT INTO group_epochs (group_id, epoch, group_secret)
		VALUES ($1, $2, $3)
		ON CONFLICT (group_id, epoch) DO UPDATE
		SET group_secret = EXCLUDED.group_secret`
	_, err := s.db.Exec(ctx, query, groupID, epoch, secret)
	return err
}

// GetGroupEpoch retrieves group secret for an epoch
func (s *MLSSessionStore) GetGroupEpoch(ctx context.Context, groupID string, epoch int64) ([]byte, error) {
	query := `SELECT group_secret FROM group_epochs WHERE group_id = $1 AND epoch = $2`
	var secret []byte
	err := s.db.QueryRow(ctx, query, groupID, epoch).Scan(&secret)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("epoch not found: %d", epoch)
		}
		return nil, err
	}
	return secret, nil
}

// SaveGroupSession stores per-device group session for an epoch
func (s *MLSSessionStore) SaveGroupSession(ctx context.Context, groupID, userID, deviceID string, epoch int64, sessionBytes []byte) error {
	query := `INSERT INTO group_sessions (group_id, user_id, device_id, epoch, session_key_bytes)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (group_id, user_id, device_id, epoch) DO UPDATE
		SET session_key_bytes = EXCLUDED.session_key_bytes,
		    updated_at = NOW()`
	_, err := s.db.Exec(ctx, query, groupID, userID, deviceID, epoch, sessionBytes)
	return err
}

// GetGroupSession retrieves per-device group session
func (s *MLSSessionStore) GetGroupSession(ctx context.Context, groupID, userID, deviceID string) ([]byte, int64, error) {
	query := `SELECT session_key_bytes, epoch FROM group_sessions
		WHERE group_id = $1 AND user_id = $2 AND device_id = $3 ORDER BY epoch DESC LIMIT 1`
	var sessionBytes []byte
	var epoch int64
	err := s.db.QueryRow(ctx, query, groupID, userID, deviceID).Scan(&sessionBytes, &epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, fmt.Errorf("group session not found")
		}
		return nil, 0, err
	}
	return sessionBytes, epoch, nil
}

// RecordMembershipChange logs when members are added/removed
func (s *MLSSessionStore) RecordMembershipChange(ctx context.Context, groupID, initiatorID, targetID string, changeType string, epoch int64) error {
	query := `INSERT INTO group_membership_changes (group_id, initiator_user_id, target_user_id, change_type, epoch)
		VALUES ($1, $2, $3, $4, $5)`
	_, err := s.db.Exec(ctx, query, groupID, initiatorID, targetID, changeType, epoch)
	return err
}
