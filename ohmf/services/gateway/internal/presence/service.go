package presence

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

var ErrConversationAccess = errors.New("conversation_access_denied")

type Service struct {
	db    db
	redis *redis.Client
}

type db interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Session struct {
	SessionID    string `json:"session_id"`
	DeviceID     string `json:"device_id,omitempty"`
	Version      string `json:"version,omitempty"`
	LastSeenAtMS int64  `json:"last_seen_at_ms,omitempty"`
}

type UserPresence struct {
	UserID       string    `json:"user_id"`
	Online       bool      `json:"online"`
	LastSeenAt   string    `json:"last_seen_at,omitempty"`
	SessionCount int       `json:"session_count"`
	Sessions     []Session `json:"sessions,omitempty"`
}

func NewService(db db, redisClient *redis.Client) *Service {
	return &Service{db: db, redis: redisClient}
}

func (s *Service) GetUserPresence(ctx context.Context, userID string) (UserPresence, error) {
	result := UserPresence{UserID: userID, Sessions: []Session{}}
	if s.redis == nil {
		return result, nil
	}
	if online, err := s.redis.Exists(ctx, "presence:user:"+userID).Result(); err == nil && online > 0 {
		result.Online = true
	}
	if raw, err := s.redis.Get(ctx, "presence:user:"+userID+":last_seen").Result(); err == nil && raw != "" {
		if ts, err := strconv.ParseInt(raw, 10, 64); err == nil && ts > 0 {
			result.LastSeenAt = time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
		}
	}
	sessionIDs, err := s.redis.SMembers(ctx, "user_sessions:"+userID).Result()
	if err != nil && err != redis.Nil {
		return result, err
	}
	for _, sessionID := range sessionIDs {
		body, err := s.redis.Get(ctx, "session:"+sessionID).Result()
		if err != nil {
			continue
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(body), &decoded); err != nil {
			continue
		}
		item := Session{SessionID: sessionID}
		if deviceID, _ := decoded["device_id"].(string); deviceID != "" {
			item.DeviceID = deviceID
		}
		if version, _ := decoded["version"].(string); version != "" {
			item.Version = version
		}
		if lastSeenFloat, ok := decoded["last_seen_at_ms"].(float64); ok {
			item.LastSeenAtMS = int64(lastSeenFloat)
			if result.LastSeenAt == "" && item.LastSeenAtMS > 0 {
				result.LastSeenAt = time.UnixMilli(item.LastSeenAtMS).UTC().Format(time.RFC3339Nano)
			}
		}
		result.Sessions = append(result.Sessions, item)
	}
	sort.SliceStable(result.Sessions, func(i, j int) bool {
		return result.Sessions[i].LastSeenAtMS > result.Sessions[j].LastSeenAtMS
	})
	result.SessionCount = len(result.Sessions)
	if result.SessionCount > 0 {
		result.Online = true
	}
	return result, nil
}

func (s *Service) GetConversationPresence(ctx context.Context, actorUserID, conversationID string) ([]UserPresence, error) {
	if err := s.ensureMembership(ctx, actorUserID, conversationID); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `
		SELECT user_id::text
		FROM conversation_members
		WHERE conversation_id = $1::uuid
		ORDER BY joined_at ASC
	`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]UserPresence, 0, 4)
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		item, err := s.GetUserPresence(ctx, userID)
		if err != nil {
			return nil, err
		}
		if s.redis != nil {
			if watching, err := s.redis.Exists(ctx, "presence:conv:"+conversationID+":user:"+userID).Result(); err == nil && watching > 0 {
				item.Online = true
			}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) ensureMembership(ctx context.Context, actorUserID, conversationID string) error {
	var exists bool
	if err := s.db.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM conversation_members
			WHERE conversation_id = $1::uuid
			  AND user_id = $2::uuid
		)
	`, conversationID, actorUserID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrConversationAccess
	}
	return nil
}
