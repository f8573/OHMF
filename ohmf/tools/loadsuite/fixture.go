package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func loadOrProvisionFixture(ctx context.Context, cfg config, events *jsonlWriter, targetDevices int) (*fixture, error) {
	if targetDevices <= 0 {
		targetDevices = cfg.initialDevices
	}
	if err := os.MkdirAll(cfg.fixtureDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(cfg.fixtureDir, "fixture.json")
	if existing, err := loadFixture(path); err == nil && fixtureDeviceCount(existing) >= targetDevices && len(existing.Conversations) > 0 {
		return existing, nil
	}
	built, err := buildFixture(ctx, cfg, events, targetDevices)
	if err != nil {
		return nil, err
	}
	if err := saveFixture(path, built); err != nil {
		return nil, err
	}
	return built, nil
}

func loadFixture(path string) (*fixture, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fx fixture
	if err := json.Unmarshal(body, &fx); err != nil {
		return nil, err
	}
	return &fx, nil
}

func saveFixture(path string, fx *fixture) error {
	body, err := json.MarshalIndent(fx, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func buildFixture(ctx context.Context, cfg config, events *jsonlWriter, targetDevices int) (*fixture, error) {
	httpClient := &http.Client{Timeout: 15 * time.Second}
	usersTarget, pairedUsers := populationShape(targetDevices, 0)
	users := make([]*loadUser, usersTarget)

	if err := runWorkerPool(ctx, usersTarget, 16, func(i int) error {
		primary, err := createVerifiedDevice(ctx, httpClient, cfg, "provision", i, "primary", events)
		if err != nil {
			return err
		}
		users[i] = &loadUser{
			UserID:    primary.userID,
			PhoneE164: primary.phoneE164,
			Devices:   []*deviceClient{primary},
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := runWorkerPool(ctx, pairedUsers, 8, func(i int) error {
		linked, err := createPairedDevice(ctx, httpClient, cfg, users[i].Devices[0], "provision", i, events)
		if err != nil {
			return err
		}
		users[i].Devices = append(users[i].Devices, linked)
		return nil
	}); err != nil {
		return nil, err
	}

	groups := partitionConversationGroups(users, 4)
	conversations := make([]*conversationFixture, len(groups))
	if err := runWorkerPool(ctx, len(groups), 8, func(i int) error {
		conv, err := createConversationFixture(ctx, httpClient, cfg, groups[i], i)
		if err != nil {
			return err
		}
		conversations[i] = conv
		return nil
	}); err != nil {
		return nil, err
	}

	fx := &fixture{
		CreatedAt:     time.Now().UTC(),
		TargetDevices: targetDevices,
		Users:         make([]fixtureUser, 0, len(users)),
		Conversations: make([]fixtureConversation, 0, len(conversations)),
	}
	for _, user := range users {
		item := fixtureUser{
			UserID:    user.UserID,
			PhoneE164: user.PhoneE164,
			Devices:   make([]fixtureDevice, 0, len(user.Devices)),
		}
		for _, device := range user.Devices {
			item.Devices = append(item.Devices, fixtureDevice{
				UserID:       device.userID,
				DeviceID:     device.deviceID,
				PhoneE164:    device.phoneE164,
				AccessToken:  device.accessToken,
				RefreshToken: device.refreshToken,
			})
		}
		fx.Users = append(fx.Users, item)
	}
	for _, conv := range conversations {
		item := fixtureConversation{
			ID:                 conv.ID,
			ParticipantUserIDs: make([]string, 0, len(conv.Participants)),
		}
		for _, participant := range conv.Participants {
			item.ParticipantUserIDs = append(item.ParticipantUserIDs, participant.UserID)
		}
		fx.Conversations = append(fx.Conversations, item)
	}
	return fx, nil
}

func createConversationFixture(ctx context.Context, client *http.Client, cfg config, group []*loadUser, index int) (*conversationFixture, error) {
	participantIDs := make([]string, 0, len(group)-1)
	for i := 1; i < len(group); i++ {
		participantIDs = append(participantIDs, group[i].UserID)
	}
	baseURL := strings.TrimRight(cfg.baseURL, "/")
	created, err := postJSONWithRetry(ctx, client, baseURL+"/v1/conversations", group[0].Devices[0].accessToken, map[string]any{
		"type":         "GROUP",
		"participants": participantIDs,
		"title":        fmt.Sprintf("Load Conversation %d", index+1),
	})
	if err != nil {
		return nil, err
	}
	conversationID := textField(created["conversation_id"])
	if _, err := patchJSONRequest(ctx, client, baseURL+"/v1/conversations/"+conversationID+"/metadata", group[0].Devices[0].accessToken, map[string]any{
		"encryption_state": "PLAINTEXT",
	}); err != nil {
		return nil, err
	}
	fixture := &conversationFixture{ID: conversationID, Participants: group}
	for _, participant := range group {
		fixture.SenderDevices = append(fixture.SenderDevices, participant.Devices...)
		for _, device := range participant.Devices {
			fixture.AllDeviceIDs = append(fixture.AllDeviceIDs, device.deviceID)
		}
	}
	return fixture, nil
}

func populationFromFixture(fx *fixture, targetDevices int, baseURL string, profile string) (*population, error) {
	indexes, actualDevices := selectConversationIndexesByDeviceTarget(fx, targetDevices)
	if len(indexes) == 0 {
		return nil, fmt.Errorf("no fixture conversations available for target %d", targetDevices)
	}
	selectedUsers := make(map[string]*loadUser)
	selected := &population{
		users:         make([]*loadUser, 0),
		devices:       make([]*deviceClient, 0, actualDevices),
		conversations: make([]*conversationFixture, 0, len(indexes)),
	}
	userSource := make(map[string]fixtureUser, len(fx.Users))
	for _, user := range fx.Users {
		userSource[user.UserID] = user
	}
	for _, idx := range indexes {
		stored := fx.Conversations[idx]
		conv := &conversationFixture{ID: stored.ID}
		for _, userID := range stored.ParticipantUserIDs {
			user := selectedUsers[userID]
			if user == nil {
				source, ok := userSource[userID]
				if !ok {
					return nil, fmt.Errorf("fixture missing user %s", userID)
				}
				user = &loadUser{
					UserID:    source.UserID,
					PhoneE164: source.PhoneE164,
					Devices:   make([]*deviceClient, 0, len(source.Devices)),
				}
				for _, device := range source.Devices {
					client := &deviceClient{
						userID:       source.UserID,
						deviceID:     device.DeviceID,
						phoneE164:    source.PhoneE164,
						accessToken:  device.AccessToken,
						refreshToken: device.RefreshToken,
						baseURL:      baseURL,
						httpClient:   &http.Client{Timeout: 15 * time.Second},
						phase:        profile,
					}
					user.Devices = append(user.Devices, client)
					selected.devices = append(selected.devices, client)
				}
				selectedUsers[userID] = user
				selected.users = append(selected.users, user)
			}
			conv.Participants = append(conv.Participants, user)
			conv.SenderDevices = append(conv.SenderDevices, user.Devices...)
			for _, device := range user.Devices {
				conv.AllDeviceIDs = append(conv.AllDeviceIDs, device.deviceID)
			}
		}
		selected.conversations = append(selected.conversations, conv)
	}
	return selected, nil
}

func persistPopulationFixtureTokens(cfg config, fx *fixture, pop *population) error {
	if fx == nil || pop == nil || strings.TrimSpace(cfg.fixtureDir) == "" {
		return nil
	}
	deviceIndex := make(map[string]*fixtureDevice)
	for i := range fx.Users {
		for j := range fx.Users[i].Devices {
			device := &fx.Users[i].Devices[j]
			deviceIndex[device.DeviceID] = device
		}
	}

	updated := false
	for _, device := range pop.devices {
		stored := deviceIndex[device.deviceID]
		if stored == nil {
			continue
		}
		accessToken := device.accessTokenValue()
		refreshToken := device.refreshTokenValue()
		if stored.AccessToken == accessToken && stored.RefreshToken == refreshToken {
			continue
		}
		stored.AccessToken = accessToken
		stored.RefreshToken = refreshToken
		updated = true
	}
	if !updated {
		return nil
	}
	return saveFixture(filepath.Join(cfg.fixtureDir, "fixture.json"), fx)
}

func selectConversationIndexesByDeviceTarget(fx *fixture, targetDevices int) ([]int, int) {
	if fx == nil || len(fx.Conversations) == 0 {
		return nil, 0
	}
	deviceCountByUser := make(map[string]int, len(fx.Users))
	totalAvailable := 0
	weights := make([]int, len(fx.Conversations))
	for _, user := range fx.Users {
		deviceCountByUser[user.UserID] = len(user.Devices)
	}
	for i, conv := range fx.Conversations {
		for _, userID := range conv.ParticipantUserIDs {
			weights[i] += deviceCountByUser[userID]
		}
		totalAvailable += weights[i]
	}
	type state struct {
		prevSum int
		idx     int
		valid   bool
	}
	reachable := make([]bool, totalAvailable+1)
	parent := make([]state, totalAvailable+1)
	reachable[0] = true
	for idx, weight := range weights {
		for sum := totalAvailable - weight; sum >= 0; sum-- {
			if !reachable[sum] || reachable[sum+weight] {
				continue
			}
			reachable[sum+weight] = true
			parent[sum+weight] = state{prevSum: sum, idx: idx, valid: true}
		}
	}

	bestSum := 0
	bestDiff := totalAvailable + targetDevices
	for sum, ok := range reachable {
		if !ok || sum == 0 {
			continue
		}
		diff := absInt(sum - targetDevices)
		if diff < bestDiff || (diff == bestDiff && sum >= bestSum) {
			bestDiff = diff
			bestSum = sum
		}
	}
	if bestSum == 0 {
		return nil, 0
	}
	indexes := make([]int, 0)
	for sum := bestSum; sum > 0; {
		item := parent[sum]
		if !item.valid {
			break
		}
		indexes = append(indexes, item.idx)
		sum = item.prevSum
	}
	reverseInts(indexes)
	return indexes, bestSum
}

func fixtureDeviceCount(fx *fixture) int {
	total := 0
	for _, user := range fx.Users {
		total += len(user.Devices)
	}
	return total
}

func populationShape(targetDevices int, fixedUsers int) (int, int) {
	usersTarget := fixedUsers
	if usersTarget <= 0 {
		pairedDevices := int(mathRound(float64(targetDevices) * (0.3 / 1.3)))
		if pairedDevices < 0 {
			pairedDevices = 0
		}
		usersTarget = targetDevices - pairedDevices
	}
	if usersTarget < 2 {
		usersTarget = 2
	}
	pairedUsers := targetDevices - usersTarget
	if pairedUsers < 0 {
		pairedUsers = 0
	}
	if pairedUsers > usersTarget {
		pairedUsers = usersTarget
	}
	return usersTarget, pairedUsers
}

func runWorkerPool(ctx context.Context, total int, workers int, fn func(int) error) error {
	if total <= 0 {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > total {
		workers = total
	}
	indexCh := make(chan int)
	errCh := make(chan error, 1)
	var once sync.Once
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range indexCh {
				if err := fn(idx); err != nil {
					once.Do(func() {
						errCh <- err
					})
					return
				}
			}
		}()
	}
	for idx := 0; idx < total; idx++ {
		select {
		case <-ctx.Done():
			close(indexCh)
			wg.Wait()
			return ctx.Err()
		case err := <-errCh:
			close(indexCh)
			wg.Wait()
			return err
		case indexCh <- idx:
		}
	}
	close(indexCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func reverseInts(values []int) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func mathRound(value float64) int {
	if value < 0 {
		return int(value - 0.5)
	}
	return int(value + 0.5)
}
