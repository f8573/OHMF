package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFixtureSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	original := &fixture{
		CreatedAt:     time.Now().UTC(),
		TargetDevices: 20,
		Users: []fixtureUser{
			{
				UserID:    "user-1",
				PhoneE164: "+15550000001",
				Devices: []fixtureDevice{
					{UserID: "user-1", DeviceID: "device-1", PhoneE164: "+15550000001", AccessToken: "access-1", RefreshToken: "refresh-1"},
				},
			},
		},
		Conversations: []fixtureConversation{
			{ID: "conversation-1", ParticipantUserIDs: []string{"user-1"}},
		},
	}

	if err := saveFixture(path, original); err != nil {
		t.Fatalf("save fixture: %v", err)
	}
	loaded, err := loadFixture(path)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	if loaded.TargetDevices != original.TargetDevices {
		t.Fatalf("expected target devices %d, got %d", original.TargetDevices, loaded.TargetDevices)
	}
	if fixtureDeviceCount(loaded) != 1 {
		t.Fatalf("expected one device, got %d", fixtureDeviceCount(loaded))
	}
	if len(loaded.Conversations) != 1 || loaded.Conversations[0].ID != "conversation-1" {
		t.Fatalf("unexpected loaded conversations: %#v", loaded.Conversations)
	}
}

func TestPersistPopulationFixtureTokens(t *testing.T) {
	dir := t.TempDir()
	cfg := config{fixtureDir: dir}
	fx := &fixture{
		Users: []fixtureUser{
			{
				UserID: "user-1",
				Devices: []fixtureDevice{
					{UserID: "user-1", DeviceID: "device-1", AccessToken: "access-old", RefreshToken: "refresh-old"},
				},
			},
		},
	}
	if err := saveFixture(filepath.Join(dir, "fixture.json"), fx); err != nil {
		t.Fatalf("seed fixture: %v", err)
	}

	pop := &population{
		devices: []*deviceClient{
			{userID: "user-1", deviceID: "device-1", accessToken: "access-new", refreshToken: "refresh-new"},
		},
	}
	if err := persistPopulationFixtureTokens(cfg, fx, pop); err != nil {
		t.Fatalf("persist fixture tokens: %v", err)
	}

	loaded, err := loadFixture(filepath.Join(dir, "fixture.json"))
	if err != nil {
		t.Fatalf("load persisted fixture: %v", err)
	}
	device := loaded.Users[0].Devices[0]
	if device.AccessToken != "access-new" || device.RefreshToken != "refresh-new" {
		t.Fatalf("expected persisted tokens to be refreshed, got access=%q refresh=%q", device.AccessToken, device.RefreshToken)
	}
}

func TestSelectConversationIndexesByDeviceTarget(t *testing.T) {
	fx := &fixture{
		Users: []fixtureUser{
			{UserID: "u1", Devices: []fixtureDevice{{DeviceID: "d1"}, {DeviceID: "d2"}}},
			{UserID: "u2", Devices: []fixtureDevice{{DeviceID: "d3"}, {DeviceID: "d4"}}},
			{UserID: "u3", Devices: []fixtureDevice{{DeviceID: "d5"}}},
			{UserID: "u4", Devices: []fixtureDevice{{DeviceID: "d6"}}},
			{UserID: "u5", Devices: []fixtureDevice{{DeviceID: "d7"}}},
			{UserID: "u6", Devices: []fixtureDevice{{DeviceID: "d8"}}},
		},
		Conversations: []fixtureConversation{
			{ID: "c1", ParticipantUserIDs: []string{"u1", "u2"}}, // 4 devices
			{ID: "c2", ParticipantUserIDs: []string{"u3", "u4"}}, // 2 devices
			{ID: "c3", ParticipantUserIDs: []string{"u5", "u6"}}, // 2 devices
		},
	}

	indexes, actual := selectConversationIndexesByDeviceTarget(fx, 6)
	if actual != 6 {
		t.Fatalf("expected exact 6-device selection, got %d", actual)
	}
	if len(indexes) != 2 {
		t.Fatalf("expected two conversations selected, got %d", len(indexes))
	}
}

func TestConnectWaveSizeForTarget(t *testing.T) {
	if got := connectWaveSizeForTarget(0, 250); got != 25 {
		t.Fatalf("expected 25-device waves up to 300, got %d", got)
	}
	if got := connectWaveSizeForTarget(0, 350); got != 15 {
		t.Fatalf("expected 15-device waves above 300, got %d", got)
	}
	if got := connectWaveSizeForTarget(12, 500); got != 12 {
		t.Fatalf("expected explicit wave size override, got %d", got)
	}
}

func TestDefaultPlanForModeUsesProfileConnectThreshold(t *testing.T) {
	plan := defaultPlanForMode(config{
		mode:           "smoke",
		profile:        profileConnectedIdle,
		initialDevices: 20,
		proveDuration:  30 * time.Second,
	})
	if plan.profile != profileConnectedIdle {
		t.Fatalf("expected connected-idle profile, got %q", plan.profile)
	}
	if plan.enableTraffic {
		t.Fatalf("expected connected-idle smoke plan to disable traffic")
	}
	if plan.connectThreshold != 0.99 {
		t.Fatalf("expected connected-idle smoke plan to require 99%% readiness, got %.2f", plan.connectThreshold)
	}
}

func TestClassifyFailureReason(t *testing.T) {
	cases := map[string]string{
		"websocket: bad handshake":                         "bad_handshake",
		"http://localhost returned 429: too many requests": "429_rate_limited",
		"http://localhost returned 401: unauthorized":      "401_unauthorized",
		"read tcp 127.0.0.1: i/o timeout":                  "http_timeout",
		"resume_failed while replaying user events":        "resume_failed",
		"something else entirely":                          "other",
	}
	for input, expected := range cases {
		if got := classifyFailureReason(assertError(input)); got != expected {
			t.Fatalf("expected %q for %q, got %q", expected, input, got)
		}
	}
}

func TestEvaluateScenarioPassAndFirstBottleneck(t *testing.T) {
	result := summary{
		Workload: workloadSummary{
			ConnectReadinessPercent: 100,
		},
		Performance: performanceSummary{
			P95SendToAckLatency:        "2.00s",
			P95EndToEndDeliveryLatency: "2.50s",
		},
		Correctness: correctnessSummary{
			MultiDeviceConvergenceRate: 1,
		},
	}
	if passed, _, _ := evaluateScenarioPass(profileActiveSustain, result); !passed {
		t.Fatalf("expected active scenario to pass")
	}

	rows := []campaignTierSummary{
		{Label: "active-250", Passed: true},
		{Label: "active-350", Passed: false, FirstFailureReason: "bad_handshake"},
	}
	if got := deriveFirstBottleneck(rows); got != "websocket/bootstrap API saturation during connection and recovery ramps" {
		t.Fatalf("unexpected bottleneck text: %q", got)
	}
}

func assertError(message string) error {
	return testError(message)
}

type testError string

func (e testError) Error() string {
	return string(e)
}
