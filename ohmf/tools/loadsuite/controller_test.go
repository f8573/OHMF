package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildControllerAssignmentsSkipsZeroShareAgents(t *testing.T) {
	fx := &fixture{
		Users: []fixtureUser{
			{
				UserID: "u1",
				Devices: []fixtureDevice{
					{UserID: "u1", DeviceID: "d1"},
				},
			},
			{
				UserID: "u2",
				Devices: []fixtureDevice{
					{UserID: "u2", DeviceID: "d2"},
				},
			},
			{
				UserID: "u3",
				Devices: []fixtureDevice{
					{UserID: "u3", DeviceID: "d3"},
				},
			},
			{
				UserID: "u4",
				Devices: []fixtureDevice{
					{UserID: "u4", DeviceID: "d4"},
				},
			},
		},
		Conversations: []fixtureConversation{
			{ID: "c1", ParticipantUserIDs: []string{"u1", "u2", "u3", "u4"}},
		},
	}

	assignments, _, _, _, err := buildControllerAssignments(config{
		initialDevices: 4,
		hostBaseURL:    "http://host",
		agentShare:     "host=0,laptop-a=0.5,laptop-b=0.5",
	}, fx)
	if err != nil {
		t.Fatalf("build assignments: %v", err)
	}

	host := assignments[hostAgentID]
	if len(host.Users) != 0 {
		t.Fatalf("expected zero-share host to receive no users, got %d", len(host.Users))
	}
	if len(host.Conversations) != 0 {
		t.Fatalf("expected zero-share host to own no conversations, got %d", len(host.Conversations))
	}
}

func TestBuildControllerAssignmentsDefaultsToHost(t *testing.T) {
	fx := &fixture{
		Users: []fixtureUser{
			{
				UserID: "u1",
				Devices: []fixtureDevice{
					{UserID: "u1", DeviceID: "d1"},
				},
			},
			{
				UserID: "u2",
				Devices: []fixtureDevice{
					{UserID: "u2", DeviceID: "d2"},
				},
			},
			{
				UserID: "u3",
				Devices: []fixtureDevice{
					{UserID: "u3", DeviceID: "d3"},
				},
			},
			{
				UserID: "u4",
				Devices: []fixtureDevice{
					{UserID: "u4", DeviceID: "d4"},
				},
			},
		},
		Conversations: []fixtureConversation{
			{ID: "c1", ParticipantUserIDs: []string{"u1", "u2", "u3", "u4"}},
		},
	}

	assignments, _, devices, conversations, err := buildControllerAssignments(config{
		initialDevices: 4,
		hostBaseURL:    "http://host",
	}, fx)
	if err != nil {
		t.Fatalf("build assignments: %v", err)
	}
	if devices != 4 || conversations != 1 {
		t.Fatalf("unexpected selection size: devices=%d conversations=%d", devices, conversations)
	}
	host := assignments[hostAgentID]
	if len(host.Users) == 0 {
		t.Fatalf("expected default controller assignment to target host")
	}
	if len(host.Conversations) == 0 {
		t.Fatalf("expected default controller assignment to include a conversation")
	}
}

func TestAgentEventUploaderFlushRequeuesOnFailure(t *testing.T) {
	fail := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/agents/agent-a/events" {
			http.NotFound(w, r)
			return
		}
		if fail {
			http.Error(w, "retry later", http.StatusServiceUnavailable)
			return
		}
		var payload agentEventsUpload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if len(payload.Events) != 1 {
			t.Fatalf("expected one uploaded event, got %d", len(payload.Events))
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	uploader := &agentEventUploader{
		cfg: config{
			controllerURL: server.URL,
			agentID:       "agent-a",
		},
	}
	uploader.enqueue(rawEvent{Type: "t_send_ack"})

	if err := uploader.flush(context.Background()); err == nil {
		t.Fatalf("expected flush to fail")
	}
	if len(uploader.events) != 1 {
		t.Fatalf("expected failed flush to preserve pending events, got %d", len(uploader.events))
	}

	fail = false
	if err := uploader.flush(context.Background()); err != nil {
		t.Fatalf("expected retry flush to succeed: %v", err)
	}
	if len(uploader.events) != 0 {
		t.Fatalf("expected pending events to drain after retry, got %d", len(uploader.events))
	}
}
