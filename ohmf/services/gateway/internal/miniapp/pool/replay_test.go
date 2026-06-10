package pool

import "testing"

func TestReplayLogRoundTrip(t *testing.T) {
	record := ShotReplayRecord{
		Game:              GameTypeEightBall,
		Mode:              GameModeHeadToHead,
		Sequence:          3,
		SeatIndex:         1,
		Seed:              55,
		StartedAtUnixMS:   999,
		StartTick:         12,
		StartStateDigest:  "abc",
		SettledStateDigest: "def",
		SettledAtTick:     24,
		Input: ShotInput{
			SeatIndex:         1,
			Sequence:          3,
			ClientUnixMillis:  999,
			DeclaredPocketID:  "bottom_left",
			DeclaredTargetBall: 8,
		},
		Events: []RefereeEvent{
			{Type: EventTypeShotAccepted, AtTick: 12, ActorSeat: 1},
		},
	}
	log := AppendReplay(ReplayLog{}, record)
	data, err := ReplayLogJSON(log)
	if err != nil {
		t.Fatalf("ReplayLogJSON() error = %v", err)
	}
	parsed, err := ParseReplayLog(data)
	if err != nil {
		t.Fatalf("ParseReplayLog() error = %v", err)
	}
	if len(parsed.Records) != 1 {
		t.Fatalf("record count = %d", len(parsed.Records))
	}
	if parsed.Records[0].Input.DeclaredTargetBall != 8 {
		t.Fatalf("declared target ball = %d", parsed.Records[0].Input.DeclaredTargetBall)
	}
}
