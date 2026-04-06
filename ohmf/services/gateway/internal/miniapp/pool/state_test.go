package pool

import "testing"

func TestNewMatchStateBuildsTwoPlayerPreRack(t *testing.T) {
	state, err := NewMatchState(MatchSetup{
		Profile: DefaultRuleProfile(GameTypeEightBall),
		Seats: []SeatSpec{
			{SeatIndex: 0, UserID: "u1", DisplayName: "Avery", PlayerKind: PlayerKindHuman},
			{SeatIndex: 1, UserID: "u2", DisplayName: "Jordan", PlayerKind: PlayerKindBot},
		},
	}, 42)
	if err != nil {
		t.Fatalf("NewMatchState() error = %v", err)
	}
	if state.Phase != PhaseBallInHand {
		t.Fatalf("phase = %q", state.Phase)
	}
	if state.ActiveSeatIndex != 0 || state.BreakSeatIndex != 0 {
		t.Fatalf("active=%d break=%d", state.ActiveSeatIndex, state.BreakSeatIndex)
	}
	if len(state.Balls) != 16 {
		t.Fatalf("ball count = %d", len(state.Balls))
	}
	if state.PlacementConstraint.Region.MaxX > state.Table.HeadStringX {
		t.Fatalf("placement zone crosses head string")
	}
	if state.Balls[0].Position.X > state.Table.HeadStringX {
		t.Fatalf("cue ball starts beyond head string")
	}
	foundEight := false
	for _, ball := range state.Balls {
		if ball.Number == 8 {
			foundEight = true
			if ball.Group != BallGroupEight {
				t.Fatalf("8 ball group = %q", ball.Group)
			}
		}
	}
	if !foundEight {
		t.Fatal("8 ball missing from rack")
	}
}

func TestNewMatchStateRejectsUnsupportedSeatCount(t *testing.T) {
	_, err := NewMatchState(MatchSetup{
		Profile: DefaultRuleProfile(GameTypeEightBall),
		Seats: []SeatSpec{
			{SeatIndex: 0, UserID: "u1", DisplayName: "One"},
			{SeatIndex: 1, UserID: "u2", DisplayName: "Two"},
			{SeatIndex: 2, UserID: "u3", DisplayName: "Three"},
		},
	}, 1)
	if err == nil {
		t.Fatal("expected error for 3-seat phase 1 setup")
	}
}
