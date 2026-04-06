package pool

import "testing"

func TestClampShotInput(t *testing.T) {
	input := ClampShotInput(ShotInput{
		AngleRadians:     -1,
		Power:            4,
		CueOffset:        Vec2{X: 3, Y: 4},
		DeclaredPocketID: "  bottom_right  ",
	})
	if input.Power != 1 {
		t.Fatalf("power = %v", input.Power)
	}
	if input.AngleRadians < 0 {
		t.Fatalf("angle should be normalized, got %v", input.AngleRadians)
	}
	if input.CueOffset.X < 0.59 || input.CueOffset.X > 0.61 {
		t.Fatalf("cue offset x = %v", input.CueOffset.X)
	}
	if input.CueOffset.Y < 0.79 || input.CueOffset.Y > 0.81 {
		t.Fatalf("cue offset y = %v", input.CueOffset.Y)
	}
	if input.DeclaredPocketID != "bottom_right" {
		t.Fatalf("declared pocket = %q", input.DeclaredPocketID)
	}
}

func TestApplyShotStartsMotionAndReplay(t *testing.T) {
	state, err := NewMatchState(MatchSetup{
		Profile: DefaultRuleProfile(GameTypeEightBall),
		Seats: []SeatSpec{
			{SeatIndex: 0, UserID: "u1", DisplayName: "Avery"},
			{SeatIndex: 1, UserID: "u2", DisplayName: "Jordan"},
		},
	}, 99)
	if err != nil {
		t.Fatalf("NewMatchState() error = %v", err)
	}
	cfg := DefaultDeterministicConfig()
	next, events, err := ApplyShot(state, ShotInput{
		SeatIndex:        0,
		Sequence:         1,
		ClientUnixMillis: 1234,
		AngleRadians:     0.5,
		Power:            0.75,
		CueOffset:        Vec2{X: 0.2, Y: -0.1},
	}, cfg)
	if err != nil {
		t.Fatalf("ApplyShot() error = %v", err)
	}
	if next.Phase != PhaseBallsInMotion {
		t.Fatalf("phase = %q", next.Phase)
	}
	if next.PendingShot == nil || next.LastReplay == nil {
		t.Fatal("pending shot and replay must be set")
	}
	if len(events) != 2 {
		t.Fatalf("event count = %d", len(events))
	}
	cue := next.Balls[findBallIndex(next.Balls, 0)]
	if cue.Velocity.X == 0 && cue.Velocity.Y == 0 {
		t.Fatal("cue ball velocity should be non-zero")
	}
	if cue.AngularVelocity.X == 0 && cue.AngularVelocity.Y == 0 && cue.AngularVelocity.Z == 0 {
		t.Fatal("cue ball spin should be non-zero")
	}
}

func TestStepMotionIsDeterministicAcrossRoundTrip(t *testing.T) {
	setup := MatchSetup{
		Profile: DefaultRuleProfile(GameTypeEightBall),
		Seats: []SeatSpec{
			{SeatIndex: 0, UserID: "u1", DisplayName: "Avery"},
			{SeatIndex: 1, UserID: "u2", DisplayName: "Jordan"},
		},
	}
	state, err := NewMatchState(setup, 123)
	if err != nil {
		t.Fatalf("NewMatchState() error = %v", err)
	}
	cfg := DefaultDeterministicConfig()
	shot := ShotInput{
		SeatIndex:        0,
		Sequence:         7,
		ClientUnixMillis: 777,
		AngleRadians:     1.2,
		Power:            0.6,
		CueOffset:        Vec2{X: 0.1, Y: 0.1},
	}
	first, _, err := ApplyShot(state, shot, cfg)
	if err != nil {
		t.Fatalf("ApplyShot() error = %v", err)
	}
	second, _, err := ApplyShot(state, shot, cfg)
	if err != nil {
		t.Fatalf("ApplyShot() error = %v", err)
	}
	first, _, err = StepMotion(first, cfg, 240)
	if err != nil {
		t.Fatalf("StepMotion() error = %v", err)
	}
	second, _, err = StepMotion(second, cfg, 240)
	if err != nil {
		t.Fatalf("StepMotion() error = %v", err)
	}
	if got, want := StateDigest(first), StateDigest(second); got != want {
		t.Fatalf("deterministic digest mismatch\ngot  %s\nwant %s", got, want)
	}
}
