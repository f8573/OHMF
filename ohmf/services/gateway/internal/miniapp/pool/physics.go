package pool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

func DefaultDeterministicConfig() DeterministicConfig {
	return DeterministicConfig{
		TickRateHz:             120,
		MaxCueSpeedMetersSec:   8.5,
		LinearDampingPerStep:   0.991,
		SpinDampingPerStep:     0.986,
		RailRestitution:        0.92,
		VelocitySleepMetersSec: 0.02,
		ReconnectTimeout:       15 * time.Second,
	}
}

func ClampShotInput(input ShotInput) ShotInput {
	clamped := input
	clamped.Power = clamp(input.Power, 0, 1)
	clamped.AngleRadians = math.Mod(input.AngleRadians, math.Pi*2)
	if clamped.AngleRadians < 0 {
		clamped.AngleRadians += math.Pi * 2
	}
	clamped.CueOffset = clampCueOffset(input.CueOffset)
	clamped.DeclaredPocketID = strings.TrimSpace(input.DeclaredPocketID)
	clamped.DeclaredShotType = strings.TrimSpace(input.DeclaredShotType)
	if clamped.DeclaredTargetBall < 0 {
		clamped.DeclaredTargetBall = 0
	}
	return clamped
}

func ApplyShot(state MatchState, input ShotInput, cfg DeterministicConfig) (MatchState, []RefereeEvent, error) {
	if state.Phase != PhaseBallInHand && state.Phase != PhaseAim {
		return state, nil, errors.New("shot can only be applied during ball_in_hand or aim")
	}
	if input.SeatIndex != state.ActiveSeatIndex {
		return state, nil, errors.New("only the active seat may shoot")
	}
	input = ClampShotInput(input)
	cueIndex := findBallIndex(state.Balls, 0)
	if cueIndex < 0 {
		return state, nil, errors.New("cue ball not found")
	}
	next := cloneState(state)
	speed := cfg.MaxCueSpeedMetersSec * input.Power
	next.Balls[cueIndex].Velocity = Vec2{
		X: math.Cos(input.AngleRadians) * speed,
		Y: math.Sin(input.AngleRadians) * speed,
	}
	next.Balls[cueIndex].AngularVelocity = Vec3{
		X: input.CueOffset.Y * input.Power * 18,
		Y: -input.CueOffset.X * input.Power * 18,
		Z: input.CueOffset.X * input.Power * 9,
	}
	next.PendingShot = &PendingShot{
		Input:            input,
		Seed:             next.RNGSeed,
		StartedAtUnixMS:  input.ClientUnixMillis,
		StartTick:        next.Tick,
		StartStateDigest: StateDigest(state),
	}
	next.Phase = PhaseBallsInMotion
	replay := ShotReplayRecord{
		Game:             next.Profile.Game,
		Mode:             next.Profile.Mode,
		Sequence:         input.Sequence,
		SeatIndex:        input.SeatIndex,
		Seed:             next.RNGSeed,
		StartedAtUnixMS:  input.ClientUnixMillis,
		StartTick:        next.Tick,
		StartStateDigest: StateDigest(state),
		Input:            input,
	}
	next.LastReplay = &replay
	events := []RefereeEvent{
		{Type: EventTypeShotAccepted, AtTick: next.Tick, ActorSeat: input.SeatIndex},
		{Type: EventTypeMotionStarted, AtTick: next.Tick, ActorSeat: input.SeatIndex},
	}
	return next, events, nil
}

func StepMotion(state MatchState, cfg DeterministicConfig, ticks int) (MatchState, []RefereeEvent, error) {
	if ticks < 1 {
		return state, nil, nil
	}
	next := cloneState(state)
	events := make([]RefereeEvent, 0, 1)
	for i := 0; i < ticks; i++ {
		next.Tick++
		for ballIndex := range next.Balls {
			if next.Balls[ballIndex].Pocketed {
				continue
			}
			advanceBall(&next.Balls[ballIndex], next.Table, cfg)
		}
		if allBallsSleeping(next.Balls, cfg.VelocitySleepMetersSec) {
			zeroAllVelocities(next.Balls)
			next.Phase = PhaseFoulResolution
			if next.LastReplay != nil {
				next.LastReplay.SettledAtTick = next.Tick
				next.LastReplay.SettledStateDigest = StateDigest(next)
				next.LastReplay.Events = append(slicesCloneEvents(next.LastReplay.Events), RefereeEvent{
					Type:      EventTypeMotionSettled,
					AtTick:    next.Tick,
					ActorSeat: next.ActiveSeatIndex,
				})
			}
			events = append(events, RefereeEvent{
				Type:      EventTypeMotionSettled,
				AtTick:    next.Tick,
				ActorSeat: next.ActiveSeatIndex,
			})
			return next, events, nil
		}
	}
	return next, events, nil
}

func StateDigest(state MatchState) string {
	payload, _ := json.Marshal(state)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func cloneState(state MatchState) MatchState {
	payload, _ := json.Marshal(state)
	var cloned MatchState
	_ = json.Unmarshal(payload, &cloned)
	return cloned
}

func advanceBall(ball *BallState, table TableSpec, cfg DeterministicConfig) {
	ball.Position.X += ball.Velocity.X / float64(cfg.TickRateHz)
	ball.Position.Y += ball.Velocity.Y / float64(cfg.TickRateHz)
	ball.Velocity.X *= cfg.LinearDampingPerStep
	ball.Velocity.Y *= cfg.LinearDampingPerStep
	ball.AngularVelocity.X *= cfg.SpinDampingPerStep
	ball.AngularVelocity.Y *= cfg.SpinDampingPerStep
	ball.AngularVelocity.Z *= cfg.SpinDampingPerStep

	minX := table.BallRadiusMeters
	maxX := table.LengthMeters - table.BallRadiusMeters
	minY := table.BallRadiusMeters
	maxY := table.WidthMeters - table.BallRadiusMeters

	if ball.Position.X < minX {
		ball.Position.X = minX
		ball.Velocity.X = math.Abs(ball.Velocity.X) * cfg.RailRestitution
	}
	if ball.Position.X > maxX {
		ball.Position.X = maxX
		ball.Velocity.X = -math.Abs(ball.Velocity.X) * cfg.RailRestitution
	}
	if ball.Position.Y < minY {
		ball.Position.Y = minY
		ball.Velocity.Y = math.Abs(ball.Velocity.Y) * cfg.RailRestitution
	}
	if ball.Position.Y > maxY {
		ball.Position.Y = maxY
		ball.Velocity.Y = -math.Abs(ball.Velocity.Y) * cfg.RailRestitution
	}
}

func allBallsSleeping(balls []BallState, threshold float64) bool {
	for _, ball := range balls {
		if ball.Pocketed {
			continue
		}
		speed := math.Hypot(ball.Velocity.X, ball.Velocity.Y)
		if speed >= threshold {
			return false
		}
	}
	return true
}

func zeroAllVelocities(balls []BallState) {
	for index := range balls {
		balls[index].Velocity = Vec2{}
		balls[index].AngularVelocity = Vec3{}
	}
}

func findBallIndex(balls []BallState, number int) int {
	for index, ball := range balls {
		if ball.Number == number {
			return index
		}
	}
	return -1
}

func clamp(value, min, max float64) float64 {
	switch {
	case value < min:
		return min
	case value > max:
		return max
	default:
		return value
	}
}

func clampCueOffset(offset Vec2) Vec2 {
	length := math.Hypot(offset.X, offset.Y)
	if length <= 1 || length == 0 {
		return offset
	}
	return Vec2{
		X: offset.X / length,
		Y: offset.Y / length,
	}
}

func slicesCloneEvents(values []RefereeEvent) []RefereeEvent {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]RefereeEvent, len(values))
	copy(cloned, values)
	return cloned
}
