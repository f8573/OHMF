package pool

import (
	"errors"
	"fmt"
	"math"
	"slices"
)

func DefaultEightFootTableSpec() TableSpec {
	length := 2.2352
	width := 1.1176
	ballRadius := 0.028575
	headStringX := length * 0.25
	headSpot := Vec2{X: headStringX / 2, Y: width / 2}
	footSpot := Vec2{X: length * 0.75, Y: width / 2}
	cornerRadius := 0.067
	sideRadius := 0.073
	return TableSpec{
		Name:             "8ft WPA table",
		LengthMeters:     length,
		WidthMeters:      width,
		BallRadiusMeters: ballRadius,
		HeadStringX:      headStringX,
		HeadSpot:         headSpot,
		FootSpot:         footSpot,
		PlacementHeadZone: Rect{
			MinX: ballRadius,
			MaxX: headStringX - ballRadius,
			MinY: ballRadius,
			MaxY: width - ballRadius,
		},
		Pockets: []PocketSpec{
			{ID: "top_left", Position: Vec2{X: 0, Y: 0}, Radius: cornerRadius},
			{ID: "top_center", Position: Vec2{X: length / 2, Y: 0}, Radius: sideRadius},
			{ID: "top_right", Position: Vec2{X: length, Y: 0}, Radius: cornerRadius},
			{ID: "bottom_left", Position: Vec2{X: 0, Y: width}, Radius: cornerRadius},
			{ID: "bottom_center", Position: Vec2{X: length / 2, Y: width}, Radius: sideRadius},
			{ID: "bottom_right", Position: Vec2{X: length, Y: width}, Radius: cornerRadius},
		},
	}
}

func NewMatchState(setup MatchSetup, seed uint64) (MatchState, error) {
	setup = NormalizeMatchSetup(setup)
	if err := setup.Profile.Validate(); err != nil {
		return MatchState{}, err
	}
	if len(setup.Seats) != 2 {
		return MatchState{}, errors.New("phase 1 pool engine supports exactly 2 seats")
	}
	if setup.Profile.Mode != GameModeHeadToHead {
		return MatchState{}, fmt.Errorf("phase 1 pool engine does not yet start mode %q", setup.Profile.Mode)
	}
	if err := validateSeats(setup.Seats); err != nil {
		return MatchState{}, err
	}
	table := DefaultEightFootTableSpec()
	balls := initialRack(setup.Profile.Game, table)
	seats := make([]SeatState, len(setup.Seats))
	for i, seat := range setup.Seats {
		seats[i] = SeatState{
			SeatSpec:    seat,
			Group:       BallGroupNone,
			Connected:   true,
			Eliminated:  false,
			FoulCount:   0,
			PocketedCount: 0,
		}
	}
	state := MatchState{
		Profile:             setup.Profile,
		Table:               table,
		Tooling:             setup.Tooling,
		Phase:               PhaseBallInHand,
		Tick:                0,
		RNGSeed:             seed,
		BreakSeatIndex:      0,
		ActiveSeatIndex:     0,
		TurnNumber:          1,
		LegalGroupsAssigned: false,
		PlacementConstraint: PlacementConstraint{Region: table.PlacementHeadZone},
		Seats:               seats,
		Balls:               balls,
	}
	return state, nil
}

func validateSeats(seats []SeatSpec) error {
	seen := map[int]struct{}{}
	for i, seat := range seats {
		if seat.SeatIndex < 0 {
			return fmt.Errorf("seat %d has negative seat index", i)
		}
		if seat.UserID == "" {
			return fmt.Errorf("seat %d missing user_id", i)
		}
		if _, ok := seen[seat.SeatIndex]; ok {
			return fmt.Errorf("duplicate seat index %d", seat.SeatIndex)
		}
		seen[seat.SeatIndex] = struct{}{}
	}
	slices.SortFunc(seats, func(a, b SeatSpec) int {
		return a.SeatIndex - b.SeatIndex
	})
	return nil
}

func initialRack(game GameType, table TableSpec) []BallState {
	balls := []BallState{
		{
			Number:   0,
			Kind:     BallKindCue,
			Group:    BallGroupNone,
			Position: table.HeadSpot,
		},
	}
	triangleSpacing := table.BallRadiusMeters * 2.08
	rowHeight := table.BallRadiusMeters * math.Sqrt(3)
	switch game {
	case GameTypeNineBall:
		layout := []int{1, 2, 3, 9, 4, 5, 6, 7, 8}
		for i, number := range layout {
			row := rackRow(i)
			col := rackCol(i)
			position := rackPosition(table.FootSpot, row, col, triangleSpacing, rowHeight)
			balls = append(balls, BallState{
				Number:   number,
				Kind:     BallKindObject,
				Group:    groupForBall(number, game),
				Position: position,
			})
		}
	default:
		layout := []int{1, 9, 2, 10, 8, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15}
		for i, number := range layout {
			row := rackRow(i)
			col := rackCol(i)
			position := rackPosition(table.FootSpot, row, col, triangleSpacing, rowHeight)
			balls = append(balls, BallState{
				Number:   number,
				Kind:     BallKindObject,
				Group:    groupForBall(number, game),
				Position: position,
			})
		}
	}
	return balls
}

func rackRow(index int) int {
	row := 0
	for consumed := 0; ; row++ {
		width := row + 1
		if index < consumed+width {
			return row
		}
		consumed += width
	}
}

func rackCol(index int) int {
	row := rackRow(index)
	consumed := row * (row + 1) / 2
	return index - consumed
}

func rackPosition(apex Vec2, row, col int, dx, dy float64) Vec2 {
	return Vec2{
		X: apex.X + float64(row)*dx,
		Y: apex.Y + (float64(col)-float64(row)/2)*dy,
	}
}

func groupForBall(number int, game GameType) BallGroup {
	if number == 0 {
		return BallGroupNone
	}
	if game == GameTypeNineBall {
		if number == 9 {
			return BallGroupNine
		}
		return BallGroupNone
	}
	switch {
	case number == 8:
		return BallGroupEight
	case number >= 1 && number <= 7:
		return BallGroupSolids
	case number >= 9 && number <= 15:
		return BallGroupStripes
	default:
		return BallGroupNone
	}
}
