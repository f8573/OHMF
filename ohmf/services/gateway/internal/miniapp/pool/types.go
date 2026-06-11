package pool

import "time"

type GameType string

const (
	GameTypeEightBall GameType = "eight_ball"
	GameTypeNineBall  GameType = "nine_ball"
)

type GameMode string

const (
	GameModeHeadToHead GameMode = "head_to_head"
	GameModeTeam       GameMode = "team"
	GameModeRoundRobin GameMode = "round_robin"
)

type CallPocketMode string

const (
	CallPocketNone     CallPocketMode = "none"
	CallPocketEightOnly CallPocketMode = "eight_only"
	CallPocketAllShots CallPocketMode = "all_shots"
)

type Phase string

const (
	PhaseBallInHand    Phase = "ball_in_hand"
	PhaseAim           Phase = "aim"
	PhaseShotInFlight  Phase = "shot_in_flight"
	PhaseBallsInMotion Phase = "balls_in_motion"
	PhaseFoulResolution Phase = "foul_resolution"
	PhaseMatchOver     Phase = "match_over"
)

type PlayerKind string

const (
	PlayerKindHuman PlayerKind = "human"
	PlayerKindBot   PlayerKind = "bot"
)

type BallKind string

const (
	BallKindCue    BallKind = "cue"
	BallKindObject BallKind = "object"
)

type BallGroup string

const (
	BallGroupNone    BallGroup = "none"
	BallGroupSolids  BallGroup = "solids"
	BallGroupStripes BallGroup = "stripes"
	BallGroupEight   BallGroup = "eight"
	BallGroupNine    BallGroup = "nine"
)

type EventType string

const (
	EventTypeShotAccepted EventType = "shot_accepted"
	EventTypeMotionStarted EventType = "motion_started"
	EventTypeMotionSettled EventType = "motion_settled"
)

type Vec2 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Vec3 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type Rect struct {
	MinX float64 `json:"min_x"`
	MaxX float64 `json:"max_x"`
	MinY float64 `json:"min_y"`
	MaxY float64 `json:"max_y"`
}

type PocketSpec struct {
	ID       string `json:"id"`
	Position Vec2   `json:"position"`
	Radius   float64 `json:"radius"`
}

type TableSpec struct {
	Name              string       `json:"name"`
	LengthMeters      float64      `json:"length_meters"`
	WidthMeters       float64      `json:"width_meters"`
	BallRadiusMeters  float64      `json:"ball_radius_meters"`
	HeadStringX       float64      `json:"head_string_x"`
	FootSpot          Vec2         `json:"foot_spot"`
	HeadSpot          Vec2         `json:"head_spot"`
	PlacementHeadZone Rect         `json:"placement_head_zone"`
	Pockets           []PocketSpec `json:"pockets"`
}

type RuleProfile struct {
	Game               GameType       `json:"game"`
	Mode               GameMode       `json:"mode"`
	CallPocketMode     CallPocketMode `json:"call_pocket_mode"`
	FoulLimit          int            `json:"foul_limit"`
	TimerEnabled       bool           `json:"timer_enabled"`
	TurnTimeoutSeconds int            `json:"turn_timeout_seconds"`
	GuidelinesEnabled  bool           `json:"guidelines_enabled"`
	MaxPlayers         int            `json:"max_players"`
	BackendAuthority   bool           `json:"backend_authority"`
	DeterministicReplay bool          `json:"deterministic_replay"`
}

type ToolingFlags struct {
	AdminToolsEnabled bool `json:"admin_tools_enabled"`
}

type SeatSpec struct {
	SeatIndex    int        `json:"seat_index"`
	UserID       string     `json:"user_id"`
	DisplayName  string     `json:"display_name"`
	PlayerKind   PlayerKind `json:"player_kind"`
	TeamIndex    int        `json:"team_index"`
}

type MatchSetup struct {
	Profile RuleProfile  `json:"profile"`
	Seats   []SeatSpec   `json:"seats"`
	Tooling ToolingFlags `json:"tooling"`
}

type SeatState struct {
	SeatSpec
	Group           BallGroup `json:"group"`
	Eliminated      bool      `json:"eliminated"`
	FoulCount       int       `json:"foul_count"`
	PocketedCount   int       `json:"pocketed_count"`
	Connected       bool      `json:"connected"`
	ReconnectByUnix int64     `json:"reconnect_by_unix"`
}

type BallState struct {
	Number          int       `json:"number"`
	Kind            BallKind  `json:"kind"`
	Group           BallGroup `json:"group"`
	Pocketed        bool      `json:"pocketed"`
	Position        Vec2      `json:"position"`
	Velocity        Vec2      `json:"velocity"`
	AngularVelocity Vec3      `json:"angular_velocity"`
}

type PlacementConstraint struct {
	Region Rect `json:"region"`
}

type ShotInput struct {
	SeatIndex         int     `json:"seat_index"`
	Sequence          int64   `json:"sequence"`
	ClientUnixMillis  int64   `json:"client_unix_millis"`
	AngleRadians      float64 `json:"angle_radians"`
	Power             float64 `json:"power"`
	CueOffset         Vec2    `json:"cue_offset"`
	DeclaredPocketID  string  `json:"declared_pocket_id,omitempty"`
	DeclaredTargetBall int    `json:"declared_target_ball,omitempty"`
	DeclaredShotType  string  `json:"declared_shot_type,omitempty"`
}

type PendingShot struct {
	Input            ShotInput `json:"input"`
	Seed             uint64    `json:"seed"`
	StartedAtUnixMS  int64     `json:"started_at_unix_ms"`
	StartTick        uint64    `json:"start_tick"`
	StartStateDigest string    `json:"start_state_digest"`
}

type RefereeEvent struct {
	Type         EventType `json:"type"`
	AtTick       uint64    `json:"at_tick"`
	ActorSeat    int       `json:"actor_seat"`
	Reason       string    `json:"reason,omitempty"`
}

type ShotReplayRecord struct {
	Game              GameType       `json:"game"`
	Mode              GameMode       `json:"mode"`
	Sequence          int64          `json:"sequence"`
	SeatIndex         int            `json:"seat_index"`
	Seed              uint64         `json:"seed"`
	StartedAtUnixMS   int64          `json:"started_at_unix_ms"`
	StartTick         uint64         `json:"start_tick"`
	StartStateDigest  string         `json:"start_state_digest"`
	Input             ShotInput      `json:"input"`
	SettledStateDigest string        `json:"settled_state_digest,omitempty"`
	SettledAtTick     uint64         `json:"settled_at_tick,omitempty"`
	Events            []RefereeEvent `json:"events,omitempty"`
}

type ReplayLog struct {
	Records []ShotReplayRecord `json:"records"`
}

type DeterministicConfig struct {
	TickRateHz            int           `json:"tick_rate_hz"`
	MaxCueSpeedMetersSec  float64       `json:"max_cue_speed_meters_sec"`
	LinearDampingPerStep  float64       `json:"linear_damping_per_step"`
	SpinDampingPerStep    float64       `json:"spin_damping_per_step"`
	RailRestitution       float64       `json:"rail_restitution"`
	VelocitySleepMetersSec float64      `json:"velocity_sleep_meters_sec"`
	ReconnectTimeout      time.Duration `json:"reconnect_timeout"`
}

type MatchState struct {
	Profile             RuleProfile         `json:"profile"`
	Table               TableSpec           `json:"table"`
	Tooling             ToolingFlags        `json:"tooling"`
	Phase               Phase               `json:"phase"`
	Tick                uint64              `json:"tick"`
	RNGSeed             uint64              `json:"rng_seed"`
	BreakSeatIndex      int                 `json:"break_seat_index"`
	ActiveSeatIndex     int                 `json:"active_seat_index"`
	TurnNumber          int                 `json:"turn_number"`
	LegalGroupsAssigned bool                `json:"legal_groups_assigned"`
	PlacementConstraint PlacementConstraint `json:"placement_constraint"`
	Seats               []SeatState         `json:"seats"`
	Balls               []BallState         `json:"balls"`
	PendingShot         *PendingShot        `json:"pending_shot,omitempty"`
	LastReplay          *ShotReplayRecord   `json:"last_replay,omitempty"`
}
