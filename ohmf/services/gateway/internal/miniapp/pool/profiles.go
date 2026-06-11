package pool

import (
	"errors"
	"fmt"
	"strings"
)

func DefaultRuleProfile(game GameType) RuleProfile {
	return RuleProfile{
		Game:                game,
		Mode:                GameModeHeadToHead,
		CallPocketMode:      CallPocketEightOnly,
		FoulLimit:           3,
		TimerEnabled:        false,
		TurnTimeoutSeconds:  15,
		GuidelinesEnabled:   true,
		MaxPlayers:          4,
		BackendAuthority:    true,
		DeterministicReplay: true,
	}
}

func (p RuleProfile) Validate() error {
	if p.Game != GameTypeEightBall && p.Game != GameTypeNineBall {
		return fmt.Errorf("unsupported game type %q", p.Game)
	}
	switch p.Mode {
	case GameModeHeadToHead, GameModeTeam, GameModeRoundRobin:
	default:
		return fmt.Errorf("unsupported game mode %q", p.Mode)
	}
	switch p.CallPocketMode {
	case CallPocketNone, CallPocketEightOnly, CallPocketAllShots:
	default:
		return fmt.Errorf("unsupported call pocket mode %q", p.CallPocketMode)
	}
	if p.FoulLimit < 1 {
		return errors.New("foul_limit must be at least 1")
	}
	if p.MaxPlayers < 2 || p.MaxPlayers > 4 {
		return errors.New("max_players must be between 2 and 4")
	}
	if p.TimerEnabled && p.TurnTimeoutSeconds < 1 {
		return errors.New("turn_timeout_seconds must be positive when timer is enabled")
	}
	if !p.BackendAuthority {
		return errors.New("backend_authority must be enabled")
	}
	if !p.DeterministicReplay {
		return errors.New("deterministic_replay must be enabled")
	}
	return nil
}

func NormalizeMatchSetup(setup MatchSetup) MatchSetup {
	normalized := setup
	if normalized.Profile.Game == "" {
		normalized.Profile = DefaultRuleProfile(GameTypeEightBall)
	}
	if normalized.Profile.Mode == "" {
		normalized.Profile.Mode = GameModeHeadToHead
	}
	if normalized.Profile.CallPocketMode == "" {
		if normalized.Profile.Game == GameTypeNineBall {
			normalized.Profile.CallPocketMode = CallPocketNone
		} else {
			normalized.Profile.CallPocketMode = CallPocketEightOnly
		}
	}
	if normalized.Profile.FoulLimit < 1 {
		normalized.Profile.FoulLimit = 3
	}
	if normalized.Profile.MaxPlayers == 0 {
		normalized.Profile.MaxPlayers = 4
	}
	if normalized.Profile.TurnTimeoutSeconds < 1 {
		normalized.Profile.TurnTimeoutSeconds = 15
	}
	normalized.Profile.BackendAuthority = true
	normalized.Profile.DeterministicReplay = true
	for i := range normalized.Seats {
		normalized.Seats[i].DisplayName = strings.TrimSpace(normalized.Seats[i].DisplayName)
		normalized.Seats[i].UserID = strings.TrimSpace(normalized.Seats[i].UserID)
		if normalized.Seats[i].PlayerKind == "" {
			normalized.Seats[i].PlayerKind = PlayerKindHuman
		}
	}
	return normalized
}
