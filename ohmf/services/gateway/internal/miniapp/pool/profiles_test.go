package pool

import "testing"

func TestDefaultRuleProfileEightBall(t *testing.T) {
	profile := DefaultRuleProfile(GameTypeEightBall)
	if profile.Game != GameTypeEightBall {
		t.Fatalf("game = %q", profile.Game)
	}
	if profile.Mode != GameModeHeadToHead {
		t.Fatalf("mode = %q", profile.Mode)
	}
	if profile.CallPocketMode != CallPocketEightOnly {
		t.Fatalf("call pocket mode = %q", profile.CallPocketMode)
	}
	if profile.FoulLimit != 3 {
		t.Fatalf("foul limit = %d", profile.FoulLimit)
	}
	if !profile.BackendAuthority || !profile.DeterministicReplay {
		t.Fatalf("backend authority and deterministic replay must be enabled")
	}
}

func TestDefaultRuleProfileNineBall(t *testing.T) {
	profile := NormalizeMatchSetup(MatchSetup{
		Profile: RuleProfile{Game: GameTypeNineBall},
	})
	if profile.Profile.CallPocketMode != CallPocketNone {
		t.Fatalf("call pocket mode = %q", profile.Profile.CallPocketMode)
	}
}
