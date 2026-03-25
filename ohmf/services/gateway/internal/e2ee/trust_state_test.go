package e2ee

import "testing"

func TestDeriveEffectiveTrustState(t *testing.T) {
	t.Run("verified stays verified when fingerprint matches", func(t *testing.T) {
		state, warning := deriveEffectiveTrustState("VERIFIED", "abc", "abc")
		if state != "VERIFIED" {
			t.Fatalf("expected VERIFIED, got %s", state)
		}
		if warning != "" {
			t.Fatalf("expected no warning, got %q", warning)
		}
	})

	t.Run("revoked stays visible when fingerprint matches", func(t *testing.T) {
		state, warning := deriveEffectiveTrustState("BLOCKED", "abc", "abc")
		if state != "REVOKED" {
			t.Fatalf("expected REVOKED, got %s", state)
		}
		if warning == "" {
			t.Fatal("expected revoke warning")
		}
	})

	t.Run("mismatch overrides previously stored state", func(t *testing.T) {
		state, warning := deriveEffectiveTrustState("VERIFIED", "old", "new")
		if state != "MISMATCH" {
			t.Fatalf("expected MISMATCH, got %s", state)
		}
		if warning == "" {
			t.Fatal("expected mismatch warning")
		}
	})

	t.Run("missing stored trust defaults to unverified", func(t *testing.T) {
		state, warning := deriveEffectiveTrustState("", "", "abc")
		if state != "UNVERIFIED" {
			t.Fatalf("expected UNVERIFIED, got %s", state)
		}
		if warning != "" {
			t.Fatalf("expected no warning, got %q", warning)
		}
	})
}
