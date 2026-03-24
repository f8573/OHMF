package messages

import (
	"testing"
	"time"
)

func assertSameInstant(t *testing.T, field, want, got string) {
	t.Helper()

	wantTime, err := time.Parse(time.RFC3339Nano, want)
	if err != nil {
		t.Fatalf("parse expected %s %q: %v", field, want, err)
	}
	gotTime, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("parse actual %s %q: %v", field, got, err)
	}
	if !wantTime.Truncate(time.Second).Equal(gotTime.Truncate(time.Second)) {
		t.Fatalf("expected preserved %s %q, got %q", field, want, got)
	}
}
