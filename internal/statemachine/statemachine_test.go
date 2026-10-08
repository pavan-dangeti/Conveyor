package statemachine

import (
	"testing"

	"pgregory.net/rapid"
)

var allEvents = []Event{Enqueue, Due, Dequeue, Complete, FailRetry, FailDiscard, RetryDue, Cancel, ManualRetry, LeaseExpiredRetry, LeaseExpiredDiscard}

func TestTransitionTableEntriesReferenceKnownStates(t *testing.T) {
	for from, m := range transitions {
		for ev, to := range m {
			if _, err := Transition(from, ev); err != nil {
				t.Fatalf("%s --%s must be valid: %v", from, ev, err)
			}
			_ = to
		}
	}
}

func TestTerminalStatesAcceptNothing(t *testing.T) {
	for _, s := range []State{Completed, Cancelled} {
		for _, e := range allEvents {
			if Valid(s, e) {
				t.Fatalf("%s must reject %s", s, e)
			}
		}
	}
}

func TestIllegalTransitionsRejected(t *testing.T) {
	cases := []struct {
		from State
		ev   Event
	}{
		{Scheduled, Complete}, {Scheduled, Dequeue}, {Available, Complete},
		{Retryable, Complete}, {Completed, Dequeue}, {Discarded, Dequeue},
		{Cancelled, RetryDue}, {Running, RetryDue},
	}
	for _, c := range cases {
		if Valid(c.from, c.ev) {
			t.Fatalf("%s --%s must be rejected", c.from, c.ev)
		}
	}
}

func TestPathFromAvailableToCompleted(t *testing.T) {
	s := Available
	for _, ev := range []Event{Dequeue, Complete} {
		var err error
		s, err = Transition(s, ev)
		if err != nil {
			t.Fatal(err)
		}
	}
	if s != Completed {
		t.Fatalf("want completed, got %s", s)
	}
}

// Property: starting from a terminal state, no apply of valid transitions
// is accepted — completed and cancelled are absorbing. (Discarded is not:
// ManualRetry is a deliberate operator path.)
func TestRandomWalkTerminalAbsorbing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := State(rapid.SampledFrom([]State{Completed, Cancelled}).Draw(t, "state"))
		for _, e := range allEvents {
			if _, err := Transition(s, e); err == nil {
				t.Fatalf("terminal %s accepted %s", s, e)
			}
		}
	})
}
