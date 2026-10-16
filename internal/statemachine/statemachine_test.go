package statemachine

import (
	"slices"
	"testing"

	"pgregory.net/rapid"
)

func TestEveryEventHasExactlyOneTarget(t *testing.T) {
	for _, ev := range Events {
		targets := map[State]bool{}
		for _, m := range transitions {
			if to, ok := m[ev]; ok {
				targets[to] = true
			}
		}
		if len(targets) != 1 {
			t.Fatalf("event %s has %d targets, Edge requires exactly 1", ev, len(targets))
		}
		from, to := Edge(ev)
		for _, f := range from {
			if got, err := Transition(State(f), ev); err != nil || got != to {
				t.Fatalf("Edge(%s) disagrees with Transition(%s): %v %v", ev, f, got, err)
			}
		}
	}
}

func TestCompletedAndCancelledAreAbsorbing(t *testing.T) {
	for _, s := range []State{Completed, Cancelled} {
		for _, e := range Events {
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
		{Cancelled, RetryDue}, {Running, RetryDue}, {Running, ManualDiscard},
	}
	for _, c := range cases {
		if Valid(c.from, c.ev) {
			t.Fatalf("%s --%s must be rejected", c.from, c.ev)
		}
	}
}

func TestInitialAndTerminalStates(t *testing.T) {
	if Initial(true) != Scheduled || Initial(false) != Available {
		t.Fatal("initial state wrong")
	}
	if got := TerminalStates(); !slices.Equal(got, []string{"completed", "discarded", "cancelled"}) {
		t.Fatalf("terminal states: %v", got)
	}
	if !IsTerminal(Discarded) || IsTerminal(Running) {
		t.Fatal("IsTerminal wrong")
	}
}

func TestEdgePanicsOnUnknownEvent(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	Edge("bogus")
}

// Property: any random walk of valid transitions only visits known states,
// and once it reaches completed or cancelled it can never leave.
func TestRandomWalkStaysInModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := Initial(rapid.Bool().Draw(t, "delayed"))
		absorbed := false
		for range rapid.IntRange(0, 50).Draw(t, "steps") {
			ev := rapid.SampledFrom(Events).Draw(t, "event")
			next, err := Transition(s, ev)
			if err != nil {
				continue
			}
			if absorbed {
				t.Fatalf("left absorbing state %s via %s", s, ev)
			}
			if !slices.Contains(States, next) {
				t.Fatalf("unknown state %s", next)
			}
			s = next
			absorbed = s == Completed || s == Cancelled
		}
	})
}
