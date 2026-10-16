// Package statemachine is the single place where job states, events, and
// allowed transitions are defined. The queue package derives every state
// write (source states in the WHERE clause, target state in the SET clause)
// from Edge, so SQL cannot perform a transition this table does not allow.
package statemachine

import (
	"fmt"
	"sort"
)

type State string

const (
	Scheduled State = "scheduled"
	Available State = "available"
	Running   State = "running"
	Completed State = "completed"
	Retryable State = "retryable"
	Discarded State = "discarded"
	Cancelled State = "cancelled"
)

type Event string

const (
	Due                 Event = "due"
	Dequeue             Event = "dequeue"
	Complete            Event = "complete"
	FailRetry           Event = "fail_retry"
	FailDiscard         Event = "fail_discard"
	RetryDue            Event = "retry_due"
	Cancel              Event = "cancel"
	ManualRetry         Event = "manual_retry"
	ManualDiscard       Event = "manual_discard"
	LeaseExpiredRetry   Event = "lease_expired_retry"
	LeaseExpiredDiscard Event = "lease_expired_discard"
)

var Events = []Event{Due, Dequeue, Complete, FailRetry, FailDiscard, RetryDue, Cancel,
	ManualRetry, ManualDiscard, LeaseExpiredRetry, LeaseExpiredDiscard}

var States = []State{Scheduled, Available, Running, Completed, Retryable, Discarded, Cancelled}

// Discarded is terminal for the automatic lifecycle but not absorbing: an
// operator may send a dead-lettered job back with ManualRetry.
var terminal = map[State]bool{Completed: true, Discarded: true, Cancelled: true}

func IsTerminal(s State) bool { return terminal[s] }

var transitions = map[State]map[Event]State{
	Scheduled: {Due: Available, Cancel: Cancelled, ManualDiscard: Discarded},
	Available: {Dequeue: Running, Cancel: Cancelled, ManualDiscard: Discarded},
	Running: {
		Complete:            Completed,
		FailRetry:           Retryable,
		FailDiscard:         Discarded,
		Cancel:              Cancelled,
		LeaseExpiredRetry:   Retryable,
		LeaseExpiredDiscard: Discarded,
	},
	Retryable: {RetryDue: Available, Cancel: Cancelled, ManualDiscard: Discarded},
	Discarded: {ManualRetry: Available},
	Completed: {},
	Cancelled: {},
}

// Initial is the state a newly enqueued job starts in.
func Initial(delayed bool) State {
	if delayed {
		return Scheduled
	}
	return Available
}

func Transition(from State, ev Event) (State, error) {
	next, ok := transitions[from][ev]
	if !ok {
		return from, fmt.Errorf("illegal transition %s --%s-->", from, ev)
	}
	return next, nil
}

func Valid(from State, ev Event) bool {
	_, err := Transition(from, ev)
	return err == nil
}

// Edge returns every state the event may fire from and the single state it
// leads to. Each event has exactly one target (checked by tests), which is
// what lets a transition be one guarded UPDATE.
func Edge(ev Event) (from []string, to State) {
	for s, m := range transitions {
		if t, ok := m[ev]; ok {
			from = append(from, string(s))
			to = t
		}
	}
	if len(from) == 0 {
		panic(fmt.Sprintf("statemachine: event %q has no transitions", ev))
	}
	sort.Strings(from)
	return from, to
}

// TerminalStates lists the states whose rows may be archived.
func TerminalStates() []string {
	var out []string
	for _, s := range States {
		if terminal[s] {
			out = append(out, string(s))
		}
	}
	return out
}
