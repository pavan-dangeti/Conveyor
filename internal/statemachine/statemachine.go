// Package statemachine is the single place where job states, events, and
// allowed transitions are defined. All writes to jobs.state must route
// through Transition.
package statemachine

import "fmt"

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
	Enqueue             Event = "enqueue"
	Due                 Event = "due"
	Dequeue             Event = "dequeue"
	Complete            Event = "complete"
	FailRetry           Event = "fail_retry"
	FailDiscard         Event = "fail_discard"
	RetryDue            Event = "retry_due"
	Cancel              Event = "cancel"
	ManualRetry         Event = "manual_retry"
	LeaseExpiredRetry   Event = "lease_expired_retry"
	LeaseExpiredDiscard Event = "lease_expired_discard"
)

var terminal = map[State]bool{Completed: true, Discarded: true, Cancelled: true}

func IsTerminal(s State) bool { return terminal[s] }

var transitions = map[State]map[Event]State{
	Scheduled: {Due: Available, Cancel: Cancelled},
	Available: {Dequeue: Running, Cancel: Cancelled},
	Running: {
		Complete:            Completed,
		FailRetry:           Retryable,
		FailDiscard:         Discarded,
		Cancel:              Cancelled,
		LeaseExpiredRetry:   Retryable,
		LeaseExpiredDiscard: Discarded,
	},
	Retryable: {RetryDue: Available, Cancel: Cancelled},
	Discarded: {ManualRetry: Available},
	Completed: {},
	Cancelled: {},
}

func Transition(from State, ev Event) (State, error) {
	next, ok := transitions[from][ev]
	if !ok {
		return from, fmt.Errorf("illegal transition %s --%s-->", from, ev)
	}
	return next, nil
}

// Valid reports whether the transition is allowed.
func Valid(from State, ev Event) bool {
	_, err := Transition(from, ev)
	return err == nil
}
