---- MODULE Conveyor ----
(***************************************************************************)
(* The lease and fencing protocol of Conveyor.                             *)
(*                                                                         *)
(* Each element of Jobs is an enqueue attempt carrying an idempotency key  *)
(* (KeyOf); attempts that share a key must create at most one job. Workers *)
(* claim jobs, heartbeat, and complete or fail them; the reaper expires    *)
(* leases. A lease may lapse at any moment, which over-approximates real   *)
(* time: whatever holds here holds for every timing of heartbeats. Workers *)
(* crash at most MaxCrashes times, losing their in-memory tokens while     *)
(* their leases stay live until they lapse and are reaped, as after        *)
(* kill -9. Completion applies the effect and the state change in one      *)
(* atomic step, mirroring the guarded transaction in queue.CompleteTx.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Jobs, Workers, KeyOf, MaxAttempts, MaxCrashes

VARIABLES
  created,   \* the job row exists
  state,     \* jobs.state ("none" before the enqueue, "deduped" if it hit an existing key)
  owner,     \* jobs.lease_owner
  version,   \* jobs.lease_version, the fencing token
  attempts,  \* jobs.attempt
  leaseLive, \* jobs.lease_expiry is in the future
  effects,   \* ledger rows for the job
  alive,     \* the worker process is running
  token,     \* token[w][j]: the fencing token worker w holds for j, 0 if none
  crashes

vars == <<created, state, owner, version, attempts, leaseLive, effects, alive, token, crashes>>

None == "none"
Terminal == {"completed", "discarded"}

TypeOK ==
  /\ created \in [Jobs -> BOOLEAN]
  /\ state \in [Jobs -> {"none", "deduped", "available", "running", "retryable", "completed", "discarded"}]
  /\ owner \in [Jobs -> Workers \cup {None}]
  /\ version \in [Jobs -> Nat]
  /\ attempts \in [Jobs -> 0..MaxAttempts]
  /\ leaseLive \in [Jobs -> BOOLEAN]
  /\ effects \in [Jobs -> Nat]
  /\ alive \in [Workers -> BOOLEAN]
  /\ token \in [Workers -> [Jobs -> Nat]]
  /\ crashes \in 0..MaxCrashes

Init ==
  /\ created = [j \in Jobs |-> FALSE]
  /\ state = [j \in Jobs |-> "none"]
  /\ owner = [j \in Jobs |-> None]
  /\ version = [j \in Jobs |-> 0]
  /\ attempts = [j \in Jobs |-> 0]
  /\ leaseLive = [j \in Jobs |-> FALSE]
  /\ effects = [j \in Jobs |-> 0]
  /\ alive = [w \in Workers |-> TRUE]
  /\ token = [w \in Workers |-> [j \in Jobs |-> 0]]
  /\ crashes = 0

\* INSERT ... ON CONFLICT (queue, idempotency_key) DO NOTHING
Enqueue(j) ==
  /\ state[j] = "none"
  /\ IF \E i \in Jobs : created[i] /\ KeyOf[i] = KeyOf[j]
       THEN /\ state' = [state EXCEPT ![j] = "deduped"]
            /\ UNCHANGED created
       ELSE /\ created' = [created EXCEPT ![j] = TRUE]
            /\ state' = [state EXCEPT ![j] = "available"]
  /\ UNCHANGED <<owner, version, attempts, leaseLive, effects, alive, token, crashes>>

\* SELECT ... FOR UPDATE SKIP LOCKED, then lease_version = lease_version + 1
Dequeue(w, j) ==
  /\ alive[w]
  /\ state[j] = "available"
  /\ attempts[j] < MaxAttempts
  /\ state' = [state EXCEPT ![j] = "running"]
  /\ owner' = [owner EXCEPT ![j] = w]
  /\ version' = [version EXCEPT ![j] = @ + 1]
  /\ attempts' = [attempts EXCEPT ![j] = @ + 1]
  /\ leaseLive' = [leaseLive EXCEPT ![j] = TRUE]
  /\ token' = [token EXCEPT ![w][j] = version[j] + 1]
  /\ UNCHANGED <<created, effects, alive, crashes>>

\* w is the current lease holder of j: it holds j's latest fencing token.
IsCurrent(w, j) == token[w][j] # 0 /\ token[w][j] = version[j] /\ owner[j] = w

\* The guard on every worker write (the WHERE clause of the guarded UPDATE).
Holds(w, j) == IsCurrent(w, j) /\ state[j] = "running"

Heartbeat(w, j) ==
  /\ alive[w] /\ Holds(w, j)
  /\ leaseLive' = [leaseLive EXCEPT ![j] = TRUE]
  /\ UNCHANGED <<created, state, owner, version, attempts, effects, alive, token, crashes>>

\* A guarded write that matches no row: the worker learns it lost the job.
LoseLease(w, j) ==
  /\ alive[w] /\ token[w][j] # 0 /\ ~Holds(w, j)
  /\ token' = [token EXCEPT ![w][j] = 0]
  /\ UNCHANGED <<created, state, owner, version, attempts, leaseLive, effects, alive, crashes>>

\* Effect and completion commit in one transaction guarded by the token.
Complete(w, j) ==
  /\ alive[w] /\ Holds(w, j)
  /\ effects' = [effects EXCEPT ![j] = @ + 1]
  /\ state' = [state EXCEPT ![j] = "completed"]
  /\ leaseLive' = [leaseLive EXCEPT ![j] = FALSE]
  /\ token' = [token EXCEPT ![w][j] = 0]
  /\ UNCHANGED <<created, owner, version, attempts, alive, crashes>>

Fail(w, j) ==
  /\ alive[w] /\ Holds(w, j)
  /\ state' = [state EXCEPT ![j] = IF attempts[j] < MaxAttempts THEN "retryable" ELSE "discarded"]
  /\ leaseLive' = [leaseLive EXCEPT ![j] = FALSE]
  /\ token' = [token EXCEPT ![w][j] = 0]
  /\ UNCHANGED <<created, owner, version, attempts, effects, alive, crashes>>

\* Time passes without a heartbeat.
Expire(j) ==
  /\ state[j] = "running" /\ leaseLive[j]
  /\ leaseLive' = [leaseLive EXCEPT ![j] = FALSE]
  /\ UNCHANGED <<created, state, owner, version, attempts, effects, alive, token, crashes>>

\* The maintenance leader's reaper.
Reap(j) ==
  /\ state[j] = "running" /\ ~leaseLive[j]
  /\ state' = [state EXCEPT ![j] = IF attempts[j] < MaxAttempts THEN "retryable" ELSE "discarded"]
  /\ UNCHANGED <<created, owner, version, attempts, leaseLive, effects, alive, token, crashes>>

RetryDue(j) ==
  /\ state[j] = "retryable"
  /\ state' = [state EXCEPT ![j] = "available"]
  /\ UNCHANGED <<created, owner, version, attempts, leaseLive, effects, alive, token, crashes>>

\* kill -9: in-memory tokens are lost; the leases stay live until they lapse.
Crash(w) ==
  /\ alive[w] /\ crashes < MaxCrashes
  /\ alive' = [alive EXCEPT ![w] = FALSE]
  /\ token' = [token EXCEPT ![w] = [j \in Jobs |-> 0]]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<created, state, owner, version, attempts, leaseLive, effects>>

Restart(w) ==
  /\ ~alive[w]
  /\ alive' = [alive EXCEPT ![w] = TRUE]
  /\ UNCHANGED <<created, state, owner, version, attempts, leaseLive, effects, token, crashes>>

Next ==
  \/ \E j \in Jobs : Enqueue(j) \/ Expire(j) \/ Reap(j) \/ RetryDue(j)
  \/ \E w \in Workers, j \in Jobs :
       Dequeue(w, j) \/ Heartbeat(w, j) \/ LoseLease(w, j) \/ Complete(w, j) \/ Fail(w, j)
  \/ \E w \in Workers : Crash(w) \/ Restart(w)

\* Steps that stay enabled eventually happen. Expire is fair because time
\* passes: without that, a worker that crashes while holding a lease would
\* leave its job running forever, which is the counterexample TLC finds when
\* the fairness on Expire is removed. Heartbeats, failures and crashes get
\* no fairness: the environment may withhold them forever.
Fairness ==
  /\ \A j \in Jobs : WF_vars(Enqueue(j)) /\ WF_vars(Expire(j)) /\ WF_vars(Reap(j)) /\ WF_vars(RetryDue(j))
  /\ \A w \in Workers : WF_vars(Restart(w))
  /\ \A w \in Workers, j \in Jobs : WF_vars(Dequeue(w, j)) /\ WF_vars(Complete(w, j)) /\ WF_vars(LoseLease(w, j))

Spec == Init /\ [][Next]_vars /\ Fairness

----
\* An idempotency key creates at most one job.
IdempotentEnqueue == \A j, i \in Jobs : (created[j] /\ created[i] /\ KeyOf[j] = KeyOf[i]) => j = i

\* At most one worker holds the current token for a job.
SingleHolder == \A j \in Jobs : state[j] = "running" => Cardinality({w \in Workers : IsCurrent(w, j)}) <= 1

\* Exactly-once effects: at most one effect, and one exactly when completed.
EffectOnce == \A j \in Jobs : effects[j] <= 1
EffectIffCompleted == \A j \in Jobs : (effects[j] = 1) <=> (state[j] = "completed")

\* A running job's outcome changes only through the worker holding its
\* current token, or through the reaper once its lease has lapsed.
OnlyHolderDecides ==
  [][\A j \in Jobs :
      (state[j] = "running" /\ state'[j] # "running") =>
        (\E w \in Workers : IsCurrent(w, j)) \/ ~leaseLive[j]]_vars

\* Every created job eventually reaches a terminal state.
EventuallyTerminal == \A j \in Jobs : created[j] ~> state[j] \in Terminal
====
