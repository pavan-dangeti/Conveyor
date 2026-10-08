---- MODULE Conveyor ----
EXTENDS Integers, FiniteSets

CONSTANTS Jobs, Workers, MaxAttempts

VARIABLES jobState, jobOwner, jobVersion, jobAttempts, leaseValid,
          effectCount, workerAlive, workerVersion

vars == <<jobState, jobOwner, jobVersion, jobAttempts, leaseValid,
          effectCount, workerAlive, workerVersion>>

None == "none"

Init ==
  /\ jobState = [j \in Jobs |-> "available"]
  /\ jobOwner = [j \in Jobs |-> None]
  /\ jobVersion = [j \in Jobs |-> 0]
  /\ jobAttempts = [j \in Jobs |-> 0]
  /\ leaseValid = [j \in Jobs |-> FALSE]
  /\ effectCount = [j \in Jobs |-> 0]
  /\ workerAlive = [w \in Workers |-> TRUE]
  /\ workerVersion = [w \in Workers |-> [j \in Jobs |-> 0]]

Dequeue(w, j) ==
  /\ workerAlive[w]
  /\ jobState[j] \in {"available", "retryable"}
  /\ ~leaseValid[j]
  /\ jobAttempts[j] < MaxAttempts
  /\ jobAttempts' = [jobAttempts EXCEPT ![j] = @ + 1]
  /\ jobState' = [jobState EXCEPT ![j] = "running"]
  /\ jobOwner' = [jobOwner EXCEPT ![j] = w]
  /\ jobVersion' = [jobVersion EXCEPT ![j] = @ + 1]
  /\ leaseValid' = [leaseValid EXCEPT ![j] = TRUE]
  /\ workerVersion' = [workerVersion EXCEPT ![w][j] = jobVersion[j] + 1]
  /\ UNCHANGED <<effectCount, workerAlive>>

Heartbeat(w, j) ==
  /\ workerAlive[w]
  /\ jobState[j] = "running"
  /\ jobOwner[j] = w
  /\ workerVersion[w][j] = jobVersion[j]
  /\ leaseValid' = [leaseValid EXCEPT ![j] = TRUE]
  /\ UNCHANGED <<jobState, jobOwner, jobVersion, jobAttempts,
                 effectCount, workerAlive, workerVersion>>

ExpireLease(j) ==
  /\ jobState[j] = "running"
  /\ leaseValid[j]
  /\ leaseValid' = [leaseValid EXCEPT ![j] = FALSE]
  /\ jobState' = [jobState EXCEPT ![j] =
        IF jobAttempts[j] < MaxAttempts THEN "retryable" ELSE "discarded"]
  /\ UNCHANGED <<jobOwner, jobVersion, jobAttempts,
                 effectCount, workerAlive, workerVersion>>

Complete(w, j) ==
  /\ workerAlive[w]
  /\ jobState[j] = "running"
  /\ jobOwner[j] = w
  /\ workerVersion[w][j] = jobVersion[j]
  /\ leaseValid[j]
  /\ effectCount' = [effectCount EXCEPT ![j] = @ + 1]
  /\ jobState' = [jobState EXCEPT ![j] = "completed"]
  /\ leaseValid' = [leaseValid EXCEPT ![j] = FALSE]
  /\ UNCHANGED <<jobOwner, jobVersion, jobAttempts, workerAlive, workerVersion>>

Fail(w, j) ==
  /\ workerAlive[w]
  /\ jobState[j] = "running"
  /\ jobOwner[j] = w
  /\ workerVersion[w][j] = jobVersion[j]
  /\ leaseValid[j]
  /\ leaseValid' = [leaseValid EXCEPT ![j] = FALSE]
  /\ jobState' = [jobState EXCEPT ![j] =
        IF jobAttempts[j] < MaxAttempts THEN "retryable" ELSE "discarded"]
  /\ UNCHANGED <<jobOwner, jobVersion, jobAttempts, effectCount,
                 workerAlive, workerVersion>>

RequeueBackoffElapsed(j) ==
  /\ jobState[j] = "retryable"
  /\ ~leaseValid[j]
  /\ jobState' = [jobState EXCEPT ![j] = "available"]
  /\ UNCHANGED <<jobOwner, jobVersion, jobAttempts, leaseValid,
                 effectCount, workerAlive, workerVersion>>

Crash(w) ==
  /\ workerAlive[w]
  /\ workerAlive' = [workerAlive EXCEPT ![w] = FALSE]
  \* A crashed worker cannot heartbeat, so its leases are doomed; the
  \* reaper would expire them. Model that by invalidating them now.
  /\ leaseValid' = [j \in Jobs |->
        IF jobState[j] = "running" /\ jobOwner[j] = w THEN FALSE ELSE leaseValid[j]]
  /\ jobState' = [j \in Jobs |->
        IF jobState[j] = "running" /\ jobOwner[j] = w
        THEN IF jobAttempts[j] < MaxAttempts THEN "retryable" ELSE "discarded"
        ELSE jobState[j]]
  /\ UNCHANGED <<jobOwner, jobVersion, jobAttempts, effectCount, workerVersion>>

Restart(w) ==
  /\ ~workerAlive[w]
  /\ workerAlive' = [workerAlive EXCEPT ![w] = TRUE]
  /\ workerVersion' = [workerVersion EXCEPT ![w] = [j \in Jobs |-> 0]]
  /\ UNCHANGED <<jobState, jobOwner, jobVersion, jobAttempts, leaseValid,
                 effectCount>>

Next ==
  \/ \E w \in Workers, j \in Jobs : Dequeue(w, j)
  \/ \E w \in Workers, j \in Jobs : Heartbeat(w, j)
  \/ \E w \in Workers, j \in Jobs : Complete(w, j)
  \/ \E w \in Workers, j \in Jobs : Fail(w, j)
  \/ \E j \in Jobs : ExpireLease(j)
  \/ \E j \in Jobs : RequeueBackoffElapsed(j)
  \/ \E w \in Workers : Crash(w)
  \/ \E w \in Workers : Restart(w)

Spec == Init /\ [][Next]_vars

EffectOnce == \A j \in Jobs : effectCount[j] <= 1

EffectImpliesCompleted == \A j \in Jobs :
  effectCount[j] = 1 => jobState[j] = "completed"

RunningImpliesValidLease == \A j \in Jobs :
  jobState[j] = "running" =>
    IF jobOwner[j] # None
    THEN leaseValid[j] /\ workerAlive[jobOwner[j]]
    ELSE FALSE

RunningLeaseHolderAgrees == \A j \in Jobs :
  (jobState[j] = "running" /\ leaseValid[j]) =>
    \E w \in Workers : jobOwner[j] = w /\ workerVersion[w][j] = jobVersion[j]

====
