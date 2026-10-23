---- MODULE MCConveyor ----
\* Model for TLC: three enqueue attempts, two sharing an idempotency key.
EXTENDS Conveyor

MCJobs == {"a1", "a2", "b1"}
MCKeyOf == [j \in MCJobs |-> IF j = "b1" THEN "k2" ELSE "k1"]
====
