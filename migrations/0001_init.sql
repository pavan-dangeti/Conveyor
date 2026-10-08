CREATE TABLE queues (
  name               TEXT PRIMARY KEY,
  concurrency        INT  NOT NULL DEFAULT 10 CHECK (concurrency > 0),
  rate_limit_per_sec DOUBLE PRECISION CHECK (rate_limit_per_sec IS NULL OR rate_limit_per_sec > 0),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
  id              BIGSERIAL PRIMARY KEY,
  queue           TEXT NOT NULL REFERENCES queues(name),
  kind            TEXT NOT NULL,
  args            JSONB NOT NULL,
  state           TEXT NOT NULL CHECK (state IN ('scheduled','available','running','completed','retryable','discarded','cancelled')),
  priority        INT NOT NULL DEFAULT 0,
  attempt         INT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  max_attempts    INT NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
  run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner     TEXT,
  lease_expiry    TIMESTAMPTZ,
  lease_version   BIGINT NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  error           TEXT,
  error_history   JSONB NOT NULL DEFAULT '[]'::jsonb,
  trace_context   JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (queue, idempotency_key)
);

CREATE INDEX jobs_dequeue_idx ON jobs (queue, priority DESC, run_at)
  WHERE state IN ('available', 'retryable');

CREATE INDEX jobs_lease_expiry_idx ON jobs (lease_expiry)
  WHERE state = 'running';

CREATE TABLE jobs_archive (
  id              BIGINT PRIMARY KEY,
  queue           TEXT NOT NULL,
  kind            TEXT NOT NULL,
  args            JSONB NOT NULL,
  state           TEXT NOT NULL,
  priority        INT NOT NULL,
  attempt         INT NOT NULL,
  max_attempts    INT NOT NULL,
  run_at          TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT,
  error           TEXT,
  error_history   JSONB NOT NULL,
  trace_context   JSONB,
  created_at      TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE cron_schedules (
  id          BIGSERIAL PRIMARY KEY,
  queue       TEXT NOT NULL REFERENCES queues(name),
  kind        TEXT NOT NULL,
  args        JSONB NOT NULL,
  spec        TEXT NOT NULL,
  next_run_at TIMESTAMPTZ NOT NULL,
  enabled     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE job_dependencies (
  job_id     BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  depends_on BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  PRIMARY KEY (job_id, depends_on),
  CHECK (job_id <> depends_on)
);

CREATE TABLE ledger (
  job_id      BIGINT NOT NULL,
  effect_id   TEXT NOT NULL,
  fencing_token BIGINT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, effect_id)
);
