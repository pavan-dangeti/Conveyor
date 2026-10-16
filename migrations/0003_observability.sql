ALTER TABLE jobs ADD COLUMN started_at TIMESTAMPTZ;
ALTER TABLE jobs_archive ADD COLUMN started_at TIMESTAMPTZ;

CREATE INDEX jobs_started_at_idx ON jobs (started_at) WHERE started_at IS NOT NULL;
CREATE INDEX jobs_finished_idx ON jobs (updated_at) WHERE state = 'completed';
CREATE INDEX job_dependencies_depends_on_idx ON job_dependencies (depends_on);

CREATE TABLE counters (
  name  TEXT PRIMARY KEY,
  value BIGINT NOT NULL DEFAULT 0
);
INSERT INTO counters (name) VALUES ('lease_expirations');
