CREATE TABLE queue_rate (
  queue TEXT PRIMARY KEY REFERENCES queues(name),
  last_dispatch TIMESTAMPTZ
);
