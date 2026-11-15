CREATE TABLE IF NOT EXISTS events (
  id uuid PRIMARY KEY,
  type text NOT NULL,
  source text NOT NULL,
  payload jsonb NOT NULL,
  idempotency_key text NOT NULL,
  producer_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (producer_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS subscriptions (
  id uuid PRIMARY KEY,
  webhook_url text NOT NULL,
  secret text NOT NULL,
  delivery_mode text NOT NULL CHECK (delivery_mode IN ('at_most_once','at_least_once','exactly_once')),
  rules jsonb NOT NULL,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS fanout_jobs (
  event_id uuid PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
  status text NOT NULL DEFAULT 'pending',
  locked_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fanout_jobs_unfinished_idx ON fanout_jobs(event_id) WHERE status <> 'done';
CREATE TABLE IF NOT EXISTS deliveries (
  id uuid PRIMARY KEY,
  event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  subscription_id uuid NOT NULL REFERENCES subscriptions(id),
  webhook_url text NOT NULL,
  secret text NOT NULL,
  delivery_mode text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempt_count integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_until timestamptz,
  claim_token uuid,
  final_reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(event_id, subscription_id)
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id bigserial PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  attempt_no integer NOT NULL,
  claim_token uuid,
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  response_code integer,
  -- authorized is a committed send permit, not evidence that HTTP was sent.
  -- Expired permits become unknown; recovery never overwrites completed history.
  outcome text NOT NULL,
  error text
);
-- Additive migration for installations created before pre-send authorization.
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS claim_token uuid;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS claim_token uuid;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_attempts_number_idx ON delivery_attempts(delivery_id, attempt_no);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_attempts_claim_idx ON delivery_attempts(claim_token) WHERE claim_token IS NOT NULL;
CREATE INDEX IF NOT EXISTS deliveries_ready_idx ON deliveries(next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS deliveries_leases_idx ON deliveries(locked_until) WHERE locked_until IS NOT NULL;
CREATE TABLE IF NOT EXISTS test_viewer_sessions (
  id uuid PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS test_viewer_subscriptions (
  subscription_id uuid PRIMARY KEY REFERENCES subscriptions(id) ON DELETE CASCADE,
  session_id uuid NOT NULL REFERENCES test_viewer_sessions(id) ON DELETE CASCADE,
  callback_token text NOT NULL,
  failures_remaining integer NOT NULL DEFAULT 0,
  received_attempts integer NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS test_viewer_receipts (
  delivery_id uuid PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
  subscription_id uuid NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  notification jsonb NOT NULL,
  receive_count integer NOT NULL DEFAULT 1,
  first_received_at timestamptz NOT NULL DEFAULT now(),
  last_received_at timestamptz NOT NULL DEFAULT now()
);
