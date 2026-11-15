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
  final_reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(event_id, subscription_id)
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id bigserial PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  attempt_no integer NOT NULL,
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  response_code integer,
  outcome text NOT NULL,
  error text
);
