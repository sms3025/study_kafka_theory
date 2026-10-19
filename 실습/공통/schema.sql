CREATE TABLE IF NOT EXISTS processed_events (
    handler varchar(80) NOT NULL,
    event_id varchar(180) NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (handler, event_id)
);
CREATE TABLE IF NOT EXISTS order_totals (
    order_id varchar(180) PRIMARY KEY,
    event_count bigint NOT NULL,
    total_amount bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_db (
    order_id varchar(180) PRIMARY KEY,
    amount bigint NOT NULL CHECK (amount > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id varchar(180) PRIMARY KEY,
    aggregate_id varchar(180) NOT NULL,
    payload text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (created_at, event_id) WHERE published_at IS NULL;
