CREATE TABLE IF NOT EXISTS policies (
    id          UUID PRIMARY KEY,
    product_line TEXT NOT NULL,
    region       TEXT NOT NULL,
    premium      NUMERIC(10, 2) NOT NULL,
    start_date   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS claims (
    id          UUID PRIMARY KEY,
    policy_id   UUID NOT NULL REFERENCES policies(id),
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT NOT NULL,
    event_ts    TIMESTAMP NOT NULL
);
