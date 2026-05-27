# Sources

## Build & deploy

```bash
docker build -t source-simulator:latest .
docker save source-simulator:latest | sudo k3s ctr images import -
kubectl apply -f simulator-deployment.yaml
```

## Connect to the database

```bash
kubectl port-forward svc/data-lab-cluster-rw 5432:5432 &
psql -h localhost -U insurance_user -d insurance
```

## Useful queries

```sql
-- Row counts
SELECT COUNT(*) FROM policies;
SELECT COUNT(*) FROM claims;

-- Live growth (run twice, compare)
SELECT COUNT(*), MAX(start_date) AS latest FROM policies;
SELECT COUNT(*), MAX(event_ts)   AS latest FROM claims;

-- Claims by status
SELECT status, COUNT(*) FROM claims GROUP BY status;

-- Loss ratio by product line
SELECT
    p.product_line,
    ROUND(SUM(c.amount) / SUM(p.premium), 4) AS loss_ratio
FROM claims c
JOIN policies p ON c.policy_id = p.id
GROUP BY p.product_line
ORDER BY loss_ratio DESC;

-- Recent activity (last 10 events)
SELECT 'policy' AS type, id, start_date AS ts FROM policies
UNION ALL
SELECT 'claim',           id, event_ts         FROM claims
ORDER BY ts DESC
LIMIT 10;

-- Orphan claims (should always be zero)
SELECT COUNT(*) FROM claims WHERE policy_id NOT IN (SELECT id FROM policies);

-- Clear tables
TRUNCATE TABLE claims, policies RESTART IDENTITY CASCADE;
```