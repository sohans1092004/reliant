-- LOCAL DEV ONLY: tenants whose API keys are published in this repo.
-- Applied by `npm run migrate` when DEV_SEED=true, and by docker-compose's Postgres on first start.
-- Never set DEV_SEED on a server anyone else can reach: these keys open those tenants.

-- Dashboard API keys: dev_key_tenant_a / dev_key_tenant_b.
INSERT INTO tenants (tenant_id, name, api_key_hash) VALUES
  ('tenant_a', 'Tenant A', encode(sha256('dev_key_tenant_a'::bytea), 'hex')),
  ('tenant_b', 'Tenant B', encode(sha256('dev_key_tenant_b'::bytea), 'hex'))
ON CONFLICT DO NOTHING;

-- tenant_b points at fake-slow-endpoint.js. Replace tenant_a's URL with your webhook.site one.
INSERT INTO endpoints (tenant_id, url)
SELECT * FROM (VALUES
  ('tenant_a', 'https://webhook.site/REPLACE-ME'),
  ('tenant_b', 'http://localhost:9998')
) AS seed(tenant_id, url)
WHERE NOT EXISTS (SELECT 1 FROM endpoints);
