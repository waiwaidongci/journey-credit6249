-- 可靠执行：fencing 代次、租约与状态时间戳。只加列，已有行(含获批底稿)保持原样。
-- 应用启动时会幂等执行同等 ALTER(src/services/execution.py ensure_schema)，本文件供审查留档。
ALTER TABLE budget_scenarios ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE budget_scenarios ADD COLUMN IF NOT EXISTS lease_owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE budget_scenarios ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE budget_scenarios ADD COLUMN IF NOT EXISTS updated_at VARCHAR;
ALTER TABLE attribution_runs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE attribution_runs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE attribution_runs ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE attribution_runs ADD COLUMN IF NOT EXISTS updated_at VARCHAR;
UPDATE budget_scenarios SET updated_at=created_at WHERE updated_at IS NULL;
UPDATE attribution_runs SET updated_at=created_at WHERE updated_at IS NULL;
CREATE INDEX IF NOT EXISTS scenario_status_lease_idx ON budget_scenarios (status,lease_expires_at);
CREATE INDEX IF NOT EXISTS run_status_lease_idx ON attribution_runs (status,lease_expires_at);
