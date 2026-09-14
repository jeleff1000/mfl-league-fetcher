-- Add errored + error_message columns to fleet_health validation results.
-- Enables batch-error surfacing: when a batch fails (binder error etc.),
-- every check in the batch gets a row with errored=TRUE + the error message
-- instead of silently disappearing.
--
-- DuckDB constraint note: DuckDB/MotherDuck does NOT support adding NOT NULL
-- DEFAULT constraints via ALTER TABLE ADD COLUMN ("Parser Error: Adding
-- columns with constraints not yet supported"). So we:
--   1. Add the column as plain nullable BOOLEAN.
--   2. Explicitly backfill all existing rows to FALSE.
--   3. Trust the application layer (storage writer in reporter.py) to
--      never insert NULL for `errored` — CheckResult.errored is bool with
--      default False, never None.
--
-- This means the column is technically nullable in the catalog, but no row
-- will ever actually contain NULL after this migration. Queries can safely
-- write `WHERE errored = FALSE` without handling NULL. Fresh tables created
-- via reporter.py's _RESULTS_DDL (CREATE TABLE IF NOT EXISTS) do use NOT
-- NULL DEFAULT FALSE because the CREATE path supports it — only ALTER is
-- the limitation.
--
-- error_message is legitimately nullable — only errored rows have a message.
--
-- Rollback:
--   ALTER TABLE ___ops.fleet_health.validation_results DROP COLUMN IF EXISTS errored;
--   ALTER TABLE ___ops.fleet_health.validation_results DROP COLUMN IF EXISTS error_message;
-- Rollback requires a coordinated code revert of reporter.py / models.py /
-- validate.py to avoid writes to dropped columns on the next run.

-- Step 1: add columns (idempotent, no constraints — DuckDB ALTER limitation)
ALTER TABLE ___ops.fleet_health.validation_results ADD COLUMN IF NOT EXISTS errored BOOLEAN;
ALTER TABLE ___ops.fleet_health.validation_results ADD COLUMN IF NOT EXISTS error_message VARCHAR;

-- Step 2: explicit backfill — every existing row becomes errored=FALSE
-- (app layer guarantees future writes will never be NULL)
UPDATE ___ops.fleet_health.validation_results SET errored = FALSE WHERE errored IS NULL;
