-- Migration: bracket tracer generality
-- Date: 2026-04-17
-- Spec: docs/superpowers/specs/2026-04-17-bracket-tracer-generality-design.md
-- Rollback: drop the two new columns; revert matchup.placement_game to VARCHAR

-- 1. Add consolation columns to league_settings
ALTER TABLE ___leagues.public.league_settings
  ADD COLUMN IF NOT EXISTS has_consolation_bracket BOOLEAN;
ALTER TABLE ___leagues.public.league_settings
  ADD COLUMN IF NOT EXISTS num_playoff_consolation_teams INTEGER;

-- 2. Normalize matchup.placement_game before type change (MANDATORY pre-step)
-- Old pandas pipeline wrote strings like 'championship', 'third_place_game'
-- into this column; direct ALTER will fail on those.
-- Pre-flight check (2026-04-17): 0 non-numeric values found. Safe to proceed.
UPDATE ___leagues.public.matchup
SET placement_game = NULL
WHERE placement_game IS NOT NULL
  AND placement_game NOT IN ('0', '1');

-- 3. Change matchup.placement_game type VARCHAR -> INTEGER
ALTER TABLE ___leagues.public.matchup
ALTER COLUMN placement_game TYPE INTEGER USING CAST(placement_game AS INTEGER);

-- =============================================================
-- ROLLBACK (do NOT run unless explicitly backing out the migration)
-- =============================================================
-- ALTER TABLE ___leagues.public.league_settings DROP COLUMN IF EXISTS has_consolation_bracket;
-- ALTER TABLE ___leagues.public.league_settings DROP COLUMN IF EXISTS num_playoff_consolation_teams;
-- -- Revert placement_game to VARCHAR. Note: all data currently in [0,1,NULL] post-migration;
-- -- no way to restore original string values. Document any rollback as one-way.
-- ALTER TABLE ___leagues.public.matchup
-- ALTER COLUMN placement_game TYPE VARCHAR USING CAST(placement_game AS VARCHAR);
