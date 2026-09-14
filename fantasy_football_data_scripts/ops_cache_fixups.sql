-- Post-restore data fixups applied to the ops_cache.duckdb after
-- GH Actions cache restore. Each statement should be:
--   * Idempotent (re-runnable without harm)
--   * Fast (sub-second for the ~800K-row super_table)
--   * Limited to corrections that have already been applied to Fly's
--     ___ops so the local cache and Fly stay in sync.
--
-- This avoids forcing a multi-hour cache rebuild every time a small
-- super_table correction lands. Adding a new fixup here doesn't
-- invalidate the ops-cache key — workers just re-apply on next run.

-- ---------------------------------------------------------------
-- 2026-04-28: Backfill pts_def_st_td for older DEF rows.
-- Applied to Fly via scripts/backfill_pts_def_st_td.py (commit 9689e43f).
-- Without this, populate_fantasy_points's DEF recompute multiplies by 0
-- because the column was sparse pre-2014 (only kr+pr returns counted).
UPDATE nfl_historical.nfl_player_stats_all
SET pts_def_st_td = pts_def_ret_td - COALESCE(pts_def_blk_kick_td, 0)
WHERE position = 'DEF'
  AND pts_def_st_td IS NULL
  AND pts_def_ret_td IS NOT NULL;


-- ---------------------------------------------------------------
-- 2026-04-28: Targeted player_bio.yahoo_player_id corrections.
--
-- These mirror the safe-only set re-applied to Fly via
-- scripts/restore_bio_and_apply_safe_corrections.py after rolling
-- back the over-eager 859-correction batch. The earlier batch had
-- trusted yahoo_nfl_player_map which had its own corruption (newer
-- players' yahoo_ids wrongly attached to older players' NFL_ids,
-- "Royce Freeman -> Janikowski-NFL_id" pattern at scale). That
-- corrupted bio for ~hundreds of legitimate older RB/WR rows whose
-- yahoo_ids were already correct, breaking the resolver and causing
-- mohoney 2013-2015 to undercount 100s of starters.
--
-- The 6 corrections below are MANUALLY VERIFIED:
--   - Conklin/Izzo: cross-checked against Yahoo's draft API name field
--   - Janikowski/McCown/Schaub/Roethlisberger: collision-repair where the
--     cache had two Layer-1 rows pointing at the same NFL_id and the
--     dedup tiebreak picked the newer (wrong) player.
--
-- Strategy: clear yahoo_id from any row currently holding the target
-- value, then set it on the correct NFL_player_id row.

-- Phase A: clear these 6 yahoo_ids from any row that currently has them
UPDATE nfl_historical.player_bio
SET yahoo_player_id = NULL
WHERE CAST(yahoo_player_id AS BIGINT) IN (118, 138, 178, 208, 31127, 31220);

-- Phase B: set yahoo_id on the correct NFL_player_id row
UPDATE nfl_historical.player_bio SET yahoo_player_id = 118   WHERE NFL_player_id = '00-0019646';  -- Sebastian Janikowski
UPDATE nfl_historical.player_bio SET yahoo_player_id = 208   WHERE NFL_player_id = '00-0021206';  -- Josh McCown
UPDATE nfl_historical.player_bio SET yahoo_player_id = 178   WHERE NFL_player_id = '00-0022787';  -- Matt Schaub
UPDATE nfl_historical.player_bio SET yahoo_player_id = 138   WHERE NFL_player_id = '00-0022924';  -- Ben Roethlisberger
UPDATE nfl_historical.player_bio SET yahoo_player_id = 31127 WHERE NFL_player_id = '00-0034270';  -- Tyler Conklin
UPDATE nfl_historical.player_bio SET yahoo_player_id = 31220 WHERE NFL_player_id = '00-0034439';  -- Ryan Izzo


-- ---------------------------------------------------------------
-- 2026-04-30: yahoo_nfl_player_map collision repair — Charles Johnson WR.
--
-- Layer-1 name-match (100% confidence) routed yahoo_player_id 26839
-- (Charles D. Johnson, Vikings WR 2013-2016) to NFL_player_id
-- 00-0008454 (Charles Johnson, Buffalo WR retired 2002). Both share
-- the same display name, no DOB/team disambiguation kicked in. The
-- bio is already correct: 26839 lives on 00-0030113. Only the cache
-- entry needs to be repointed so future imports resolve via the cache
-- without falling back to the buggy Layer-1 path.
--
-- Concrete impact: Ross 2015 wk1 (mohoney_moproblems) had Charles
-- Johnson WR FLEX-started in Yahoo with 4.70 pts; our player_fantasy
-- row pointed at the retired-2002 Buffalo WR and stored 0 pts,
-- producing a +4.7 system_team_points_vs_player_sum gap.
--
-- Applied on Fly only:
--   UPDATE ___ops.public.yahoo_nfl_player_map
--   SET NFL_player_id = '00-0030113', nfl_name = 'Charles D. Johnson',
--       match_layer = 0, match_confidence = 100,
--       updated_at = CURRENT_TIMESTAMP
--   WHERE yahoo_player_id = '26839';
--
-- The local ops_cache.duckdb only contains the nfl_historical schema (see
-- build_ops_cache.py); the platform-id maps live exclusively on Fly. Replaying
-- this UPDATE locally would error on a missing schema and is not needed —
-- workers' import path always resolves yahoo_id via Fly, not via the local
-- cache. Documented here for the audit trail.


-- ---------------------------------------------------------------
-- 2026-04-28: Player_bio enrichment for unjoined player_fantasy IDs.
-- Applied to Fly via scripts/fix_unjoined_player_ids_2026_04_28.py +
-- scripts/_rollback_bad_lane_b_writes.py + scripts/_backfill_player_fantasy_nfl_id.py.
-- Resolved 1,934 of 7,067 previously-unjoined player_fantasy.NFL_player_id rows
-- across 121 leagues. Remaining 5,133 are STUB/HC/bench scrubs (out of scope).
--
-- Pattern: each correction sets the (yahoo_player_id, sleeper_player_id) pair
-- to the post-fix value. Idempotent — the bio table already has these values
-- on Fly; this block exists so a cold local cache restored from the backup
-- arrives at the same state.

UPDATE nfl_historical.player_bio SET yahoo_player_id = 4672, sleeper_player_id = NULL WHERE NFL_player_id = '00-0018093';  -- Antoine Winfield
UPDATE nfl_historical.player_bio SET yahoo_player_id = 9039, sleeper_player_id = 5820 WHERE NFL_player_id = '00-0025860';  -- BenJarvus Green-Ellis
UPDATE nfl_historical.player_bio SET yahoo_player_id = 9004, sleeper_player_id = 660 WHERE NFL_player_id = '00-0026367';  -- Peyton Hillis
UPDATE nfl_historical.player_bio SET yahoo_player_id = 30082, sleeper_player_id = 3957 WHERE NFL_player_id = '00-0033193';  -- Blake Sims
UPDATE nfl_historical.player_bio SET yahoo_player_id = 31944, sleeper_player_id = 5889 WHERE NFL_player_id = '00-0035256';  -- Bryce Love
UPDATE nfl_historical.player_bio SET yahoo_player_id = 32740, sleeper_player_id = 6911 WHERE NFL_player_id = '00-0036221';  -- Brandon Jones
UPDATE nfl_historical.player_bio SET yahoo_player_id = 32715, sleeper_player_id = 6888 WHERE NFL_player_id = '00-0036411';  -- Antoine Winfield Jr.
UPDATE nfl_historical.player_bio SET yahoo_player_id = 33572, sleeper_player_id = 7808 WHERE NFL_player_id = '00-0036565';  -- Ben Mason
UPDATE nfl_historical.player_bio SET yahoo_player_id = 33918, sleeper_player_id = NULL WHERE NFL_player_id = '00-0036873';  -- Jake Verity
UPDATE nfl_historical.player_bio SET yahoo_player_id = 33492, sleeper_player_id = 7685 WHERE NFL_player_id = '00-0036987';  -- Brandon Stephens
UPDATE nfl_historical.player_bio SET yahoo_player_id = 34232, sleeper_player_id = 8532 WHERE NFL_player_id = '00-0037493';  -- Mike Brown
UPDATE nfl_historical.player_bio SET yahoo_player_id = 34284, sleeper_player_id = 8663 WHERE NFL_player_id = '00-0037580';  -- Jared Bernhardt
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40572, sleeper_player_id = 11328 WHERE NFL_player_id = '00-0038778';  -- Jalen Redmond
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40801, sleeper_player_id = 11524 WHERE NFL_player_id = '00-0038954';  -- Izaiah Gathings
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40160, sleeper_player_id = 10949 WHERE NFL_player_id = '00-0038982';  -- Chamarri Conner
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40873, sleeper_player_id = 11653 WHERE NFL_player_id = '00-0039229';  -- Charlie Smyth
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40895, sleeper_player_id = 11668 WHERE NFL_player_id = '00-0039309';  -- Byron Murphy II
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40956, sleeper_player_id = 11724 WHERE NFL_player_id = '00-0039347';  -- Trevin Wallace
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41098, sleeper_player_id = 11621 WHERE NFL_player_id = '00-0039415';  -- Brenden Rice
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41123, sleeper_player_id = 11842 WHERE NFL_player_id = '00-0039435';  -- Tatum Bethune
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41135, sleeper_player_id = 11859 WHERE NFL_player_id = '00-0039456';  -- John Rhys Plumlee
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41000, sleeper_player_id = 11734 WHERE NFL_player_id = '00-0039815';  -- Tyrice Knight
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41090, sleeper_player_id = 11807 WHERE NFL_player_id = '00-0039829';  -- Darius Muasau
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40948, sleeper_player_id = 11710 WHERE NFL_player_id = '00-0039838';  -- Calen Bullock
UPDATE nfl_historical.player_bio SET yahoo_player_id = 11760, sleeper_player_id = 11760 WHERE NFL_player_id = '00-0039840';  -- Austin Booker
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40934, sleeper_player_id = 11678 WHERE NFL_player_id = '00-0039841';  -- Cooper DeJean
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41562, sleeper_player_id = 12267 WHERE NFL_player_id = '00-0039843';  -- Qadir Ismail
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40894, sleeper_player_id = 11665 WHERE NFL_player_id = '00-0039852';  -- Jared Verse
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40935, sleeper_player_id = 11674 WHERE NFL_player_id = '00-0039861';  -- Tyler Nubin
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40912, sleeper_player_id = 11682 WHERE NFL_player_id = '00-0039872';  -- Cole Bishop
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40932, sleeper_player_id = 11676 WHERE NFL_player_id = '00-0039898';  -- Mike Sainristil
UPDATE nfl_historical.player_bio SET yahoo_player_id = NULL, sleeper_player_id = 11680 WHERE NFL_player_id = '00-0039899';  -- Patrick Paul
UPDATE nfl_historical.player_bio SET yahoo_player_id = NULL, sleeper_player_id = 11688 WHERE NFL_player_id = '00-0039908';  -- Kris Jenkins
UPDATE nfl_historical.player_bio SET yahoo_player_id = 40879, sleeper_player_id = 11663 WHERE NFL_player_id = '00-0039911';  -- Chop Robinson
UPDATE nfl_historical.player_bio SET yahoo_player_id = 42018, sleeper_player_id = 12738 WHERE NFL_player_id = '00-0040021';  -- Phil Mafah
UPDATE nfl_historical.player_bio SET yahoo_player_id = NULL, sleeper_player_id = 12629 WHERE NFL_player_id = '00-0040068';  -- Emery Jones Jr.
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41908, sleeper_player_id = 12640 WHERE NFL_player_id = '00-0040069';  -- Teddye Buchanan
UPDATE nfl_historical.player_bio SET yahoo_player_id = 42470, sleeper_player_id = 13155 WHERE NFL_player_id = '00-0040108';  -- Chandler Martin
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41883, sleeper_player_id = 12591 WHERE NFL_player_id = '00-0040148';  -- Xavier Watts
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41906, sleeper_player_id = 12646 WHERE NFL_player_id = '00-0040157';  -- Barrett Carter
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41989, sleeper_player_id = 12511 WHERE NFL_player_id = '00-0040203';  -- Will Howard
UPDATE nfl_historical.player_bio SET yahoo_player_id = 42025, sleeper_player_id = 12534 WHERE NFL_player_id = '00-0040236';  -- Kyle Monangai
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41800, sleeper_player_id = 12568 WHERE NFL_player_id = '00-0040577';  -- Jalon Walker
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41918, sleeper_player_id = 12648 WHERE NFL_player_id = '00-0040645';  -- Cody Simon
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41812, sleeper_player_id = 12567 WHERE NFL_player_id = '00-0040688';  -- Malaki Starks
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41816, sleeper_player_id = 12566 WHERE NFL_player_id = '00-0040708';  -- Jihaad Campbell
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41823, sleeper_player_id = 12529 WHERE NFL_player_id = '00-0040734';  -- TreVeyon Henderson
UPDATE nfl_historical.player_bio SET yahoo_player_id = 41834, sleeper_player_id = 12617 WHERE NFL_player_id = '00-0040738';  -- Demetrius Knight Jr.
UPDATE nfl_historical.player_bio SET yahoo_player_id = 28589, sleeper_player_id = 2502 WHERE NFL_player_id = 'HIST-99605491';  -- Bud Sasser
UPDATE nfl_historical.player_bio SET yahoo_player_id = 32894, sleeper_player_id = 6954 WHERE NFL_player_id = 'HIST-99605507';  -- Cole McDonald

-- NOTE: The Travis Hunter cache-map alias (yahoo 99002 -> 00-0040718) lives
-- ONLY on Fly's ___ops.public.yahoo_nfl_player_map. It is NOT replayed here
-- because the local ops_cache.duckdb only carries the nfl_historical schema
-- (see build_ops_cache.py). Adding fixups for tables that don't exist in the
-- local cache would block imports — and ops cache writes must NEVER block an
-- import. See memory feedback_ops_cache_must_not_block_imports.md.


-- ---------------------------------------------------------------
-- 2026-04-29: Franchise lineage gap-fill — Fly-only.
--
-- Closed three contradictions between franchise_history_backup and the
-- self-claimed franchise lineages of the Bears, Lions, and Chiefs.
-- Researched against Pro Football Hall of Fame team-history pages and
-- each franchise's own materials.
--
-- Applied to Fly only via scripts/merge_franchise_history_gaps_2026_04_29.py:
--   1. UPDATE ___ops.nfl_historical.franchise_history_backup
--        SET franchise_id = 5  WHERE franchise_id = 141;   -- Bears absorb Decatur/Chicago Staleys
--   2. UPDATE ___ops.nfl_historical.franchise_history_backup
--        SET franchise_id = 6  WHERE franchise_id = 127;   -- Lions absorb Portsmouth Spartans
--      UPDATE ___ops.nfl_historical.franchise_eras
--        SET franchise_id = 6  WHERE franchise_id = 127;
--   3. UPDATE ___ops.nfl_historical.franchise_history_backup
--        SET end_year = 1952   WHERE franchise_id = 137;   -- clamp 1952 NFL Dallas Texans
--      INSERT (30, 'DTX', 1960, 1962, FALSE)               -- DTX alias to Chiefs
--        INTO ___ops.nfl_historical.franchise_history_backup;
--   4. INSERT (28, 'HOU', 1997, 1998, FALSE)               -- HOU alias to Titans
--        INTO ___ops.nfl_historical.franchise_history_backup;
--      Applied via scripts/add_hou_tennessee_oilers_alias_2026_04_29.py
--      Resolves 918 super_table rows from the Tennessee Oilers years
--      (1997-1998) that were tagged 'HOU' but didn't fall in the canonical
--      HOU range (1960-1996) on franchise_id=28.
--   5. UPDATE 315 super_table rows for player-team identity corrections
--      vs PFR's authoritative team tagging (1920-1999). Applied via
--      scripts/correct_super_table_nfl_team_2026_04_29.py.
--      Examples: Paul Christman 1947 CHI -> CRD (Cardinals not Bears),
--      Bulldog Turner 1947 CRD -> CHI (Bears not Cardinals).
--      228 ambiguous_super_unresolved (CHI Bears/Cardinals) +
--      87 TRUE_IDENTITY_BUG (player on wrong franchise per PFR).
--      Diff sourced from PFR Excel files in
--      fantasy_football_data/cache/pfr_excel/ (15 files, 257K rows,
--      enriched with franchise_id via PFR-aware lookup).
--
--   6. DELETE 13 phantom Raiders DST rows + UPDATE Raiders DST stat cols
--      2003-2019 from unified reference. Applied via
--      scripts/fix_raiders_dst_corruption_2026_04_29.py.
--
--      Phantoms (each = 1 phantom DEF row, 0 supporting player rows):
--        OAK 2003 wk8, 2003 wk19, 2004 wk10/18/19, 2005 wk5, 2006 wk3,
--        2007 wk5, 2010 wk10, 2011 wk8, 2012 wk5, 2013 wk7, 2014 wk5
--      Were bye weeks or non-played POST weeks where super_table had a
--      single DEF row with fake stats. Verified via Tim Brown 2003 wk8
--      cross-check: player rows correctly skip Raiders bye weeks.
--
--      Stat updates ~273 rows × 11 stat cols (points_allowed,
--      total_yds_allowed, passing_yds_allowed, rushing_yds_allowed,
--      passing_tds_allowed, rushing_tds_allowed, def_sacks,
--      def_interceptions, fum_rec, def_safeties, special_teams_tds).
--      Source: fantasy_football_data/cache/pfr_excel/_dst_unified.parquet
--      built from nflverse_dst_regenerated.parquet (1999-2025) + PFR
--      Excel for older years. Example fix: Raiders 2013 wk1 vs IND
--      pts_allowed 18 -> 21 (matches actual 21-17 loss).
--
--      Bug origin: super_table's Raiders DST 2003-2019 was populated from
--      a buggy intermediate source; defense_stats.py's franchise_id
--      self-join produces correct values, but existing rows weren't
--      refreshed. All other 31 franchises were clean per the master
--      schedule audit.
--
--      Downstream: Raiders DST fantasy points + season/career
--      aggregations need recompute (acknowledged out-of-scope here).
--
-- Backups before mutation:
--   ___ops.public.franchise_history_backup_pre_merge_20260429   (Bears/Lions/DTX)
--   ___ops.public.franchise_eras_pre_merge_20260429             (Lions Portsmouth)
--   ___ops.public.franchise_history_backup_pre_hou_alias_20260429 (HOU alias)
--   ___ops.public.super_table_team_correction_backup_20260429   (315 player team rows)
--   ___ops.public.super_table_team_corrections_20260429         (staging)
--   ___ops.public.raiders_dst_correction_backup_20260429        (350 Raiders DEF rows)
--   ___ops.public.raiders_dst_corrections_20260429              (staging, 358 unified-sourced)
--
-- NOT replayed in local cache: build_ops_cache.py copies only
-- nfl_player_stats_all and player_bio; the franchise_history_backup /
-- franchise_eras tables don't exist locally. The Raiders DST stat
-- corrections (item 6) DO target nfl_player_stats_all so they would
-- replay if needed, but the corrections are best applied via Fly UPDATE
-- (already done) — local cache will pick them up on next ops cache
-- rebuild from Fly. Documented here for audit-trail continuity.
-- See feedback_ops_cache_must_not_block_imports.md.


-- ---------------------------------------------------------------
-- 2026-04-29: Player-ID collision fix — 502 super_table rows + 2 bios.
--
-- Master schedule audit on 2026-04-29 surfaced 22 collision cohorts
-- (player, year, nfl_team, position tuples mapping to >1 NFL_player_id)
-- across 4 named players. Two patterns:
--
-- Type A — same person, two IDs (MERGE, drop duplicate bio):
--   Jim McMahon  HIST-43176231 -> 00-0010997   (128 rows; both DOB 1959-08-21, BYU)
--   Ken O'Brien  HIST-87805727 -> 00-0012246   (132 rows; canonical has DOB/college, HIST null)
--
-- Type B — different people, same name (MISTAG, keep both bios intact):
--   Freddie Solomon  00-0015390 -> SOL176699   (160 rows; 1975-85 MIA/SF only)
--     1953-DOB Tampa WR vs 1972-DOB SC State WR. Wrong-ID's 1996-98 PHI
--     rows preserved (legit owner of 00-0015390).
--   Irv Smith        00-0034970 -> 00-0015195  (82 rows; 1993-98 NO/SF only)
--     1971-DOB Notre Dame TE vs 1998-DOB Alabama TE. Wrong-ID's 2019-24
--     MIN/CIN/HOU rows preserved (legit owner of 00-0034970).
--
-- Applied to Fly via scripts/fix_player_id_collisions_2026_04_29.py.
-- Idempotent: each UPDATE pins old NFL_player_id in WHERE clause; a
-- second run finds 0 rows to update. Collision cohorts after fix: 0.
--
-- Backups before mutation:
--   ___ops.public.player_id_collision_backup_20260429        (502 affected rows, full schema)
--   ___ops.public.player_id_collision_corrections_20260429   (4-rule audit summary)
--
-- Replays cleanly on the local ops_cache.duckdb because both target
-- tables (nfl_player_stats_all, player_bio) exist locally:

UPDATE nfl_historical.nfl_player_stats_all
SET NFL_player_id = '00-0010997'
WHERE player = 'Jim McMahon' AND NFL_player_id = 'HIST-43176231';

UPDATE nfl_historical.nfl_player_stats_all
SET NFL_player_id = '00-0012246'
WHERE player = 'Ken O''Brien' AND NFL_player_id = 'HIST-87805727';

UPDATE nfl_historical.nfl_player_stats_all
SET NFL_player_id = 'SOL176699'
WHERE player = 'Freddie Solomon'
  AND NFL_player_id = '00-0015390'
  AND year BETWEEN 1975 AND 1985
  AND nfl_team IN ('MIA', 'SF');

UPDATE nfl_historical.nfl_player_stats_all
SET NFL_player_id = '00-0015195'
WHERE player = 'Irv Smith'
  AND NFL_player_id = '00-0034970'
  AND year BETWEEN 1993 AND 1998
  AND nfl_team IN ('NO', 'SF');

DELETE FROM nfl_historical.player_bio WHERE NFL_player_id = 'HIST-43176231';
DELETE FROM nfl_historical.player_bio WHERE NFL_player_id = 'HIST-87805727';

-- Downstream: aggregations attributing pre-fix rows to the wrong player
-- (e.g. Freddie Solomon's 1975-77 MIA stats credited to the 1972-DOB
-- owner's career bio) need recompute. Acknowledged out-of-scope here;
-- belongs to the broader cascade-recompute work (Pickup 2 in
-- prompt_next_session_2026_04_30_collision_fix_and_cascade.md).


-- ---------------------------------------------------------------
-- 2026-04-29: Phase 1 — Fleet-wide DST stat fix for 1999+ era.
--
-- Audit (audit_super_table_dst_vs_unified_2026_04_29.py) flagged 715 distinct
-- (year, week, nfl_team) DST cohorts in 1999+ with stat disagreements vs
-- _dst_unified.parquet, excluding OAK 2003-2019 (already fixed in the
-- 2026-04-29 Raiders surgical fix). 2 known-bad-opp postseason rows skipped
-- (2001 wk19 GB, wk20 PHI — super phantoms with wrong opp).
--
-- For double-entry bookkeeping consistency: the update was expanded to BOTH
-- sides of every affected game (715 A + 183 new B = 898 rows total). This
-- guarantees that for every deduction applied to A's pts_allowed (because
-- B scored a def or ST TD against A's offense), B's super_table row will
-- also reflect that defensive credit. Verified pre-fix: 69 of 203 deductions
-- referenced opp B rows in super_table that were MISSING the matching
-- def_tds value. Expanded scope eliminates this inconsistency.
--
-- Scope:
--   2001: 442 rows (PFR Excel "01-02 def" trusted over nflverse for these years)
--   2002: 452 rows (same)
--   2018:   2 rows (LAR wk3 + LAC; nflverse)
--   2022:   2 rows (LAR wk17 + LAC; nflverse — the residual pts_allowed=10
--                   should-be-31 case from project_supertable_points_allowed_corruption.md)
--
-- Source-aware DST-eligible pts_allowed derivation:
--   For PFR Excel sources (pts_allowed stored as RAW = opp's team_pts):
--     dst_pa = raw_pa - opp_def_int_tds*7 - opp_def_fum_tds*7
--                     - opp_st_kr_tds*7 - opp_st_pr_tds*7
--                     - opp_def_safeties*2
--   For nflverse_dst_regenerated (pts_allowed already DST-eligible per
--   nflverse semantic — excludes def+ST TDs but NOT safeties):
--     dst_pa = stored_pa - opp_def_safeties*2
--   Cap at 0 (77 pre-1970 data-bug rows had impossible negative deductions
--   in the broader audit; none materialized in the 1999+ scope).
--
-- Validated against golden Super Bowl samples: XLIX, XLII, XLVIII, XX,
-- XXXVII, XXXV. Pivot consistency 99.45% in unified parquet (mismatches
-- all pre-1950, none in 1999+). Bidirectional super_table consistency
-- 96-99.9% across major DST stats in 1999+ era (verified via player-level
-- aggregation cross-check).
--
-- 12 stat columns updated:
--   points_allowed, total_yds_allowed, passing_yds_allowed, rushing_yds_allowed,
--   passing_tds_allowed, rushing_tds_allowed, def_sacks, def_interceptions,
--   fum_rec, def_safeties, def_tds, special_teams_tds
--
-- 7 bracket columns recomputed from new points_allowed:
--   pts_allow_0, pts_allow_1_6, pts_allow_7_13, pts_allow_14_20,
--   pts_allow_21_27, pts_allow_28_34, pts_allow_35_plus
--
-- Idempotent: UPDATE uses COALESCE(c.col, s.col) so NULL in staging preserves
-- super's value (important for def_fum_rec which is 0% populated in the PFR
-- "01-02 def" source). Re-running yields the same end state.
--
-- Applied to Fly via scripts/fix_dst_stats_1999plus_2026_04_29.py --apply.
-- Backups:
--   ___ops.public.dst_1999plus_correction_backup_20260429   (896 pre-fix rows full schema)
--   ___ops.public.dst_1999plus_corrections_20260429         (898 staged corrections)
--
-- Pre-fix global flag count: 8,474   Post-fix: 7,070   (-1,404 flags resolved)
-- Remaining 1999+ flags (228 of original 1,604) are expected raw-vs-DST-eligible
-- offsets (audit script compares super's DST-eligible value to unified's RAW
-- for PFR sources, so the deductions we applied appear as "diffs"). Not real errors.
--
-- Pre-1999 stat fix is a separate phase, deferred — depends on PFR Excel
-- offensive aggregation (super_table's pre-1999 player rows have only 12-19%
-- match for pass_yds vs aggregated team offense) and postseason structural
-- cleanup (super_table missing real playoff games like 1985 SB XX, has
-- phantom rows like 1985 wk19 CHI vs SF that never happened).
--
-- Local cache replay: targets nfl_player_stats_all only; will replay on
-- next ops_cache rebuild from Fly. The bracket recompute is data-driven
-- (CASE on points_allowed), idempotent, and self-corrects.
