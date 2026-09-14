-- Idempotent ALTER TABLE for new pts_def_* columns.

ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_int_ret_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_fum_ret_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_blk_kick_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_kr_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_pr_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_st_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_pass_def DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_sack_yd DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_2pt DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_st_ff DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_st_fum_rec DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_fum_rec_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_bonus_sack_2p DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_bonus_tkl_10p DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_bonus_int_td_50p DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_bonus_fum_td_50p DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_def_forced_punts DOUBLE;

-- IDP advanced columns (Task 5 audit gap fix)
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_blk_kick DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_blk_kick_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_fum_rec_yd DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_fum_ret_td DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_int_ret_yd DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_xpr DOUBLE;
ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_pass_def_3p DOUBLE;
