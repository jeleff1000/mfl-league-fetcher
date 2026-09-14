-- TFL of Extraordinary Gentleman — 2024 playoff matchup fixture
-- 12 teams, 6 playoff teams, 2 byes (seeds 1-2), weeks 15-17
-- Champion: {51305A7_0 (Ari, seed 2)
-- Runner-up: {9685490_0 (Michael/nicole, seed 4)
-- Seeds 1,2 have byes in week 15 (NULL opponent/points)

CREATE TABLE IF NOT EXISTS matchup (
    year INTEGER,
    week INTEGER,
    franchise_id VARCHAR,
    opponent VARCHAR,
    opponent_franchise_id VARCHAR,
    team_points DOUBLE,
    opponent_points DOUBLE,
    is_playoffs BOOLEAN DEFAULT FALSE,
    is_consolation BOOLEAN DEFAULT FALSE,
    is_championship BOOLEAN DEFAULT FALSE,
    champion INTEGER DEFAULT 0,
    playoff_round VARCHAR DEFAULT '',
    final_playoff_seed INTEGER,
    -- columns that championship tracer must NOT write
    sacko INTEGER DEFAULT 0,
    placement_rank INTEGER DEFAULT 0,
    placement_game INTEGER DEFAULT 0,
    consolation_round VARCHAR DEFAULT '',
    postseason INTEGER DEFAULT 0
);

-- Week 15: Quarterfinals (seeds 3v9, 4v5) + consolation + byes for seeds 1,2
INSERT INTO matchup (year, week, franchise_id, opponent, opponent_franchise_id, team_points, opponent_points, final_playoff_seed) VALUES
-- Byes (seed 1 and 2): NULL opponent, NULL points
(2024, 15, '{2C787F0_0', NULL, NULL, NULL, NULL, 1),
(2024, 15, '{51305A7_0', NULL, NULL, NULL, NULL, 2),
-- Quarterfinal: seed 3 ({36B567E_0) vs seed 9 ({09F1733_0) — seed 9 wins
(2024, 15, '{36B567E_0', 'Max', '{09F1733_0', 90.24, 114.46, 3),
(2024, 15, '{09F1733_0', 'Joseph', '{36B567E_0', 114.46, 90.24, 9),
-- Quarterfinal: seed 4 ({9685490_0) vs seed 5 ({0828C62_0) — seed 4 wins
(2024, 15, '{9685490_0', 'Michael', '{0828C62_0', 145.58, 141.6, 4),
(2024, 15, '{0828C62_0', 'nicole', '{9685490_0', 141.6, 145.58, 5),
-- Consolation games week 15
(2024, 15, '{1AE27F0_0', 'Avigayil', '{3A7EB1D_0', 63.54, 127.44, 6),
(2024, 15, '{3A7EB1D_0', 'eytan', '{1AE27F0_0', 127.44, 63.54, 7),
(2024, 15, '{28109AF_0', 'Gavriel', '{122E295_0', 105.06, 133.22, 8),
(2024, 15, '{122E295_0', 'Joel', '{28109AF_0', 133.22, 105.06, 10),
(2024, 15, '{1B323A0_0', 'josh', '{78C33E2_0', 100.5, 149.56, 11),
(2024, 15, '{78C33E2_0', 'Matthew', '{1B323A0_0', 149.56, 100.5, 12);

-- Week 16: Semifinals (1 vs winner-of-4v5, 2 vs winner-of-3v9) + consolation
INSERT INTO matchup (year, week, franchise_id, opponent, opponent_franchise_id, team_points, opponent_points, final_playoff_seed) VALUES
-- Semifinal: seed 1 ({2C787F0_0) vs seed 4 ({9685490_0) — seed 4 wins (upset!)
(2024, 16, '{2C787F0_0', 'nicole', '{9685490_0', 138.98, 139.76, 1),
(2024, 16, '{9685490_0', 'Jared', '{2C787F0_0', 139.76, 138.98, 4),
-- Semifinal: seed 2 ({51305A7_0) vs seed 9 ({09F1733_0) — seed 2 wins
(2024, 16, '{51305A7_0', 'Max', '{09F1733_0', 145.64, 109.42, 2),
(2024, 16, '{09F1733_0', 'Ari', '{51305A7_0', 109.42, 145.64, 9),
-- Consolation games week 16
(2024, 16, '{36B567E_0', 'Michael', '{0828C62_0', 126.32, 129.94, 3),
(2024, 16, '{0828C62_0', 'Joseph', '{36B567E_0', 129.94, 126.32, 5),
(2024, 16, '{1AE27F0_0', 'josh', '{78C33E2_0', 99.84, 169.6, 6),
(2024, 16, '{3A7EB1D_0', 'Gavriel', '{28109AF_0', 99.9, 134.18, 7),
(2024, 16, '{28109AF_0', 'Matthew', '{1B323A0_0', 87.48, 95.12, 8),
(2024, 16, '{122E295_0', 'Avigayil', '{3A7EB1D_0', 134.18, 99.9, 10),
(2024, 16, '{1B323A0_0', 'Joel', '{28109AF_0', 95.12, 87.48, 11),
(2024, 16, '{78C33E2_0', 'eytan', '{1AE27F0_0', 169.6, 99.84, 12);

-- Week 17: Championship + consolation finals
INSERT INTO matchup (year, week, franchise_id, opponent, opponent_franchise_id, team_points, opponent_points, final_playoff_seed) VALUES
-- Championship: seed 2 ({51305A7_0) vs seed 4 ({9685490_0) — seed 2 wins!
(2024, 17, '{51305A7_0', 'nicole', '{9685490_0', 134.98, 109.48, 2),
(2024, 17, '{9685490_0', 'Ari', '{51305A7_0', 109.48, 134.98, 4),
-- 3rd place + consolation games
(2024, 17, '{2C787F0_0', 'Max', '{09F1733_0', 166.58, 127.06, 1),
(2024, 17, '{09F1733_0', 'Jared', '{2C787F0_0', 127.06, 166.58, 9),
(2024, 17, '{36B567E_0', 'Michael', '{0828C62_0', 94.28, 89.7, 3),
(2024, 17, '{0828C62_0', 'Joseph', '{36B567E_0', 89.7, 94.28, 5),
(2024, 17, '{1AE27F0_0', 'Joel', '{28109AF_0', 79.78, 104.5, 6),
(2024, 17, '{28109AF_0', 'eytan', '{1AE27F0_0', 104.5, 79.78, 8),
(2024, 17, '{3A7EB1D_0', 'Matthew', '{78C33E2_0', 109.2, 96.26, 7),
(2024, 17, '{78C33E2_0', 'Gavriel', '{3A7EB1D_0', 96.26, 109.2, 12),
(2024, 17, '{122E295_0', 'josh', '{1B323A0_0', 85.7, 143.2, 10),
(2024, 17, '{1B323A0_0', 'Avigayil', '{122E295_0', 143.2, 85.7, 11);
