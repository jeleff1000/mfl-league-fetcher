"""Measure source-only neighboring-layout and slug-identity candidates.

Neighbor transfer is accepted only when adjacent weeks expose exactly one recovered
offensive layout.  Slug identity is measured independently: it identifies the player,
but does not by itself choose an RBFB5 versus WRTE layout.  No v26 values are consulted.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .nflcom_player_logs_capture_inventory import (
    PLAYER_LOG_BLOCK_RECOVERY,
    RAW_VALUE_COLUMNS,
    SHARED_SCHEMA_TIE,
)
from .sources import NFLCOM_SLUG_PFRID, PLAYER_BIO, PFR_PLAYER_INDEX


RAW = Path(r"D:/league-history-data/nfl/raw/nflcom/tables/player_logs")
PFR_PLAYER_TABLES = Path(r"D:/league-history-data/nfl/raw/pfr/players/tables")
OUT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_neighbor_identity_diagnostic.json"
)

# Historical NFL.com display slugs absent from the local slug->PFR bridge. These are
# candidate aliases only; they do not mutate the bridge or license a witness.
HISTORICAL_SLUG_ALIASES = (
    ("tommy-harmon", "Tom Harmon"),
    ("crazy-legs-hirsch", "Elroy Hirsch"),
    ("billie-cross", "Billy Cross"),
    ("j-r-boone", "J.R. Boone"),
    ("j-d-smith-3", "J.D. Smith"),
    ("george-magulick", "George Magulick"),
    ("bill-decorrevont", "Billy deCorrevont"),
    ("billy-barnes", "Billy Ray Barnes"),
    ("skeets-quinlan", "Skeet Quinlan"),
    ("whizzer-white", "Wilford White"),
    ("rip-ryan", "Kent Ryan"),
    ("johnny-blood-mcnally", "Johnny Blood"),
    ("tom-kalmanir", "Tommy Kalmanir"),
    ("hopalong-cassady", "Howard Cassady"),
    ("franny-murray", "Fran Murray"),
    ("penny-hart", "Penny Hart"),
    ("art-whittington", "Arthur Whittington"),
    ("raghib-ismail", "Rocket Ismail"),
    ("eddie-rucinski", "Ed Rucinski"),
    ("gus-tinsley", "Gaynell Tinsley"),
    ("choo-choo-roberts", "Gene Roberts"),
    ("jim-hadnot", "James Hadnot"),
    ("benjamin-gay", "Ben Gay"),
    ("al-dotson", "Alphonse Dotson"),
    ("al-pastrana", "Alan Pastrana"),
    ("angie-brovelli", "Angelo Brovelli"),
    ("benny-lapresta", "Bennie LaPresta"),
    ("abdul-karim-al-jabbar", "Karim Abdul-Jabbar"),
    ("bill-dewell", "Billy Dewell"),
    ("bobby-clatterbuck", "Bob Clatterbuck"),
    ("bobby-gage", "Bob Gage"),
    ("bronko-smilanich", "Branko Smilanich"),
    ("cameron-cleeland", "Cam Cleeland"),
    ("dave-hollis", "David Hollis"),
    ("duke-iverson", "Duke Iversen"),
    ("ed-meador", "Eddie Meador"),
    ("eddie-crawford", "Ed Crawford"),
    ("freddie-hyatt", "Fred Hyatt"),
    ("hamp-pool", "Hampton Pool"),
    ("heinie-weisenbaugh", "Heinie Wiesenbaugh"),
    ("howie-maley", "Howard Maley"),
    ("ike-peterson", "Ike Petersen"),
    ("j-b-andrews", "Jaby Andrews"),
    ("lloyd-lowe", "Loyd Lowe"),
    ("marv-upshaw", "Marvin Upshaw"),
    ("mike-mcgruder", "Michael McGruder"),
    ("mo-bassett", "Maurice Bassett"),
    ("norman-mosley", "Norm Mosley"),
    ("olie-cordill", "Ollie Cordill"),
    ("phil-spiller", "Philip Spiller"),
    ("randy-minnear", "Randy Minniear"),
    ("reg-carolan", "Reggie Carolan"),
    ("rob-awalt", "Robert Awalt"),
    ("scott-vines", "Scottie Vines"),
    ("thurman-jones", "Thurmon Jones"),
    ("tilly-manton", "Tillie Manton"),
    ("tom-stephens", "Thomas Stephens"),
    ("tommy-addison", "Tom Addison"),
    ("wally-francis", "Wallace Francis"),
    ("walter-williams-2", "Walt Williams"),
    ("jerry-allen", "Gerry Allen"),
    ("mike-clemons", "Michael Clemons"),
    ("chuck-malone", "Charley Malone"),
    ("ed-conti", "Enio Conti"),
    ("jack-tracey", "John Tracey"),
    ("johnny-carson", "John Carson"),
    ("special-delivery-jones", "Edgar Jones"),
    ("tom-henderson", "Thomas Henderson"),
    ("deac-sanders", "John Sanders"),
)

# Explicit PFR-ID aliases are used where the NFL.com display slug contains a
# disambiguating middle name but the PFR display name is shared by multiple players.
# They remain source-only candidates; no bridge row is written.
HISTORICAL_SLUG_PFR_ALIASES = (
    ("chris-david-thompson", "ThomCh03"),
    ("brandon-markieth-marshall", "MarsBr01"),
    ("phillip-walker", "WalkPh00"),
    ("bill-gay", "GayxWi20"),
    ("deaundre-ford", "AlfoDe00"),
)

# Source-page position headers for the remaining multi-candidate slugs. These were
# fetched from the corresponding NFL.com player pages and are used only as a
# collision discriminator; they never create an identity by themselves.
HISTORICAL_SLUG_PAGE_POSITIONS = (
    ("al-johnson-3", "QB"),
    ("austin-bryant", "DE"),
    ("chuck-evans", "RB"),
    ("gary-anderson-3", "RB"),
    ("j-r-boone", "HB"),
    ("derek-brown", "RB"),
    ("bronko-smilanich", "HB"),
    ("j-d-smith-3", "WR"),
    ("bill-decorrevont", "HB"),
    ("kyle-williams-2", "WR"),
    ("bruce-davis-2", "WR"),
    ("bobby-johnson", "WR"),
    ("j-d-smith", "FB"),
    ("richard-johnson", "WR"),
    ("brandon-williams-6", "WR"),
    ("john-jackson", "WR"),
    ("willie-brown", "WR"),
    ("chris-williams-2", "WR"),
    ("dexter-jackson-2", "WR"),
    ("j-j-jones", "WR"),
    ("marcus-thomas-2", "RB"),
    ("bob-harrison", "DB"),
    ("bob-smith-3", "FB"),
    ("bobby-smith", "HB"),
    ("brian-smith", "LB"),
    ("byron-young", "DT"),
    ("c-j-wilson", "DB"),
    ("crazy-legs-hirsch", "E"),
    ("charlie-brown-3", "RB"),
    ("chris-clemons", "DB"),
    ("chris-sanders", "TE"),
    ("chuck-hinton", "C"),
    ("chuck-reynolds", "C"),
    ("dave-tipton", "DT"),
    ("david-long", "ILB"),
    ("doug-smith", "DB"),
    ("ed-bradley", "OLB"),
    ("ezell-jones", "OT"),
    ("j-t-thomas-3", "S"),
    ("j-b-andrews", "HB"),
    ("jack-tracey", "OLB"),
    ("jack-sommers", "C"),
    ("james-williams-4", "DB"),
    ("jim-jones-3", "DB"),
    ("jim-mitchell", "DE"),
    ("jimmy-robinson-2", "WR"),
    ("joe-jones", "LB"),
    ("jonah-williams", "OT"),
    ("justin-anderson", "LB"),
    ("mark-thomas", "TE"),
    ("malik-reed", "LB"),
    ("michael-mitchell", "DE"),
    ("michael-thomas", "WR"),
    ("mike-harris", "DB"),
    ("mike-kelley", "QB"),
    ("n-d-kalu", "DE"),
    ("olie-cordill", "DB"),
    ("randy-jackson", "RB"),
    ("reggie-brown-4", "OLB"),
    ("robert-woods-2", "WR"),
    ("ron-brown-2", "WR"),
    ("ron-heller", "TE"),
    ("ron-smith-4", "QB"),
    ("ryan-griffin", "QB"),
    ("steve-griffin", "WR"),
    ("steve-jordan", "K"),
    ("steve-smith-2", "WR"),
    ("t-j-carter", "DE"),
    ("tim-ryan", "G"),
    ("todd-collins", "QB"),
    ("tony-woods", "DT"),
    ("victor-jones", "RB"),
    ("wayne-davis", "LB"),
    ("wayne-walker-2", "K"),
)

# Source-only identity resolutions from the NFL.com career table's season/team
# rows, cross-checked against the local PFR index/bio. These never mutate the
# nflcom_slug_pfrid bridge.
HISTORICAL_SLUG_IDENTITY_OVERRIDES = (
    ("j-r-boone", "NFL:BoonJ.20"),
    ("j-d-smith-3", "PFR:SmitJ.00"),
    ("bill-decorrevont", "PFR:deCoBi20"),
    ("j-d-smith", "PFR:SmitJ.01"),
    ("john-jackson", "PFR:JackJo00"),
    ("chris-williams-2", "PFR:WillCh06"),
    ("bob-smith-3", "PFR:SmitBo22"),
    ("brian-smith", "PFR:SmitBr02"),
    ("charlie-brown-3", "PFR:BrowCh02"),
    ("chris-sanders", "PFR:SandCh20"),
    ("dave-tipton", "PFR:TiptDa20"),
    ("ed-bradley", "PFR:BradEd21"),
    ("ezell-jones", "PFR:JoneEz20"),
    ("j-t-thomas-3", "NFL:ThomJ.01"),
    ("jack-sommers", "PFR:SommJa20"),
    ("james-williams-4", "PFR:WillJa23"),
    ("jim-mitchell", "PFR:MitcJi20"),
    ("n-d-kalu", "NFL:KaluN.20"),
    ("randy-jackson", "PFR:JackRa00"),
    ("reggie-brown-4", "PFR:BrowRe22"),
    ("ron-brown-2", "PFR:BrowRo23"),
    ("steve-griffin", "PFR:GrifSt22"),
    ("steve-smith-2", "PFR:SmitSt02"),
    ("t-j-carter", "NFL:00-0035916"),
    ("tony-woods", "PFR:WoodTo21"),
    ("wayne-walker-2", "PFR:WalkWa20"),
)

# The remaining page-position-only candidates are retained separately so the
# receipt can distinguish them from the career-table resolutions above.
HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES = (
    ("chuck-evans", "PFR:EvanCh00"),
    ("gary-anderson-3", "PFR:AndeGa00"),
    ("derek-brown", "PFR:BrowDe01"),
    ("kyle-williams-2", "PFR:WillKy01"),
    ("bruce-davis-2", "PFR:DaviBr22"),
    ("bobby-johnson", "PFR:JohnBo00"),
    ("richard-johnson", "PFR:JohnRi00"),
    ("brandon-williams-6", "PFR:WillBr01"),
    ("willie-brown", "PFR:BrowWi00"),
    ("dexter-jackson-2", "PFR:JackDe01"),
    ("j-j-jones", "PFR:JoneJJ00"),
    ("marcus-thomas-2", "PFR:ThomMa01"),
    ("bob-harrison", "PFR:HarrBo22"),
    ("bobby-smith", "PFR:SmitBo00"),
    ("byron-young", "PFR:YounBy00"),
    ("c-j-wilson", "PFR:WilsC.00"),
    ("chris-clemons", "PFR:ClemCh97"),
    ("chuck-hinton", "PFR:HintCh21"),
    ("chuck-reynolds", "PFR:ReynCh20"),
    ("david-long", "PFR:LongDa04"),
    ("doug-smith", "PFR:SmitDo26"),
    ("jim-jones-3", "PFR:JoneJi21"),
    ("jimmy-robinson-2", "NFL:ROB523624"),
    ("joe-jones", "PFR:JoneJo05"),
    ("jonah-williams", "PFR:WillJo10"),
    ("justin-anderson", "PFR:AndeJu01"),
    ("mark-thomas", "PFR:ThomMa21"),
    ("michael-mitchell", "NFL:MIT561200"),
    ("michael-thomas", "PFR:ThomMi05"),
    ("mike-harris", "PFR:HarrMi00"),
    ("mike-kelley", "PFR:KellMi21"),
    ("robert-woods-2", "PFR:WoodRo00"),
    ("ron-heller", "PFR:HellRo00"),
    ("ron-smith-4", "PFR:SmitRo03"),
    ("ryan-griffin", "PFR:GrifRy01"),
    ("steve-jordan", "PFR:jordaste01"),
    ("tim-ryan", "PFR:RyanTi21"),
    ("todd-collins", "PFR:CollTo00"),
    ("victor-jones", "PFR:JoneVi20"),
    ("wayne-davis", "PFR:DaviWa21"),
)


def _q(path: Path) -> str:
    return str(path).replace("'", "''")


def _pfr_player_tables_sql() -> str:
    files = sorted(PFR_PLAYER_TABLES.glob("**/_combined.parquet"))
    if not files:
        raise FileNotFoundError(f"No combined PFR player tables under {PFR_PLAYER_TABLES}")
    return "[" + ",".join("'" + _q(path) + "'" for path in files) + "]"


def _fumble_only_predicate(alias: str = "r") -> str:
    other = [
        column
        for column in RAW_VALUE_COLUMNS
        if column not in {"fum", "lost", "fumbles", "fumbles_lost"}
    ]
    return " AND ".join(
        f'TRY_CAST({alias}."{column}" AS DOUBLE) IS NULL' for column in other
    )


def _raw_cte() -> str:
    return f"""
WITH raw AS (
    SELECT row_number() OVER () AS raw_row_id, *,
           TRY_CAST(wk AS INTEGER) AS week_num,
           ({PLAYER_LOG_BLOCK_RECOVERY}) AS direct_layout
    FROM parquet_scan('{_q(RAW)}/*.parquet', union_by_name=true)
    WHERE _view='logs'
), shared AS (
    SELECT * FROM raw
    WHERE direct_layout IS NULL AND ({SHARED_SCHEMA_TIE})
), fumble_only AS (
    SELECT * FROM raw
    WHERE direct_layout IS NULL
      AND TRY_CAST(fum AS DOUBLE) IS NOT NULL
      AND TRY_CAST(lost AS DOUBLE) IS NOT NULL
      AND ({_fumble_only_predicate('raw')})
)
"""


def _neighbor_query() -> str:
    return _raw_cte() + """
, neighbor_labels AS (
    SELECT s.raw_row_id,
           COUNT(*) AS neighbor_rows,
           COUNT(DISTINCT n.direct_layout) AS neighbor_layout_n,
           MIN(n.direct_layout) AS neighbor_layout
    FROM shared s
    JOIN raw n
      ON n.nflcom_slug=s.nflcom_slug
     AND n.season=s.season
     AND n._table=s._table
     AND n.week_num IS NOT NULL
     AND s.week_num IS NOT NULL
     AND ABS(n.week_num-s.week_num)=1
     AND n.direct_layout IN ('RBFB5','WRTE')
    GROUP BY s.raw_row_id
)
SELECT
  s._table,
  COALESCE(n.neighbor_layout, 'NO_UNIQUE_LAYOUT') AS candidate_layout,
  COUNT(*) AS shared_rows,
  COUNT(*) FILTER (WHERE n.neighbor_rows IS NOT NULL) AS rows_with_neighbor,
  COUNT(*) FILTER (WHERE n.neighbor_layout_n=1) AS unique_neighbor_rows,
  COUNT(*) FILTER (WHERE n.neighbor_layout_n>1) AS conflicting_neighbor_rows
FROM shared s
LEFT JOIN neighbor_labels n USING (raw_row_id)
GROUP BY 1,2 ORDER BY 1,2
"""


def _identity_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           MIN(b.NFL_player_id) AS NFL_player_id
    FROM read_parquet('{slug_path}') x
    JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), populations AS (
    SELECT 'shared_layout_tie' AS population, nflcom_slug FROM shared
    UNION ALL
    SELECT 'fumble_only' AS population, nflcom_slug FROM fumble_only
), classified AS (
    SELECT p.population,
           CASE
             WHEN i.player_id_n=1 THEN 'UNIQUE_PLAYER_ID'
             WHEN i.player_id_n>1 THEN 'COLLIDING_PLAYER_ID'
             ELSE 'NO_PLAYER_ID'
           END AS identity_status,
           COUNT(*) AS rows
    FROM populations p
    LEFT JOIN ident i USING (nflcom_slug)
    GROUP BY 1,2
)
SELECT population, identity_status, rows
FROM classified ORDER BY 1,2
"""


def _position_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), populations AS (
    SELECT 'all_logs' AS population, raw_row_id, direct_layout, nflcom_slug FROM raw
    UNION ALL
    SELECT 'shared_layout_tie', raw_row_id, direct_layout, nflcom_slug FROM shared
    UNION ALL
    SELECT 'fumble_only', raw_row_id, direct_layout, nflcom_slug FROM fumble_only
), labeled AS (
    SELECT p.population, p.direct_layout, i.player_id_n, i.position_n,
           i.position_label,
           CASE
             WHEN i.position_label IN ('RB','FB','B') THEN 'RBFB5'
             WHEN i.position_label IN ('WR','TE') THEN 'WRTE'
             ELSE NULL
           END AS position_layout
    FROM populations p
    LEFT JOIN ident i USING (nflcom_slug)
)
SELECT population,
       CASE
         WHEN player_id_n=1 AND position_n=1 AND position_layout IS NOT NULL
           THEN 'UNIQUE_ID_CLEAN_LAYOUT_POSITION'
         WHEN player_id_n=1 AND position_n=1 THEN 'UNIQUE_ID_CLEAN_OTHER_POSITION'
         WHEN player_id_n=1 AND position_n=0 THEN 'UNIQUE_ID_NO_POSITION'
         WHEN player_id_n=1 AND position_n>1 THEN 'UNIQUE_ID_MULTI_POSITION'
         WHEN player_id_n>1 THEN 'COLLIDING_PLAYER_ID'
         ELSE 'NO_PLAYER_ID'
       END AS status,
       COUNT(*) AS rows
FROM labeled
GROUP BY 1,2 ORDER BY 1,2
"""


def _position_layout_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), labeled AS (
    SELECT s._table, i.player_id_n, i.position_n, i.position_label,
           CASE
             WHEN i.position_label IN ('RB','FB','B') THEN 'RBFB5'
             WHEN i.position_label IN ('WR','TE') THEN 'WRTE'
             ELSE NULL
           END AS position_layout
    FROM shared s LEFT JOIN ident i USING (nflcom_slug)
)
SELECT _table, COALESCE(position_label, '<NO_POSITION>') AS position_label,
       COALESCE(position_layout, 'NO_LAYOUT_CANDIDATE') AS position_layout,
       COUNT(*) AS rows
FROM labeled
GROUP BY 1,2,3 ORDER BY 1,3,4 DESC,2
"""


def _position_calibration_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), labeled AS (
    SELECT r.direct_layout, i.player_id_n, i.position_n,
           CASE
             WHEN i.position_label IN ('RB','FB','B') THEN 'RBFB5'
             WHEN i.position_label IN ('WR','TE') THEN 'WRTE'
             ELSE NULL
           END AS position_layout
    FROM raw r LEFT JOIN ident i USING (nflcom_slug)
)
SELECT direct_layout,
       COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE player_id_n=1 AND position_n=1) AS clean_position_rows,
       COUNT(*) FILTER (WHERE player_id_n=1 AND position_n=1
                              AND position_layout=direct_layout) AS agree_rows,
       COUNT(*) FILTER (WHERE player_id_n=1 AND position_n=1
                              AND position_layout IS NOT NULL
                              AND position_layout<>direct_layout) AS disagree_rows
FROM labeled
WHERE direct_layout IN ('RBFB5','WRTE')
GROUP BY 1 ORDER BY 1
"""


def _player_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.NFL_player_id) AS NFL_player_id,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), active AS (
    SELECT DISTINCT nflcom_slug FROM raw
), labeled AS (
    SELECT a.nflcom_slug, i.player_id_n, i.position_n, i.NFL_player_id,
           CASE
             WHEN i.player_id_n=1 AND i.position_n=1
                  AND i.position_label IN ('RB','FB','B','WR','TE')
               THEN 'UNIQUE_ID_CLEAN_LAYOUT_POSITION'
             WHEN i.player_id_n=1 AND i.position_n=1
               THEN 'UNIQUE_ID_CLEAN_OTHER_POSITION'
             WHEN i.player_id_n=1 AND i.position_n=0
               THEN 'UNIQUE_ID_NO_POSITION'
             WHEN i.player_id_n>1 THEN 'COLLIDING_PLAYER_ID'
             ELSE 'NO_PLAYER_ID'
           END AS status
    FROM active a LEFT JOIN ident i USING (nflcom_slug)
)
SELECT status, COUNT(*) AS slug_count,
       COUNT(DISTINCT NFL_player_id) FILTER (WHERE player_id_n=1) AS unique_nfl_player_ids
FROM labeled
GROUP BY 1 ORDER BY 1
"""


def _position_neighbor_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), neighbor_labels AS (
    SELECT s.raw_row_id,
           COUNT(DISTINCT n.direct_layout) AS neighbor_layout_n,
           MIN(n.direct_layout) AS neighbor_layout
    FROM shared s
    JOIN raw n
      ON n.nflcom_slug=s.nflcom_slug
     AND n.season=s.season
     AND n._table=s._table
     AND n.week_num IS NOT NULL
     AND s.week_num IS NOT NULL
     AND ABS(n.week_num-s.week_num)=1
     AND n.direct_layout IN ('RBFB5','WRTE')
    GROUP BY s.raw_row_id
), labeled AS (
    SELECT s.raw_row_id,
           CASE
             WHEN i.player_id_n=1 AND i.position_n=1
                  AND i.position_label IN ('RB','FB','B') THEN 'RBFB5'
             WHEN i.player_id_n=1 AND i.position_n=1
                  AND i.position_label IN ('WR','TE') THEN 'WRTE'
             ELSE NULL
           END AS position_layout,
           n.neighbor_layout_n, n.neighbor_layout
    FROM shared s
    LEFT JOIN ident i USING (nflcom_slug)
    LEFT JOIN neighbor_labels n USING (raw_row_id)
)
SELECT CASE
         WHEN position_layout IS NULL THEN 'NO_CLEAN_LAYOUT_POSITION'
         WHEN neighbor_layout_n=1 AND position_layout=neighbor_layout
           THEN 'POSITION_AND_NEIGHBOR_AGREE'
         WHEN neighbor_layout_n=1 AND position_layout<>neighbor_layout
           THEN 'POSITION_NEIGHBOR_CONFLICT'
         ELSE 'POSITION_WITHOUT_UNIQUE_NEIGHBOR'
       END AS status,
       COUNT(*) AS rows
FROM labeled
GROUP BY 1 ORDER BY 1
"""


def _shared_resolution_query() -> str:
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    index_path = _q(Path(PFR_PLAYER_INDEX.path))
    alias_values = ", ".join(
        "(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")"
        for row in HISTORICAL_SLUG_ALIASES
    )
    return _raw_cte() + f"""
, ident AS (
    SELECT x.nflcom_slug,
           COUNT(DISTINCT b.NFL_player_id) AS player_id_n,
           COUNT(DISTINCT b.nfl_position) AS position_n,
           MIN(b.nfl_position) AS position_label
    FROM read_parquet('{slug_path}') x
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=x.pfr_id
    GROUP BY 1
), season_direct AS (
    SELECT nflcom_slug, season, _table,
           COUNT(DISTINCT direct_layout) AS direct_layout_n,
           MIN(direct_layout) AS direct_layout
    FROM raw
    WHERE direct_layout IN ('RBFB5','WRTE')
    GROUP BY 1,2,3
), pfr_index AS (
    SELECT pfr_id, MIN(index_position) AS index_position
    FROM read_parquet('{index_path}')
    GROUP BY 1
), historical_aliases(nflcom_slug, alias_name) AS (
    VALUES {alias_values}
), alias_identity AS (
    SELECT h.nflcom_slug,
           MAX(pi.index_position) AS index_position,
           MAX(b.nfl_position) AS bio_position
    FROM historical_aliases h
    LEFT JOIN read_parquet('{index_path}') pi
      ON lower(pi.player)=lower(h.alias_name)
    LEFT JOIN read_parquet('{bio_path}') b
      ON lower(b.player)=lower(h.alias_name)
    GROUP BY 1
), active_names AS (
    SELECT DISTINCT nflcom_slug,
           regexp_replace(
             regexp_replace(lower(nflcom_slug), '-[0-9]+$', ''),
             '[^a-z0-9]', '', 'g'
           ) AS normalized_slug
    FROM raw
), exact_bio_identity AS (
    SELECT a.nflcom_slug,
           CASE MAX(
             CASE
               WHEN b.nfl_position IN ('RB','FB','B') THEN 2
               WHEN b.nfl_position IN ('WR','TE') THEN 1
               ELSE 0
             END
           )
             WHEN 2 THEN 'RBFB5'
             WHEN 1 THEN 'WRTE'
             ELSE NULL
           END AS bio_name_layout
    FROM active_names a
    JOIN read_parquet('{bio_path}') b
      ON regexp_replace(lower(b.player), '[^a-z0-9]', '', 'g')=a.normalized_slug
    GROUP BY 1
), labeled AS (
    SELECT s._table, s.raw_row_id, d.direct_layout_n, d.direct_layout,
           pi.index_position,
           ai.index_position AS alias_index_position,
           ai.bio_position AS alias_bio_position,
           ei.bio_name_layout,
           CASE
             WHEN i.player_id_n=1 AND i.position_n=1
                  AND i.position_label IN ('RB','FB','B') THEN 'RBFB5'
             WHEN i.player_id_n=1 AND i.position_n=1
                  AND i.position_label IN ('WR','TE') THEN 'WRTE'
             ELSE NULL
           END AS position_layout
    FROM shared s
    LEFT JOIN ident i USING (nflcom_slug)
    LEFT JOIN season_direct d USING (nflcom_slug, season, _table)
    LEFT JOIN read_parquet('{slug_path}') sm USING (nflcom_slug)
    LEFT JOIN pfr_index pi USING (pfr_id)
    LEFT JOIN alias_identity ai USING (nflcom_slug)
    LEFT JOIN exact_bio_identity ei USING (nflcom_slug)
), resolved AS (
    SELECT *,
           CASE
             WHEN regexp_matches(index_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
               THEN 'RBFB5'
             WHEN regexp_matches(index_position, '(^|[-])WB([-]|$)') THEN 'WRTE'
             WHEN regexp_matches(alias_index_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
                  OR alias_bio_position IN ('RB','FB','B') THEN 'RBFB5'
             WHEN regexp_matches(alias_index_position, '(^|[-])WB([-]|$)')
                  OR alias_bio_position IN ('WR','TE') THEN 'WRTE'
             WHEN bio_name_layout IS NOT NULL THEN bio_name_layout
             ELSE NULL
           END AS pfr_index_layout,
           CASE
             WHEN direct_layout_n=1 THEN direct_layout
             WHEN direct_layout_n=2 AND position_layout IS NOT NULL THEN position_layout
             WHEN direct_layout_n IS NULL AND position_layout IS NOT NULL THEN position_layout
             WHEN regexp_matches(index_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
               THEN 'RBFB5'
             WHEN regexp_matches(index_position, '(^|[-])WB([-]|$)') THEN 'WRTE'
             WHEN regexp_matches(alias_index_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
                  OR alias_bio_position IN ('RB','FB','B') THEN 'RBFB5'
             WHEN regexp_matches(alias_index_position, '(^|[-])WB([-]|$)')
                  OR alias_bio_position IN ('WR','TE') THEN 'WRTE'
             WHEN bio_name_layout IS NOT NULL THEN bio_name_layout
             ELSE NULL
           END AS resolved_layout,
           CASE
             WHEN direct_layout_n=1 THEN 'SEASON_DIRECT_SIGNATURE'
             WHEN direct_layout_n=2 AND position_layout IS NOT NULL
               THEN 'POSITION_SELECTS_MIRROR_LAYOUT'
             WHEN direct_layout_n IS NULL AND position_layout IS NOT NULL
               THEN 'POSITION_WITHOUT_DIRECT_SIGNATURE'
             WHEN regexp_matches(index_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
               OR regexp_matches(index_position, '(^|[-])WB([-]|$)')
               THEN 'PFR_INDEX_POSITION'
             WHEN alias_index_position IS NOT NULL OR alias_bio_position IS NOT NULL
               THEN 'PFR_ALIAS_POSITION'
             WHEN bio_name_layout IS NOT NULL THEN 'BIO_NAME_POSITION'
             ELSE 'PENDING_NO_UNIQUE_LAYOUT_EVIDENCE'
           END AS resolution_source
    FROM labeled
)
SELECT resolution_source,
       COALESCE(resolved_layout, 'PENDING') AS resolved_layout,
       COUNT(*) AS rows
FROM resolved
GROUP BY 1,2 ORDER BY 1,2
"""


def _no_bridge_identity_query() -> str:
    """Return season-aware identity candidates for every active no-ID slug.

    This is deliberately a candidate queue, not a bridge mutation.  A slug can still
    have a bridge row whose PFR ID fails to resolve to player_bio, so the denominator is
    defined by the same bio-backed identity test as ``_player_query`` rather than by
    bridge-row absence alone.
    """
    slug_path = _q(Path(NFLCOM_SLUG_PFRID.path))
    bio_path = _q(Path(PLAYER_BIO.path))
    index_path = _q(Path(PFR_PLAYER_INDEX.path))
    player_tables = _pfr_player_tables_sql()
    alias_values = ", ".join(
        "(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")"
        for row in HISTORICAL_SLUG_ALIASES
    )
    pfr_alias_values = ", ".join(
        "(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")"
        for row in HISTORICAL_SLUG_PFR_ALIASES
    )
    page_position_values = ", ".join(
        "(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")"
        for row in HISTORICAL_SLUG_PAGE_POSITIONS
    )
    identity_override_rows = [
        (*row, "CAREER_TABLE") for row in HISTORICAL_SLUG_IDENTITY_OVERRIDES
    ] + [
        (*row, "PAGE_POSITION_ONLY")
        for row in HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES
    ]
    identity_override_values = ", ".join(
        "(" + ", ".join("'" + value.replace("'", "''") + "'" for value in row) + ")"
        for row in identity_override_rows
    )
    return _raw_cte() + f"""
, neighbor_labels AS (
    SELECT s.raw_row_id,
           COUNT(DISTINCT n.direct_layout) AS neighbor_layout_n,
           MIN(n.direct_layout) AS neighbor_layout
    FROM shared s
    JOIN raw n
      ON n.nflcom_slug=s.nflcom_slug
     AND n.season=s.season
     AND n._table=s._table
     AND n.week_num IS NOT NULL
     AND s.week_num IS NOT NULL
     AND ABS(n.week_num-s.week_num)=1
     AND n.direct_layout IN ('RBFB5','WRTE')
    GROUP BY s.raw_row_id
), ident AS (
    SELECT a.nflcom_slug, COUNT(DISTINCT b.NFL_player_id) AS player_id_n
    FROM (SELECT DISTINCT nflcom_slug FROM raw) a
    LEFT JOIN read_parquet('{slug_path}') br USING (nflcom_slug)
    LEFT JOIN read_parquet('{bio_path}') b ON b.pfr_id=br.pfr_id
    GROUP BY 1
), no_id AS (
    SELECT r.nflcom_slug,
           MIN(TRY_CAST(r.season AS INTEGER)) AS min_season,
           MAX(TRY_CAST(r.season AS INTEGER)) AS max_season,
           COUNT(*) AS raw_rows,
           COUNT(s.raw_row_id) AS shared_rows,
           COUNT(f.raw_row_id) AS fumble_rows,
           COUNT(*) FILTER (WHERE r.direct_layout='RBFB5') AS observed_rbfb5_rows,
           COUNT(*) FILTER (WHERE r.direct_layout='WRTE') AS observed_wrte_rows,
           COUNT(*) FILTER (WHERE nl.neighbor_layout_n=1
                                  AND nl.neighbor_layout='RBFB5') AS unique_neighbor_rbfb5_rows,
           COUNT(*) FILTER (WHERE nl.neighbor_layout_n=1
                                  AND nl.neighbor_layout='WRTE') AS unique_neighbor_wrte_rows
    FROM raw r
    JOIN ident i USING (nflcom_slug)
    LEFT JOIN shared s ON s.raw_row_id=r.raw_row_id
    LEFT JOIN fumble_only f ON f.raw_row_id=r.raw_row_id
    LEFT JOIN neighbor_labels nl ON nl.raw_row_id=s.raw_row_id
    WHERE i.player_id_n=0
    GROUP BY 1
), bio AS (
    SELECT DISTINCT NFL_player_id, pfr_id, player, nfl_position,
           TRY_CAST(rookie_year AS INTEGER) AS rookie_year,
           TRY_CAST(first_year AS INTEGER) AS first_year,
           TRY_CAST(last_year AS INTEGER) AS last_year,
           regexp_replace(lower(player), '[^a-z0-9]', '', 'g') AS norm_name
    FROM read_parquet('{bio_path}')
), pfr_index AS (
    SELECT DISTINCT pfr_id, player, index_position,
           TRY_CAST(first_year AS INTEGER) AS first_year,
           TRY_CAST(last_year AS INTEGER) AS last_year,
           regexp_replace(lower(player), '[^a-z0-9]', '', 'g') AS norm_name
    FROM read_parquet('{index_path}')
), pfr_player_tables AS (
    SELECT DISTINCT pfr_id, player, index_position,
           TRY_CAST(first_year AS INTEGER) AS first_year,
           TRY_CAST(last_year AS INTEGER) AS last_year,
           TRY_CAST(subpage_year AS INTEGER) AS subpage_year,
           year_id,
           pos,
           regexp_replace(lower(player), '[^a-z0-9]', '', 'g') AS norm_name
    FROM read_parquet({player_tables}, union_by_name=true)
    WHERE pfr_id IS NOT NULL
), names AS (
    SELECT n.*,
           regexp_replace(
             regexp_replace(lower(nflcom_slug), '-[0-9]+$', ''),
             '[^a-z0-9]', '', 'g'
           ) AS norm_slug
    FROM no_id n
), bio_candidates AS (
    SELECT n.nflcom_slug, b.NFL_player_id, b.pfr_id, b.player,
           b.nfl_position AS candidate_position,
           CASE WHEN coalesce(b.last_year, 9999)>=n.min_season
                     AND coalesce(b.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'BIO_EXACT_NAME' AS evidence
    FROM names n JOIN bio b ON b.norm_name=n.norm_slug
), index_candidates AS (
    SELECT n.nflcom_slug, NULL::VARCHAR AS NFL_player_id, p.pfr_id, p.player,
           p.index_position AS candidate_position,
           CASE WHEN coalesce(p.last_year, 9999)>=n.min_season
                     AND coalesce(p.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'PFR_INDEX_EXACT_NAME' AS evidence
    FROM names n JOIN pfr_index p ON p.norm_name=n.norm_slug
), player_table_candidates AS (
    SELECT n.nflcom_slug, NULL::VARCHAR AS NFL_player_id, p.pfr_id, p.player,
           p.index_position AS candidate_position,
           CASE WHEN coalesce(p.last_year, 9999)>=n.min_season
                     AND coalesce(p.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'PFR_PLAYER_TABLE_EXACT_NAME' AS evidence
    FROM names n JOIN pfr_player_tables p ON p.norm_name=n.norm_slug
), historical_aliases(nflcom_slug, alias_name) AS (
    VALUES {alias_values}
), alias_bio_candidates AS (
    SELECT n.nflcom_slug, b.NFL_player_id, b.pfr_id, b.player,
           b.nfl_position AS candidate_position,
           CASE WHEN coalesce(b.last_year, 9999)>=n.min_season
                     AND coalesce(b.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'HISTORICAL_ALIAS_BIO' AS evidence
    FROM names n
    JOIN historical_aliases h USING (nflcom_slug)
    JOIN bio b ON lower(b.player)=lower(h.alias_name)
), alias_index_candidates AS (
    SELECT n.nflcom_slug, NULL::VARCHAR AS NFL_player_id, p.pfr_id, p.player,
           p.index_position AS candidate_position,
           CASE WHEN coalesce(p.last_year, 9999)>=n.min_season
                     AND coalesce(p.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'HISTORICAL_ALIAS_INDEX' AS evidence
    FROM names n
    JOIN historical_aliases h USING (nflcom_slug)
    JOIN pfr_index p ON lower(p.player)=lower(h.alias_name)
), historical_pfr_aliases(nflcom_slug, pfr_id) AS (
    VALUES {pfr_alias_values}
), pfr_alias_bio_candidates AS (
    SELECT n.nflcom_slug, b.NFL_player_id, b.pfr_id, b.player,
           b.nfl_position AS candidate_position,
           CASE WHEN coalesce(b.last_year, 9999)>=n.min_season
                     AND coalesce(b.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'HISTORICAL_ALIAS_PFR_ID' AS evidence
    FROM names n
    JOIN historical_pfr_aliases h USING (nflcom_slug)
    JOIN bio b ON b.pfr_id=h.pfr_id
), pfr_alias_index_candidates AS (
    SELECT n.nflcom_slug, NULL::VARCHAR AS NFL_player_id, p.pfr_id, p.player,
           p.index_position AS candidate_position,
           CASE WHEN coalesce(p.last_year, 9999)>=n.min_season
                     AND coalesce(p.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'HISTORICAL_ALIAS_PFR_ID' AS evidence
    FROM names n
    JOIN historical_pfr_aliases h USING (nflcom_slug)
    JOIN pfr_index p ON p.pfr_id=h.pfr_id
), pfr_alias_player_table_candidates AS (
    SELECT n.nflcom_slug, NULL::VARCHAR AS NFL_player_id, p.pfr_id, p.player,
           p.index_position AS candidate_position,
           CASE WHEN coalesce(p.last_year, 9999)>=n.min_season
                     AND coalesce(p.first_year, 0)<=n.max_season
                THEN 1 ELSE 0 END AS year_compatible,
           'HISTORICAL_ALIAS_PFR_ID' AS evidence
    FROM names n
    JOIN historical_pfr_aliases h USING (nflcom_slug)
    JOIN pfr_player_tables p ON p.pfr_id=h.pfr_id
), candidates AS (
    SELECT * FROM bio_candidates
    UNION ALL SELECT * FROM index_candidates
    UNION ALL SELECT * FROM player_table_candidates
    UNION ALL SELECT * FROM alias_bio_candidates
    UNION ALL SELECT * FROM alias_index_candidates
    UNION ALL SELECT * FROM pfr_alias_bio_candidates
    UNION ALL SELECT * FROM pfr_alias_index_candidates
    UNION ALL SELECT * FROM pfr_alias_player_table_candidates
), compatible_candidates AS (
    SELECT * FROM candidates WHERE year_compatible=1
), compatible_labeled AS (
    SELECT c.*,
           CASE
             WHEN c.candidate_position IN ('RB','FB','B')
               OR regexp_matches(c.candidate_position, '(^|[-])(HB|FB|TB|BB|B)([-]|$)')
               THEN 'RBFB5'
             WHEN c.candidate_position IN ('WR','TE')
               OR regexp_matches(c.candidate_position, '(^|[-])WB([-]|$)')
               THEN 'WRTE'
             ELSE NULL
           END AS candidate_layout,
           CASE
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(RB|FB|HB|TB|BB|WB|B)([-]|$)')
               THEN 'RBFB'
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(WR|TE|SE|FL|WB)([-]|$)')
               THEN 'WRTE'
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(DB|CB|S|FS|SS|SAF)([-]|$)')
               THEN 'DB'
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(DL|DE|DT|NT|E|LE|RE|LDE|RDE|LDT|RDT)([-]|$)')
               THEN 'DL'
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(LB|ILB|OLB|LLB|RLB|LILB|RILB|LOLB|ROLB)([-]|$)')
               THEN 'LB'
             WHEN regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(OL|OT|T|RT|LT|G|C|OG)([-]|$)')
               THEN 'OL'
             WHEN upper(coalesce(c.candidate_position, ''))='QB' THEN 'QB'
             WHEN upper(coalesce(c.candidate_position, ''))='K' THEN 'K'
             WHEN upper(coalesce(c.candidate_position, ''))='P' THEN 'P'
             ELSE NULL
           END AS candidate_position_family,
           CASE
             WHEN c.pfr_id IS NOT NULL THEN 'PFR:' || c.pfr_id
             WHEN c.NFL_player_id IS NOT NULL THEN 'NFL:' || c.NFL_player_id
             ELSE NULL
           END AS identity_key
    FROM compatible_candidates c
), identity_year_sources AS (
    SELECT 'PFR:' || pfr_id AS identity_key,
           first_year AS candidate_first_year,
           last_year AS candidate_last_year
    FROM pfr_index
    WHERE pfr_id IS NOT NULL
    UNION ALL
    SELECT CASE
             WHEN pfr_id IS NOT NULL THEN 'PFR:' || pfr_id
             WHEN NFL_player_id IS NOT NULL THEN 'NFL:' || NFL_player_id
             ELSE NULL
           END AS identity_key,
           coalesce(first_year, rookie_year) AS candidate_first_year,
           last_year AS candidate_last_year
    FROM bio
    WHERE pfr_id IS NOT NULL OR NFL_player_id IS NOT NULL
    UNION ALL
    SELECT 'PFR:' || pfr_id AS identity_key,
           coalesce(first_year, subpage_year,
                    TRY_CAST(NULLIF(year_id, '') AS INTEGER))
             AS candidate_first_year,
           coalesce(last_year, subpage_year,
                    TRY_CAST(NULLIF(year_id, '') AS INTEGER))
             AS candidate_last_year
    FROM pfr_player_tables
    WHERE pfr_id IS NOT NULL
), identity_years AS (
    SELECT identity_key,
           MIN(candidate_first_year) AS candidate_first_year,
           MAX(candidate_last_year) AS candidate_last_year
    FROM identity_year_sources
    WHERE identity_key IS NOT NULL
    GROUP BY 1
), historical_identity_overrides(
    nflcom_slug, identity_override_key, identity_override_source
) AS (
    VALUES {identity_override_values}
), compatible AS (
    SELECT c.*, o.identity_override_key, o.identity_override_source,
           y.candidate_first_year, y.candidate_last_year
    FROM compatible_labeled c
    LEFT JOIN historical_identity_overrides o USING (nflcom_slug)
    LEFT JOIN identity_years y USING (identity_key)
    WHERE o.identity_override_key IS NULL OR c.identity_key=o.identity_override_key
), historical_page_positions(nflcom_slug, page_position) AS (
    VALUES {page_position_values}
), compatible_page AS (
    SELECT c.*, hp.page_position,
           CASE
             WHEN hp.page_position IS NULL THEN 0
             WHEN upper(hp.page_position) IN ('RB','FB','HB')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(RB|FB|HB|TB|BB|WB|B)([-]|$)') THEN 1
             WHEN upper(hp.page_position) IN ('WR','TE')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(WR|TE|SE|FL|WB)([-]|$)') THEN 1
             WHEN upper(hp.page_position) IN ('DB','CB','S','FS','SS')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(DB|CB|S|FS|SS)([-]|$)') THEN 1
             WHEN upper(hp.page_position) IN ('DL','DE','DT','NT')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(DL|DE|DT|NT)([-]|$)') THEN 1
             WHEN upper(hp.page_position) IN ('LB','ILB','OLB')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(LB|ILB|OLB)([-]|$)') THEN 1
             WHEN upper(hp.page_position) IN ('OL','OT','T','G','C','OG')
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(OL|OT|T|G|C|OG)([-]|$)') THEN 1
             WHEN upper(hp.page_position)='QB'
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])QB([-]|$)') THEN 1
             WHEN upper(hp.page_position)='E'
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])(E|DE|DT|LE|RE|LDE|RDE|LDT|RDT)([-]|$)') THEN 1
             WHEN upper(hp.page_position)='K'
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])K([-]|$)') THEN 1
             WHEN upper(hp.page_position)='P'
               AND regexp_matches(upper(coalesce(c.candidate_position, '')), '(^|[-])P([-]|$)') THEN 1
             WHEN upper(hp.page_position) = upper(coalesce(c.candidate_position, '')) THEN 1
             ELSE 0
           END AS page_position_compatible
    FROM compatible c
    LEFT JOIN historical_page_positions hp USING (nflcom_slug)
), selected_identity_keys AS (
    SELECT DISTINCT n.nflcom_slug, n.min_season, n.max_season, c.identity_key
    FROM no_id n
    JOIN compatible_page c USING (nflcom_slug)
    WHERE c.identity_key IS NOT NULL
), source_table_positions AS (
    SELECT s.nflcom_slug,
           s.identity_key,
           coalesce(nullif(p.year_id, ''), cast(p.subpage_year AS VARCHAR)) AS year_label,
           p.pos,
           CASE
             WHEN upper(coalesce(p.pos, '')) IN
                    ('HB','FB','TB','BB','B','LH','RH','LHB','RHB')
               THEN 'RBFB'
             WHEN upper(coalesce(p.pos, '')) IN
                    ('WR','TE','FL','SE','WB')
               THEN 'WRTE'
             WHEN upper(coalesce(p.pos, '')) IN
                    ('DB','CB','S','FS','SS','SAF')
               THEN 'DB'
             WHEN upper(coalesce(p.pos, '')) IN
                    ('DL','DE','DT','NT','E','LE','RE','LDE','RDE','LDT','RDT')
               THEN 'DL'
             WHEN upper(coalesce(p.pos, '')) IN
                    ('LB','ILB','OLB','LLB','RLB','LILB','RILB','LOLB','ROLB')
               THEN 'LB'
             WHEN upper(coalesce(p.pos, '')) IN
                    ('OL','OT','T','RT','LT','G','C','OG')
               THEN 'OL'
             WHEN upper(coalesce(p.pos, ''))='QB' THEN 'QB'
             WHEN upper(coalesce(p.pos, ''))='K' THEN 'K'
             WHEN upper(coalesce(p.pos, ''))='P' THEN 'P'
             ELSE NULL
           END AS position_family,
           try_cast(coalesce(nullif(p.year_id, ''), cast(p.subpage_year AS VARCHAR)) AS INTEGER)
             AS source_year
    FROM selected_identity_keys s
    JOIN pfr_player_tables p
      ON 'PFR:' || p.pfr_id=s.identity_key
    WHERE try_cast(coalesce(nullif(p.year_id, ''), cast(p.subpage_year AS VARCHAR)) AS INTEGER)
              BETWEEN s.min_season AND s.max_season
), aggregate AS (
    SELECT n.nflcom_slug, n.min_season, n.max_season, n.raw_rows,
           n.shared_rows, n.fumble_rows,
           n.observed_rbfb5_rows, n.observed_wrte_rows,
           n.unique_neighbor_rbfb5_rows, n.unique_neighbor_wrte_rows,
           (SELECT COUNT(DISTINCT cc.identity_key)
              FROM compatible_labeled cc
             WHERE cc.nflcom_slug=n.nflcom_slug) AS all_compatible_identity_keys,
           MAX(c.identity_override_key) AS identity_override_key,
           MAX(c.identity_override_source) AS identity_override_source,
           COUNT(DISTINCT CASE
             WHEN c.pfr_id IS NOT NULL THEN 'PFR:' || c.pfr_id
             WHEN c.NFL_player_id IS NOT NULL THEN 'NFL:' || c.NFL_player_id
             ELSE NULL
           END) AS compatible_identity_keys,
           CASE WHEN COUNT(DISTINCT c.identity_key)=1
                THEN MIN(c.identity_key) ELSE NULL END AS unique_identity_key,
           COUNT(DISTINCT c.pfr_id) AS compatible_pfr_ids,
           COUNT(DISTINCT c.NFL_player_id)
             FILTER (WHERE c.NFL_player_id IS NOT NULL) AS compatible_nfl_ids,
           COUNT(DISTINCT c.candidate_position)
             FILTER (WHERE c.candidate_position IS NOT NULL) AS position_values,
           COUNT(DISTINCT c.candidate_position) AS compatible_position_values,
           CASE WHEN COUNT(DISTINCT c.identity_key)=1
                THEN MIN(c.candidate_position) ELSE NULL END AS unique_identity_position,
           COUNT(DISTINCT c.candidate_position)
             FILTER (WHERE c.candidate_position IN ('RB','FB','B','WR','TE'))
             AS layout_position_values,
           COUNT(DISTINCT c.candidate_position_family) AS identity_position_families,
           CASE WHEN COUNT(DISTINCT c.candidate_position_family)=1
                THEN MIN(c.candidate_position_family) ELSE NULL END
             AS identity_clean_position_family,
           COUNT(DISTINCT c.identity_key)
             FILTER (WHERE c.candidate_layout IS NOT NULL) AS compatible_layout_identity_keys,
           COUNT(DISTINCT c.identity_key)
             FILTER (WHERE c.candidate_layout='RBFB5' AND n.observed_rbfb5_rows>0
                           AND n.observed_wrte_rows=0)
             + COUNT(DISTINCT c.identity_key)
             FILTER (WHERE c.candidate_layout='WRTE' AND n.observed_wrte_rows>0
                           AND n.observed_rbfb5_rows=0)
             AS uniquely_observed_layout_identity_keys,
           CASE
             WHEN n.observed_rbfb5_rows>0 AND n.observed_wrte_rows=0
                  AND COUNT(DISTINCT c.identity_key)
                    FILTER (WHERE c.candidate_layout='RBFB5')=1
               THEN MIN(c.identity_key) FILTER (WHERE c.candidate_layout='RBFB5')
             WHEN n.observed_wrte_rows>0 AND n.observed_rbfb5_rows=0
                  AND COUNT(DISTINCT c.identity_key)
                    FILTER (WHERE c.candidate_layout='WRTE')=1
               THEN MIN(c.identity_key) FILTER (WHERE c.candidate_layout='WRTE')
             ELSE NULL
           END AS observed_layout_identity_key,
           CASE
             WHEN n.unique_neighbor_rbfb5_rows>0 AND n.unique_neighbor_wrte_rows=0
                  AND COUNT(DISTINCT c.identity_key)
                    FILTER (WHERE c.candidate_layout='RBFB5')=1
               THEN MIN(c.identity_key) FILTER (WHERE c.candidate_layout='RBFB5')
             WHEN n.unique_neighbor_wrte_rows>0 AND n.unique_neighbor_rbfb5_rows=0
                  AND COUNT(DISTINCT c.identity_key)
                    FILTER (WHERE c.candidate_layout='WRTE')=1
               THEN MIN(c.identity_key) FILTER (WHERE c.candidate_layout='WRTE')
             ELSE NULL
           END AS neighbor_layout_identity_key,
           MAX(c.page_position) AS page_position,
           COUNT(DISTINCT c.identity_key)
             FILTER (WHERE c.page_position_compatible=1) AS page_position_identity_keys,
           CASE WHEN COUNT(DISTINCT c.identity_key)
                          FILTER (WHERE c.page_position_compatible=1)=1
                THEN MIN(c.identity_key) FILTER (WHERE c.page_position_compatible=1)
                ELSE NULL END AS page_position_identity_key,
           CASE WHEN COUNT(DISTINCT c.identity_key)
                          FILTER (WHERE c.page_position_compatible=1)=1
                THEN MIN(c.candidate_first_year)
                     FILTER (WHERE c.page_position_compatible=1)
                ELSE NULL END AS page_candidate_first_year,
           CASE WHEN COUNT(DISTINCT c.identity_key)
                          FILTER (WHERE c.page_position_compatible=1)=1
                THEN MAX(c.candidate_last_year)
                     FILTER (WHERE c.page_position_compatible=1)
                ELSE NULL END AS page_candidate_last_year,
           COUNT(DISTINCT stp.pos) AS source_table_position_values,
           COUNT(DISTINCT stp.position_family) AS source_table_position_families,
           COUNT(*) FILTER (WHERE stp.pos IS NULL) AS source_table_unknown_position_rows,
           CASE WHEN COUNT(DISTINCT stp.position_family)=1
                THEN MIN(stp.position_family) ELSE NULL END AS source_table_clean_position_family,
           string_agg(DISTINCT coalesce(stp.pos, '?'), ' | '
                      ORDER BY coalesce(stp.pos, '?')) AS source_table_positions,
           string_agg(
             DISTINCT coalesce(c.pfr_id, 'NFL:' || c.NFL_player_id) || ':'
             || coalesce(c.NFL_player_id, '?') || ':'
             || c.player || ':' || coalesce(c.candidate_position, '?') || ':' || c.evidence,
             ' | ' ORDER BY coalesce(c.pfr_id, 'NFL:' || c.NFL_player_id) || ':'
             || coalesce(c.NFL_player_id, '?') || ':'
             || c.player || ':' || coalesce(c.candidate_position, '?') || ':' || c.evidence
           ) AS candidates
    FROM names n LEFT JOIN compatible_page c USING (nflcom_slug)
    LEFT JOIN source_table_positions stp
      ON stp.nflcom_slug=n.nflcom_slug
     AND stp.identity_key=c.identity_key
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT * FROM aggregate ORDER BY shared_rows DESC, fumble_rows DESC, nflcom_slug
"""


def build() -> dict:
    con = duckdb.connect()
    try:
        neighbor_rows = [
            {
                "table": table,
                "candidate_layout": layout,
                "shared_rows": int(shared_rows),
                "rows_with_neighbor": int(with_neighbor),
                "unique_neighbor_rows": int(unique_rows),
                "conflicting_neighbor_rows": int(conflicting_rows),
            }
            for table, layout, shared_rows, with_neighbor, unique_rows, conflicting_rows
            in con.execute(_neighbor_query()).fetchall()
        ]
        identity_rows = [
            {"population": population, "identity_status": status, "rows": int(rows)}
            for population, status, rows in con.execute(_identity_query()).fetchall()
        ]
        position_rows = [
            {"population": population, "status": status, "rows": int(rows)}
            for population, status, rows in con.execute(_position_query()).fetchall()
        ]
        position_layout_rows = [
            {
                "table": table,
                "position": position,
                "position_layout": layout,
                "rows": int(rows),
            }
            for table, position, layout, rows in con.execute(
                _position_layout_query()
            ).fetchall()
        ]
        position_calibration_rows = [
            {
                "layout": layout,
                "rows": int(rows),
                "clean_position_rows": int(clean_rows),
                "agree_rows": int(agree_rows),
                "disagree_rows": int(disagree_rows),
            }
            for layout, rows, clean_rows, agree_rows, disagree_rows in con.execute(
                _position_calibration_query()
            ).fetchall()
        ]
        player_rows = [
            {
                "status": status,
                "slug_count": int(slug_count),
                "unique_nfl_player_ids": int(unique_ids),
            }
            for status, slug_count, unique_ids in con.execute(_player_query()).fetchall()
        ]
        position_neighbor_rows = [
            {"status": status, "rows": int(rows)}
            for status, rows in con.execute(_position_neighbor_query()).fetchall()
        ]
        shared_resolution_rows = [
            {
                "resolution_source": source,
                "resolved_layout": layout,
                "rows": int(rows),
            }
            for source, layout, rows in con.execute(
                _shared_resolution_query()
            ).fetchall()
        ]
        no_bridge_identity_rows = [
            {
                "nflcom_slug": slug,
                "min_season": int(min_season) if min_season is not None else None,
                "max_season": int(max_season) if max_season is not None else None,
                "raw_rows": int(raw_rows),
                "shared_rows": int(shared_rows),
                "fumble_rows": int(fumble_rows),
                "observed_rbfb5_rows": int(observed_rbfb5_rows),
                "observed_wrte_rows": int(observed_wrte_rows),
                "unique_neighbor_rbfb5_rows": int(unique_neighbor_rbfb5_rows),
                "unique_neighbor_wrte_rows": int(unique_neighbor_wrte_rows),
                "all_compatible_identity_keys": int(all_identity_keys),
                "identity_override_key": identity_override_key,
                "identity_override_source": identity_override_source,
                "compatible_identity_keys": int(identity_keys),
                "unique_identity_key": unique_identity_key,
                "compatible_pfr_ids": int(compatible_pfr_ids),
                "compatible_nfl_ids": int(compatible_nfl_ids),
                "position_values": int(position_values),
                "compatible_position_values": int(compatible_position_values),
                "unique_identity_position": unique_identity_position,
                "layout_position_values": int(layout_position_values),
                "identity_position_families": int(identity_position_families),
                "identity_clean_position_family": identity_clean_position_family,
                "compatible_layout_identity_keys": int(compatible_layout_keys),
                "uniquely_observed_layout_identity_keys": int(unique_observed_layout_keys),
                "observed_layout_identity_key": observed_layout_identity_key,
                "neighbor_layout_identity_key": neighbor_layout_identity_key,
                "page_position": page_position,
                "page_position_identity_keys": int(page_position_identity_keys),
                "page_position_identity_key": page_position_identity_key,
                "page_candidate_first_year": int(page_candidate_first_year)
                if page_candidate_first_year is not None else None,
                "page_candidate_last_year": int(page_candidate_last_year)
                if page_candidate_last_year is not None else None,
                "page_candidate_year_fit": (
                    page_candidate_first_year is not None
                    and page_candidate_last_year is not None
                    and page_candidate_first_year <= max_season
                    and page_candidate_last_year >= min_season
                ),
                "selected_identity_key": identity_override_key or unique_identity_key,
                "page_position_identity_match": (
                    page_position_identity_key is not None
                    and page_position_identity_key == (identity_override_key or unique_identity_key)
                ),
                "source_table_position_values": int(source_table_position_values),
                "source_table_position_families": int(source_table_position_families),
                "source_table_unknown_position_rows": int(source_table_unknown_position_rows),
                "source_table_clean_position_family": source_table_clean_position_family,
                "source_table_positions": source_table_positions,
                "position_candidate_source": (
                    "PAGE_HEADER"
                    if page_position_identity_key is not None
                    and page_position_identity_key == (identity_override_key or unique_identity_key)
                    else (
                        "SOURCE_TABLE_FAMILY"
                        if source_table_position_families == 1
                        else (
                            "IDENTITY_FAMILY"
                            if source_table_position_families == 0
                            and identity_position_families == 1
                            else "PENDING_CONFLICT"
                        )
                    )
                ),
                "position_candidate": (
                    page_position
                    if page_position_identity_key is not None
                    and page_position_identity_key == (identity_override_key or unique_identity_key)
                    else (
                        source_table_clean_position_family
                        if source_table_position_families == 1
                        else (
                            identity_clean_position_family
                            if source_table_position_families == 0
                            and identity_position_families == 1
                            else None
                        )
                    )
                ),
                "candidates": candidates,
            }
            for (
                slug,
                min_season,
                max_season,
                raw_rows,
                shared_rows,
                fumble_rows,
                observed_rbfb5_rows,
                observed_wrte_rows,
                unique_neighbor_rbfb5_rows,
                unique_neighbor_wrte_rows,
                all_identity_keys,
                identity_override_key,
                identity_override_source,
                identity_keys,
                unique_identity_key,
                compatible_pfr_ids,
                compatible_nfl_ids,
                position_values,
                compatible_position_values,
                unique_identity_position,
                layout_position_values,
                identity_position_families,
                identity_clean_position_family,
                compatible_layout_keys,
                unique_observed_layout_keys,
                observed_layout_identity_key,
                neighbor_layout_identity_key,
                page_position,
                page_position_identity_keys,
                page_position_identity_key,
                page_candidate_first_year,
                page_candidate_last_year,
                source_table_position_values,
                source_table_position_families,
                source_table_unknown_position_rows,
                source_table_clean_position_family,
                source_table_positions,
                candidates,
            ) in con.execute(_no_bridge_identity_query()).fetchall()
        ]
    finally:
        con.close()
    receipt = {
        "version": "1",
        "source_only": True,
        "subject_table_used": False,
        "neighbor_rule": (
            "same slug, season, physical table, and adjacent numeric week; accept only "
            "one distinct already-recovered layout; refuse conflicts"
        ),
        "neighbor_layout_candidates": neighbor_rows,
        "neighbor_summary": {
            "shared_rows": sum(row["shared_rows"] for row in neighbor_rows),
            "unique_compatible_neighbor_rows": sum(
                row["unique_neighbor_rows"]
                for row in neighbor_rows
                if row["candidate_layout"] != "NO_UNIQUE_LAYOUT"
            ),
            "conflicting_neighbor_rows": sum(
                row["conflicting_neighbor_rows"] for row in neighbor_rows
            ),
            "no_neighbor_rows": sum(
                row["shared_rows"]
                for row in neighbor_rows
                if row["candidate_layout"] == "NO_UNIQUE_LAYOUT"
            ),
            "status": "CANDIDATE_ONLY_COLLISION_SAFE",
        },
        "identity_rule": "slug -> PFR identity -> NFL_player_id; unique IDs only",
        "identity_override_rule": (
            "for an explicitly listed slug, retain only the stated PFR/NFL identity after "
            "the season-compatible candidate set is built; preserve the pre-override count"
        ),
        "identity_coverage": identity_rows,
        "position_coverage": position_rows,
        "position_layout_candidates": position_layout_rows,
        "position_calibration": position_calibration_rows,
        "player_census": player_rows,
        "position_neighbor_resolution": position_neighbor_rows,
        "shared_gap_resolution": shared_resolution_rows,
        "no_bridge_identity_queue": {
            "slugs": no_bridge_identity_rows,
            "summary": {
                "no_id_slugs": len(no_bridge_identity_rows),
                "raw_rows": sum(row["raw_rows"] for row in no_bridge_identity_rows),
                "shared_rows": sum(row["shared_rows"] for row in no_bridge_identity_rows),
                "fumble_rows": sum(row["fumble_rows"] for row in no_bridge_identity_rows),
                "pre_override_unique_compatible_identity_candidate": sum(
                    row["all_compatible_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "pre_override_ambiguous_compatible_identity_candidate": sum(
                    row["all_compatible_identity_keys"] > 1
                    for row in no_bridge_identity_rows
                ),
                "identity_override_resolved": sum(
                    row["identity_override_key"] is not None
                    and row["compatible_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "career_table_override_resolved": sum(
                    row["identity_override_source"] == "CAREER_TABLE"
                    and row["compatible_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "page_position_only_candidate_resolved": sum(
                    row["identity_override_source"] == "PAGE_POSITION_ONLY"
                    and row["compatible_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "page_position_only_year_fit": sum(
                    row["identity_override_source"] == "PAGE_POSITION_ONLY"
                    and row["page_candidate_year_fit"]
                    for row in no_bridge_identity_rows
                ),
                "page_position_only_year_mismatch": sum(
                    row["identity_override_source"] == "PAGE_POSITION_ONLY"
                    and not row["page_candidate_year_fit"]
                    for row in no_bridge_identity_rows
                ),
                "source_table_clean_position_family": sum(
                    row["source_table_position_families"] == 1
                    for row in no_bridge_identity_rows
                ),
                "source_table_multi_position_family": sum(
                    row["source_table_position_families"] > 1
                    for row in no_bridge_identity_rows
                ),
                "source_table_no_position_rows": sum(
                    row["source_table_position_families"] == 0
                    for row in no_bridge_identity_rows
                ),
                "unique_identity_one_position_family": sum(
                    row["identity_position_families"] == 1
                    for row in no_bridge_identity_rows
                ),
                "position_family_resolved_by_source_or_identity": sum(
                    row["source_table_position_families"] == 1
                    or (
                        row["source_table_position_families"] == 0
                        and row["identity_position_families"] == 1
                    )
                    for row in no_bridge_identity_rows
                ),
                "genuine_position_family_gap": sum(
                    row["source_table_position_families"] > 1
                    or (
                        row["source_table_position_families"] == 0
                        and row["identity_position_families"] > 1
                    )
                    for row in no_bridge_identity_rows
                ),
                "position_family_unresolved": sum(
                    not (
                        row["source_table_position_families"] == 1
                        or (
                            row["source_table_position_families"] == 0
                            and row["identity_position_families"] == 1
                        )
                    )
                    for row in no_bridge_identity_rows
                ),
                "position_family_unresolved_but_page_identity_resolved": sum(
                    (
                        row["source_table_position_families"] > 1
                        or (
                            row["source_table_position_families"] == 0
                            and row["identity_position_families"] != 1
                        )
                    )
                    and row["page_position_identity_key"] is not None
                    for row in no_bridge_identity_rows
                ),
                "position_family_unresolved_after_page_evidence": sum(
                    (
                        row["source_table_position_families"] > 1
                        or (
                            row["source_table_position_families"] == 0
                            and row["identity_position_families"] != 1
                        )
                    )
                    and row["page_position_identity_key"] is None
                    for row in no_bridge_identity_rows
                ),
                "position_candidate_page_header": sum(
                    row["position_candidate_source"] == "PAGE_HEADER"
                    for row in no_bridge_identity_rows
                ),
                "position_candidate_source_table_family": sum(
                    row["position_candidate_source"] == "SOURCE_TABLE_FAMILY"
                    for row in no_bridge_identity_rows
                ),
                "position_candidate_identity_family": sum(
                    row["position_candidate_source"] == "IDENTITY_FAMILY"
                    for row in no_bridge_identity_rows
                ),
                "position_candidate_pending_conflict": sum(
                    row["position_candidate_source"] == "PENDING_CONFLICT"
                    for row in no_bridge_identity_rows
                ),
                "identity_override_unmatched": sum(
                    row["identity_override_key"] is not None
                    and row["compatible_identity_keys"] == 0
                    for row in no_bridge_identity_rows
                ),
                "no_compatible_identity_candidate": sum(
                    row["compatible_identity_keys"] == 0 for row in no_bridge_identity_rows
                ),
                "unique_compatible_identity_candidate": sum(
                    row["compatible_identity_keys"] == 1 for row in no_bridge_identity_rows
                ),
                "ambiguous_compatible_identity_candidate": sum(
                    row["compatible_identity_keys"] > 1 for row in no_bridge_identity_rows
                ),
                "unique_compatible_clean_position": sum(
                    row["compatible_identity_keys"] == 1 and row["position_values"] == 1
                    for row in no_bridge_identity_rows
                ),
                "unique_compatible_layout_position": sum(
                    row["compatible_identity_keys"] == 1
                    and row["layout_position_values"] == 1
                    for row in no_bridge_identity_rows
                ),
                "ambiguous_resolved_by_unique_observed_layout": sum(
                    row["compatible_identity_keys"] > 1
                    and row["uniquely_observed_layout_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "unique_identity_with_unique_observed_layout": sum(
                    row["compatible_identity_keys"] == 1
                    and row["uniquely_observed_layout_identity_keys"] == 1
                    for row in no_bridge_identity_rows
                ),
                "unique_identity_with_single_position": sum(
                    row["compatible_identity_keys"] == 1
                    and row["compatible_position_values"] == 1
                    for row in no_bridge_identity_rows
                ),
                "observed_layout_resolved_identity": sum(
                    row["observed_layout_identity_key"] is not None
                    for row in no_bridge_identity_rows
                ),
                "neighbor_layout_resolved_identity": sum(
                    row["neighbor_layout_identity_key"] is not None
                    for row in no_bridge_identity_rows
                ),
                "page_position_resolved_identity": sum(
                    row["page_position_identity_key"] is not None
                    for row in no_bridge_identity_rows
                ),
                "ambiguous_resolved_by_page_position": sum(
                    row["compatible_identity_keys"] > 1
                    and row["page_position_identity_key"] is not None
                    for row in no_bridge_identity_rows
                ),
                "page_position_unmatched": sum(
                    row["page_position"] is not None
                    and row["page_position_identity_keys"] == 0
                    for row in no_bridge_identity_rows
                ),
                "page_position_still_ambiguous": sum(
                    row["page_position"] is not None
                    and row["page_position_identity_keys"] > 1
                    for row in no_bridge_identity_rows
                ),
                "observed_neighbor_identity_agreements": sum(
                    row["observed_layout_identity_key"] is not None
                    and row["neighbor_layout_identity_key"] is not None
                    and row["observed_layout_identity_key"]
                    == row["neighbor_layout_identity_key"]
                    for row in no_bridge_identity_rows
                ),
                "observed_neighbor_identity_conflicts": sum(
                    row["observed_layout_identity_key"] is not None
                    and row["neighbor_layout_identity_key"] is not None
                    and row["observed_layout_identity_key"]
                    != row["neighbor_layout_identity_key"]
                    for row in no_bridge_identity_rows
                ),
            },
            "status": "CANDIDATE_ONLY_NO_BRIDGE_MUTATION",
        },
        "interpretation": (
            "player identity is sufficient for layout-independent generic fumbles, but "
            "does not by itself assign shared offensive layouts"
        ),
        "no_mapping_or_license_change": True,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
