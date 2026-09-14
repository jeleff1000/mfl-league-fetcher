from __future__ import annotations

import itertools
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd


ROOT = Path('D:/yahoo_oauth')
PANEL = ROOT / 'tmp/player_outcome_stability_2003_2025/league_player_season.parquet'
OPS = Path('D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb')
OUT = ROOT / 'tmp/position_collapse_ladder_2003_2025'
YEARS = list(range(2003, 2026))
POSITIONS = ['RB', 'WR', 'TE', 'DEF', 'K', 'QB', 'DL', 'LB', 'DB']
MODES = ['Managed', 'Bestball']
DIMENSIONS = ['teams', 'roster', 'scoring', 'pass_td', 'po_slots']
VALUES = {
    'teams': [8, 10, 12, 14],
    'roster': ['Flex', 'IDP', 'SFlex'],
    'scoring': ['Standard', '0.5 PPR', 'Full PPR'],
    'pass_td': ['4pt', '6pt'],
    'po_slots': [4, 6, 8],
}
ORDERS = {
    'RB': ['pass_td', 'roster', 'scoring', 'teams', 'po_slots'],
    'WR': ['pass_td', 'roster', 'scoring', 'teams', 'po_slots'],
    'TE': ['pass_td', 'roster', 'scoring', 'teams', 'po_slots'],
    'DEF': ['pass_td', 'scoring', 'roster', 'teams', 'po_slots'],
    'K': ['pass_td', 'scoring', 'roster', 'teams', 'po_slots'],
    'QB': ['scoring', 'pass_td', 'roster', 'teams', 'po_slots'],
    'DL': ['scoring', 'pass_td', 'teams', 'po_slots'],
    'LB': ['scoring', 'pass_td', 'teams', 'po_slots'],
    'DB': ['scoring', 'pass_td', 'teams', 'po_slots'],
}
TARGETS = [(.85, .05), (.85, .03), (.95, .05), (.95, .03), (.95, .01)]


def required_n(p: float, confidence: float, margin: float) -> int:
    if p <= 0 or p >= 1:
        return math.ceil(math.log((1 - confidence) / 2) / math.log(1 - margin))
    z = NormalDist().inv_cdf((1 + confidence) / 2)
    return math.ceil(z * z * p * (1 - p) / margin**2)


def position_map() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute(f"ATTACH '{OPS.as_posix()}' AS ops (READ_ONLY)")
    q = '''
    WITH x AS (
      SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
             CAST("year" AS INTEGER) AS year, UPPER(TRIM(position)) AS raw_position
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE "year" BETWEEN 2003 AND 2025
        AND NFL_player_id IS NOT NULL AND position IS NOT NULL
    )
    SELECT NFL_player_id, year,
      CASE
        WHEN raw_position LIKE '%QB%' THEN 'QB'
        WHEN raw_position LIKE '%RB%' THEN 'RB'
        WHEN raw_position LIKE '%WR%' THEN 'WR'
        WHEN raw_position LIKE '%TE%' THEN 'TE'
        WHEN raw_position LIKE '%DEF%' OR raw_position='DST' THEN 'DEF'
        WHEN raw_position='K' OR raw_position LIKE 'K,%' THEN 'K'
        WHEN raw_position LIKE '%DL%' THEN 'DL'
        WHEN raw_position LIKE '%LB%' THEN 'LB'
        WHEN raw_position LIKE '%DB%' THEN 'DB'
      END AS position
    FROM x
    '''
    out = con.execute(q).fetchdf()
    con.close()
    return out.dropna(subset=['position']).drop_duplicates(['NFL_player_id', 'year'])


def cohort_universe() -> pd.DataFrame:
    rows = []
    for vals in itertools.product(*(VALUES[c] for c in DIMENSIONS)):
        rows.append(dict(zip(DIMENSIONS, vals)))
    return pd.DataFrame(rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cols = ['db_name', 'year', 'NFL_player_id', *DIMENSIONS, 'lineup', 'starts', 'champ', 'playoffs']
    d = pd.read_parquet(PANEL, columns=cols)
    d['NFL_player_id'] = d.NFL_player_id.astype(str)
    d = d.merge(position_map(), on=['NFL_player_id', 'year'], how='inner')
    d = d[d.position.isin(POSITIONS) & d.year.isin(YEARS) & d.lineup.isin(MODES) & (d.starts > 0)].copy()
    d['teams'] = d['teams'].astype(int)
    d['po_slots'] = d['po_slots'].astype(int)
    d.to_parquet(OUT / 'position_panel.parquet', index=False)

    universe = cohort_universe()
    universe.to_csv(OUT / 'cohort_universe_216.csv', index=False)
    pd.DataFrame({'position': POSITIONS}).to_csv(OUT / 'position_universe.csv', index=False)

    full = ['position', 'year', 'lineup', *DIMENSIONS]
    cell = d.groupby(full, as_index=False).agg(cell_leagues=('db_name', 'nunique'))
    g = d.groupby(full + ['NFL_player_id'], as_index=False).agg(
        leagues=('db_name', 'nunique'), starts=('starts', 'sum'),
        champ_rate=('champ', 'mean'), playoff_rate=('playoffs', 'mean'))
    g = g.merge(cell, on=full, how='left')
    g = g[(g.cell_leagues >= 8) & (g.leagues >= 8) & (g.starts >= 8)].copy()

    anchors = []
    for keys, h in g.groupby(full, dropna=False):
        pos = keys[0]
        teams = int(keys[3])
        h = h.copy()
        h['champ_lift'] = h.champ_rate - 1 / teams
        h['playoff_lift'] = h.playoff_rate - h.po_slots / teams
        for metric, n_over, n_under in [('champ', 5, 3), ('playoff', 8, 5)]:
            x = h.sort_values([metric + '_lift', 'NFL_player_id'], ascending=[False, True])
            for side, part in [('over', x.head(n_over)), ('under', x.tail(n_under).sort_values([metric + '_lift', 'NFL_player_id']))]:
                for rank, (_, row) in enumerate(part.iterrows(), 1):
                    z = row.to_dict()
                    z.update({'metric': metric, 'side': side, 'rank': rank})
                    anchors.append(z)
    anchor_df = pd.DataFrame(anchors)
    anchor_df.to_csv(OUT / 'outlier_anchors.csv', index=False)

    summaries = []
    level_rows = []
    # Precompute each position/level grouping once.  The previous implementation
    # repeated these groupbys inside every year x lineup x metric cell.
    for pos in POSITIONS:
        posd = d[d.position == pos]
        posa = anchor_df[anchor_df.position == pos]
        for level in range(len(ORDERS[pos]) + 1):
            collapsed = ORDERS[pos][:level]
            keep = [x for x in DIMENSIONS if x not in collapsed]
            group_cols = ['year', 'lineup'] + keep + ['NFL_player_id']
            if posd.empty:
                lv = pd.DataFrame(columns=group_cols + ['level_leagues', 'champ_level_rate', 'playoff_level_rate'])
            else:
                lv = posd.groupby(group_cols, as_index=False).agg(
                    level_leagues=('db_name', 'nunique'), champ_level_rate=('champ', 'mean'),
                    playoff_level_rate=('playoffs', 'mean'))
            for year in YEARS:
                for mode in MODES:
                    for metric in ['champ', 'playoff']:
                        a0 = posa[(posa.year == year) & (posa.lineup == mode) & (posa.metric == metric)]
                        a = a0.merge(lv, on=group_cols, how='left') if not a0.empty else pd.DataFrame()
                        if a.empty:
                            continue
                        rate_col = 'champ_level_rate' if metric == 'champ' else 'playoff_level_rate'
                        a['level'] = level
                        a['collapsed'] = '+'.join(collapsed) if collapsed else 'none'
                        a['kept'] = '+'.join(keep) if keep else 'none'
                        a['rate_drift_abs'] = (a[rate_col] - (a['champ_rate'] if metric == 'champ' else a['playoff_rate'])).abs()
                        for conf, margin in TARGETS:
                            tag = f'{int(conf*100)}_{int(margin*100)}'
                            a[f'required_{tag}'] = a[rate_col].map(lambda p: required_n(float(p), conf, margin))
                        level_rows.append(a)
                        cells = posd[(posd.year == year) & (posd.lineup == mode)]
                        row = {'position': pos, 'year': year, 'lineup': mode, 'metric': metric,
                               'level': level, 'collapsed': '+'.join(collapsed) if collapsed else 'none',
                               'kept': '+'.join(keep) if keep else 'none', 'anchors': len(a),
                               'granularity_216_fraction': (universe[keep].drop_duplicates().shape[0] / 216) if keep else 1 / 216,
                               'granularity_observed_cells': cells[keep].drop_duplicates().shape[0] if not cells.empty else 0,
                               'league_n_p10': a.level_leagues.quantile(.1), 'league_n_p50': a.level_leagues.median(),
                               'league_n_p90': a.level_leagues.quantile(.9), 'rate_p50': a[rate_col].median(),
                               'rate_drift_p50': a.rate_drift_abs.median(), 'rate_drift_p90': a.rate_drift_abs.quantile(.9),
                               'support_gain_p50': (a.level_leagues / a.leagues).median(),
                               'support_gain_p90': (a.level_leagues / a.leagues).quantile(.9)}
                        for conf, margin in TARGETS:
                            tag = f'{int(conf*100)}_{int(margin*100)}'
                            reqs = a[f'required_{tag}']
                            row[f'required_p50_{tag}'] = reqs.median()
                            row[f'required_p90_{tag}'] = reqs.quantile(.9)
                            row[f'clear_share_{tag}'] = (a.level_leagues >= reqs).mean()
                        summaries.append(row)

    pd.concat(level_rows, ignore_index=True).to_csv(OUT / 'cohort_level_decisions.csv', index=False)
    pd.DataFrame(summaries).to_csv(OUT / 'collapse_ladder_summary.csv', index=False)

    # Complete decision matrix: 216 formats x 23 seasons x 9 positions x 2 lineup modes.
    matrix = universe.assign(key=1).merge(pd.DataFrame({'year': YEARS, 'key': 1}), on='key').merge(
        pd.DataFrame({'position': POSITIONS, 'key': 1}), on='key').merge(
        pd.DataFrame({'lineup': MODES, 'key': 1}), on='key').drop(columns='key')
    support = d.groupby(['year', 'position', 'lineup', *DIMENSIONS], as_index=False).agg(
        available_leagues=('db_name', 'nunique'), player_rows=('NFL_player_id', 'count'))
    matrix = matrix.merge(support, on=['year', 'position', 'lineup', *DIMENSIONS], how='left')
    matrix['available_leagues'] = matrix.available_leagues.fillna(0).astype(int)
    matrix['player_rows'] = matrix.player_rows.fillna(0).astype(int)
    matrix['decision_status'] = matrix.available_leagues.map(lambda n: 'supported' if n >= 8 else ('thin' if n > 0 else 'empty'))
    matrix.to_csv(OUT / 'decision_matrix_89424.csv', index=False)
    print(f'panel_rows={len(d):,} positions={sorted(d.position.unique())} anchors={len(anchor_df):,}')
    print(f'decisions={len(matrix):,} (216 formats x 23 years x 9 positions x 2 lineup modes)')
    print(pd.DataFrame(summaries).groupby(['lineup', 'metric', 'level'], as_index=False)[['anchors', 'league_n_p50', 'rate_drift_p50']].mean().to_string(index=False))


if __name__ == '__main__':
    main()
