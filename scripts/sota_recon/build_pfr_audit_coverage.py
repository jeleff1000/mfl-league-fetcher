"""Build an inventory-to-receipt coverage manifest for the PFR lake."""
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path('docs/audits')
INV=Path('docs/pfr-lake-inventory.json')
OUT=ROOT/'pfr-audit-coverage-2025.json'
def artifacts_for(rel):
 r=rel.lower().replace('/','\\')
 exact={
  'boxscores':'pfr-team-games-2025.json','boxscores\\tables\\defense_advanced':'pfr-box-advanced-2025.json','boxscores\\tables\\expected_points':'pfr-boxscore-context-2025.json','boxscores\\tables\\game_info':'pfr-boxscore-context-2025.json','boxscores\\tables\\home_drives':'pfr-drives-pbp-scoring-2025.json','boxscores\\tables\\home_snap_counts':'pfr-snap-subtables-2025.json','boxscores\\tables\\home_starters':'pfr-snap-subtables-2025.json','boxscores\\tables\\kicking':'pfr-box-kicking-returns-2025.json','boxscores\\tables\\officials':'pfr-boxscore-context-2025.json','boxscores\\tables\\passing_advanced':'pfr-box-advanced-2025.json','boxscores\\tables\\pbp':'pfr-drives-pbp-scoring-2025.json','boxscores\\tables\\player_defense':'pfr-pfr_player_defense_box-defense-regular-2025.json','boxscores\\tables\\player_offense':'pfr-pfr_player_offense_box-offense-regular-2025.json','boxscores\\tables\\receiving_advanced':'pfr-box-advanced-2025.json','boxscores\\tables\\returns':'pfr-box-kicking-returns-2025.json','boxscores\\tables\\rushing_advanced':'pfr-box-advanced-2025.json','boxscores\\tables\\scoring':'pfr-drives-pbp-scoring-2025.json','boxscores\\tables\\team_stats':'pfr-boxscore-context-2025.json','boxscores\\tables\\vis_drives':'pfr-drives-pbp-scoring-2025.json','boxscores\\tables\\vis_snap_counts':'pfr-snap-subtables-2025.json','boxscores\\tables\\vis_starters':'pfr-snap-subtables-2025.json','context\\tables\\accuracy':'pfr-detail-models-2025.json','context\\tables\\advanced_receiving':'pfr-detail-models-2025.json','context\\tables\\advanced_rushing':'pfr-detail-models-2025.json','context\\tables\\air_yards':'pfr-detail-models-2025.json','context\\tables\\coaches':'pfr-coaches-2025.json','context\\tables\\play_type':'pfr-detail-models-2025.json','context\\tables\\pressure':'pfr-detail-models-2025.json','context\\tables\\all_pro':'pfr-membership-2025.json','context\\tables\\pro_bowl':'pfr-membership-2025.json','context\\tables\\combine':'pfr-combine-2025.json','players\\tables\\adj_passing':'pfr-adjusted-passing-2025.json','players\\tables\\all_pro':'pfr-membership-2025.json','players\\tables\\combine':'pfr-combine-2025.json','players\\tables\\games_played':'pfr-games-played-2025.json','players\\tables\\games_played_playoffs':'pfr-games-played-2025.json','players\\tables\\ol_penalties':'pfr-ol-penalties-2025.json','players\\tables\\player_fantasy':'pfr-player-fantasy-probe-physical-2025.json','players\\tables\\sim_scores':'pfr-detail-models-2025.json','players\\tables\\passing':'pfr-player-passing-2025.json','players\\tables\\passing_post':'pfr-player-passing-2025.json','cache':'pfr-excel-master-schedule-2025.json','players':'pfr-identity-assets-2025.json','context':'pfr-detail-models-2025.json'}
 exact['cache']=['pfr-excel-master-schedule-2025.json','pfr-root-structures-2025.json']
 exact['context']=['pfr-detail-models-2025.json','pfr-root-structures-2025.json']
 exact['boxscores']=['pfr-team-games-2025.json','pfr-root-structures-2025.json']
 exact['players']=['pfr-identity-assets-2025.json','pfr-root-structures-2025.json']
 exact['players\\tables\\player_fantasy']=['pfr-player-fantasy-physical-2025.json']
 if r in exact:
  value=exact[r]
  return value if isinstance(value,list) else [value]
 if r.startswith('context\\tables\\voting_'):return ['pfr-voting-2025.json']
 if r.startswith('players\\tables\\adv_receiving_and_rushing'):return ['pfr-pfr_adv_recrush-2025.json']
 if r.startswith('players\\tables\\adv_rushing_and_receiving'):return ['pfr-pfr_adv_rushrec-2025.json']
 if r.startswith('players\\tables\\adv_defense'):return ['pfr-pfr_adv_defense-2025.json']
 if r.startswith('players\\tables\\defense'):return ['pfr-pfr_player_defense-2025.json']
 if r.startswith('players\\tables\\passing_advanced'):return ['pfr-player-season-passing-advanced-2025.json']
 if r.startswith('players\\tables\\passing'):return ['pfr-player-season-passing-advanced-2025.json']
 if r.startswith('players\\tables\\receiving_and_rushing'):return ['pfr-player-season-rec-rush-2025.json']
 if r.startswith('players\\tables\\rushing_and_receiving'):return ['pfr-player-season-rush-rec-2025.json']
 if r.startswith('players\\tables\\kicking'):return ['pfr-pfr_player_kicking-2025.json']
 if r.startswith('players\\tables\\punting'):return ['pfr-pfr_player_punting-2025.json']
 if r.startswith('players\\tables\\returns'):return ['pfr-pfr_player_returns-2025.json']
 if r.startswith('players\\tables\\scoring'):return ['pfr-pfr_player_scoring-2025.json']
 if r.startswith('players\\tables\\snap_counts'):return ['pfr-pfr_snap_counts-2025.json']
 if r.startswith('players\\tables\\fantasy'):return ['pfr-pfr_player_fantasy-2025.json']
 return []
def main():
 inv=json.loads(INV.read_text(encoding='utf-8')); rows=[]
 for item in inv['tables']:
  arts=artifacts_for(item['relative_path']); rows.append({'physical_path':item['physical_path'],'relative_path':item['relative_path'],'registration_status':item['registration_status'],'registered_source_keys':item['registered_source_keys'],'receipt_artifacts':arts,'coverage_status':'RECEIPTED' if arts else 'UNMAPPED'})
 allowed={'VERIFIED_DIRECT_MAPPING','MAPPED_BUT_WRONG','VERIFIED_DERIVED_WITNESS','CONTEXT_TO_SEASON_OR_BIO','PROMOTION_CANDIDATE','STRUCTURED_WITNESS_REQUIRED','DEFERRED_BACKFILL_CANDIDATE','INTENTIONALLY_UNMAPPED_WITH_REASON','OPEN_BLOCKED','MAPPED_TO_CANONICAL','EXCLUDED_WITH_REASON','NEW_SUPERTABLE_COLUMN_CANDIDATE'}; artifact_checks={}; invalid=[]; blocked=[]
 for name in sorted({a for r in rows for a in r['receipt_artifacts']}):
  path=ROOT/name; exists=path.exists(); dispositions=[]; opens=[]
  if exists:
   d=json.loads(path.read_text(encoding='utf-8'))
   def walk(x):
    if isinstance(x,dict):
     if isinstance(x.get('disposition'),str): dispositions.append(x['disposition'])
     if x.get('open_blocked'): opens.append(x.get('open_blocked'))
     for v in x.values(): walk(v)
    elif isinstance(x,list):
     for v in x: walk(v)
   walk(d)
  bad=sorted(set(x for x in dispositions if x not in allowed));
  if bad: invalid.append({'artifact':name,'invalid_dispositions':bad})
  if opens: blocked.append({'artifact':name,'open_blocked':opens})
  artifact_checks[name]={'exists':exists,'disposition_rows':len(dispositions),'invalid_dispositions':bad,'open_blocked':opens}
 out={'generated_at_utc':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),'inventory_counts':{'registered_sources':inv['registered_pfr_source_count'],'logical_physical_groups':inv['logical_physical_table_count'],'physical_unregistered_groups':inv['physical_unregistered_count']},'coverage_counts':{'groups':len(rows),'receipted':sum(x['coverage_status']=='RECEIPTED' for x in rows),'unmapped':sum(x['coverage_status']=='UNMAPPED' for x in rows),'invalid_artifacts':len(invalid),'blocked_artifacts':len(blocked)},'artifact_checks':artifact_checks,'tables':rows,'notes':['Coverage means a complete column-disposition receipt is linked; it does not erase deferred backfills or semantic candidates.','No raw or release data was modified.']}
 OUT.write_text(json.dumps(out,indent=2),encoding='utf-8'); print(out['coverage_counts'])
if __name__=='__main__':main()
