"""Record durable ledger dispositions alongside, and lock candidates into, PFR receipts."""
from __future__ import annotations
import json, glob
from pathlib import Path

LEDGER=Path('scripts/sota_recon/witness_gate/contracts/column_dispositions.v1.json')
def main():
 decisions={x['key']:x for x in json.loads(LEDGER.read_text(encoding='utf-8'))['decisions']}; changed=locks=0
 for raw in glob.glob('docs/audits/pfr*.json'):
  path=Path(raw); data=json.loads(path.read_text(encoding='utf-8')); touched=False
  source_by_file={
   'pfr-player-season-passing-advanced-2025.json':'pfr_passing_adv_season','pfr-player-season-passing-advanced-post-2025.json':'pfr_passing_adv_post',
   'pfr-pfr_adv_defense-2025.json':'pfr_adv_defense','pfr-pfr_adv_defense_post-2025.json':'pfr_adv_defense_post',
   'pfr-player-season-rec-rush-2025.json':'pfr_player_season_rec_rush','pfr-player-season-rec-rush-post-2025.json':'pfr_recrush_post',
   'pfr-player-season-rush-rec-2025.json':'pfr_player_season_rush_rec','pfr-player-season-rush-rec-post-2025.json':'pfr_rushrec_post',
  }
  inferred=source_by_file.get(path.name)
  if inferred and isinstance(data.get('column_dispositions'),list):
   for row in data['column_dispositions']:
    if isinstance(row,dict) and 'source' not in row:
     row['source']=inferred; touched=True
  def walk(x):
   nonlocal changed,locks,touched
   if isinstance(x,dict):
    source=x.get('source'); col=x.get('column')
    if isinstance(source,str) and isinstance(col,str):
     d=decisions.get(f'{source}|*|{col}')
     if d:
      x['ledger_disposition']=d['disposition']; x['ledger_key']=d['key']
      if d['disposition']=='NEW_SUPERTABLE_COLUMN_CANDIDATE' and x.get('disposition')!='PROMOTION_CANDIDATE':
       x['disposition']='PROMOTION_CANDIDATE'; x['ledger_lock']='CANDIDATE_LOCKED'; changed+=1; locks+=1; touched=True
      elif d['disposition']=='EXCLUDED_WITH_REASON' and x.get('disposition') in {'PROMOTION_CANDIDATE','DEFERRED_BACKFILL_CANDIDATE'}:
       x['disposition']='INTENTIONALLY_UNMAPPED_WITH_REASON'; x['ledger_lock']='EXCLUDED_BY_LEDGER'; changed+=1; touched=True
    for v in x.values(): walk(v)
   elif isinstance(x,list):
    for v in x: walk(v)
  walk(data)
  if touched:path.write_text(json.dumps(data,indent=2),encoding='utf-8')
 print({'receipts_changed':changed,'candidate_locks':locks})
if __name__=='__main__':main()
