"""Normalize legacy PFR receipt labels to the governing disposition vocabulary."""
from __future__ import annotations
import json, glob
from pathlib import Path

MAP={
 "CONTEXT_OR_PROVENANCE":"CONTEXT_TO_SEASON_OR_BIO",
 "MAPPED_TO_CANONICAL":"VERIFIED_DIRECT_MAPPING",
 "STRUCTURED_OR_EXCLUDED":"STRUCTURED_WITNESS_REQUIRED",
 "NEW_SUPERTABLE_COLUMN_CANDIDATE":"PROMOTION_CANDIDATE",
 "DEFERRED_BACKFILL_CANDIDATE_OR_ADJUDICATION":"DEFERRED_BACKFILL_CANDIDATE",
 "DIRECT_COUNTER_WITH_DENOMINATOR_CHECK":"VERIFIED_DIRECT_MAPPING",
 "GAME_TO_SEASON_DIRECT_ROLLUP":"VERIFIED_DIRECT_MAPPING",
 "DIRECT_COUNTER":"STRUCTURED_WITNESS_REQUIRED",
 "DERIVED_WITNESS":"VERIFIED_DERIVED_WITNESS",
 "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP":"STRUCTURED_WITNESS_REQUIRED",
 "PROMOTION_CANDIDATE_OR_STRUCTURED_WITNESS":"STRUCTURED_WITNESS_REQUIRED",
 "PROMOTION_CANDIDATE_OR_DEFERRED":"DEFERRED_BACKFILL_CANDIDATE",
 "PHYSICAL_MEASUREMENT_WITNESS":"STRUCTURED_WITNESS_REQUIRED",
}
def main():
 changed=0; files=0
 for raw in glob.glob("docs/audits/pfr*.json"):
  path=Path(raw); data=json.loads(path.read_text(encoding="utf-8")); touched=False
  def walk(x):
   nonlocal changed,touched
   if isinstance(x,dict):
    for key in ("disposition","audit_disposition"):
     old=x.get(key)
     if old in MAP:
      x[key]=MAP[old]; changed+=1; touched=True
     elif old=="EXCLUDED_WITH_REASON":
      reason=str(x.get("reason","")).upper()
      x[key]="VERIFIED_DERIVED_WITNESS" if "DERIVED RATIO" in reason or "RECOMPUTABLE RATE" in reason else "INTENTIONALLY_UNMAPPED_WITH_REASON"
      changed+=1; touched=True
    if x.get("audit_disposition") and x.get("disposition") != x.get("audit_disposition"):
     x["disposition"] = x["audit_disposition"]
     changed += 1; touched = True
    for v in x.values(): walk(v)
   elif isinstance(x,list):
    for v in x: walk(v)
  walk(data)
  if touched:
   path.write_text(json.dumps(data,indent=2),encoding="utf-8"); files+=1
 print({"files_changed":files,"dispositions_normalized":changed})
if __name__=="__main__":main()
