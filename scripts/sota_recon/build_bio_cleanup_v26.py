"""
build_bio_cleanup_v26.py  --  field-level cleanup of v26 player_bio (validation findings).

Fixes, all conservative (never destroys a value that could be legitimate):
  1. Encoding    : strip Wikipedia footnote markers ([1][2], stored as &#91;1&#93;) and HTML-unescape
                   entities in text fields (career_history, birth_place, high_school, college, conference,
                   status). En-dashes / "3x champion" / curly apostrophes are legit typography and kept.
  2. Impossible DOB: null birth_date where age-at-debut < 18 (physically impossible -> the DOB is a wrong/
                   foreign value, not a real one; nulling also kills the false name+birth_date collisions).
                   NFL debut age has never been < 20, so this cannot catch a real DOB. (Career gaps/comebacks
                   -- e.g. a player returning years later -- are NOT touched; only birth-vs-debut is.)
  3. Impossible draft: where draft_year > first_year (drafted AFTER debut -- impossible, conflated record),
                   null the whole draft block (draft_year/round/overall/nfl_draft_team/age_at_draft).
  4. draft_round : backfill missing draft_round from draft_overall, interpolated per draft_year from that
                   year's own known (overall -> round) boundaries (source-free, stays within observed rounds).

Does NOT touch: career-span/comeback anomalies (legit), the primary_* columns (rebuilt separately after),
or any column not listed above. GATED WRITE: backup + verify row count / id set / untouched columns, then
os.replace. After this, re-run build_primary_team_bio_v26 --apply so primary_* reflects the cleaned fields.

    python -m scripts.sota_recon.build_bio_cleanup_v26            # dry-run (report, no write)
    python -m scripts.sota_recon.build_bio_cleanup_v26 --apply
"""

from __future__ import annotations
import argparse
import html
import os
import re
import shutil
from datetime import datetime, timezone, UTC
from pathlib import Path

import sys

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sota_recon.sources import latest_v26  # noqa: E402

BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")
TEXT_COLS = ["career_history", "birth_place", "high_school", "college", "conference", "status"]
DRAFT_BLOCK = ["draft_year", "draft_round", "draft_overall", "nfl_draft_team", "age_at_draft"]
_FOOT = re.compile(r"\[\d+\]")


def clean_text(x):
    if not isinstance(x, str):
        return x
    s = html.unescape(x)  # &#91; -> [ , &#93; -> ]
    s = _FOOT.sub("", s)  # drop [1][2] Wikipedia footnote markers
    # NB: en-dash (U+2013 in "1992-1994"), U+00D7 ("3x champion") and curly apostrophes are LEGIT -- left as-is.
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s if s else None


def run(apply: bool) -> None:
    con = duckdb.connect()
    bio = con.execute(f"SELECT * FROM '{BIO}'").df()
    n0 = len(bio)
    orig = bio.copy(deep=True)
    fy = pd.to_numeric(bio["first_year"], errors="coerce")

    # 1. encoding
    text_changed = 0
    for c in TEXT_COLS:
        if c not in bio.columns:
            continue
        new = bio[c].map(clean_text)
        text_changed += int((new.fillna("\x00") != bio[c].fillna("\x00")).sum())
        bio[c] = new

    # 2. impossible DOB (age at debut < 18)
    bd = pd.to_datetime(bio["birth_date"], errors="coerce")
    bad_dob = bd.notna() & fy.notna() & ((fy - bd.dt.year) < 18)
    bio.loc[bad_dob, "birth_date"] = None
    bio.loc[bad_dob, "age_at_draft"] = None

    # 3. impossible draft_year (drafted after debut): null ONLY the impossible fields (draft_year +
    #    age_at_draft). Keep draft_round/overall/nfl_draft_team -- the year conflict does not prove the
    #    TEAM/pick is wrong, and nulling them would needlessly drop primary_team coverage. (True identity
    #    de-conflation is a separate pass.)
    dy = pd.to_numeric(bio["draft_year"], errors="coerce")
    bad_draft = dy.notna() & fy.notna() & (dy > fy)
    for c in ("draft_year", "age_at_draft"):
        if c in bio.columns:
            bio.loc[bad_draft, c] = None

    # 4. draft_round backfill from draft_overall, per-year boundaries
    dyr = pd.to_numeric(bio["draft_year"], errors="coerce")
    ov = pd.to_numeric(bio["draft_overall"], errors="coerce")
    rd = pd.to_numeric(bio["draft_round"], errors="coerce")
    fillable = ov.notna() & rd.isna() & dyr.notna()
    filled = 0
    for yr in sorted(dyr[fillable].dropna().unique()):
        yrmask = dyr == yr
        known = yrmask & ov.notna() & rd.notna()
        if not known.any():
            continue
        # round_max[R] = largest overall pick observed in round R that year
        rmax = pd.DataFrame({"r": rd[known], "o": ov[known]}).groupby("r")["o"].max().sort_index()
        need = yrmask & fillable

        def assign(o, rmax=rmax):
            for r, mx in rmax.items():
                if o <= mx:
                    return r
            return rmax.index.max()  # beyond last known boundary -> last round

        vals = ov[need].map(assign)
        bio.loc[need, "draft_round"] = vals.values
        filled += int(need.sum())

    # 5. reconcile IMPOSSIBLE career spans (>25 yrs) against authoritative super-table min/max year.
    #    Only fixes rows whose super career is actually <=25 yrs (bio year is wrong). Legit long careers
    #    -- George Blanda (26 super seasons) and DST team entities (whole-franchise span) -- are untouched
    #    because their SUPER span is also >25, so the guard skips them.
    vv = Path(latest_v26()).as_posix()
    sp = con.execute(
        f"SELECT NFL_player_id, MIN(year) smin, MAX(year) smax FROM '{vv}' "
        f"WHERE NFL_player_id IS NOT NULL GROUP BY 1"
    ).df()
    spmap = {r.NFL_player_id: (int(r.smin), int(r.smax)) for r in sp.itertuples()}
    fyn = pd.to_numeric(bio["first_year"], errors="coerce")
    lyn = pd.to_numeric(bio["last_year"], errors="coerce")
    span_fixed = 0
    for i in bio.index:
        pid = bio.at[i, "NFL_player_id"]
        info = spmap.get(pid)
        if info and pd.notna(fyn[i]) and pd.notna(lyn[i]) and (lyn[i] - fyn[i]) > 25:
            smin, smax = info
            if smax - smin <= 25:  # super proves it's a normal career -> bio year was wrong
                bio.at[i, "first_year"] = smin
                bio.at[i, "last_year"] = smax
                span_fixed += 1

    # ---- report ----
    print(f"player_bio rows: {n0:,}")
    print(f"1. text fields cleaned (footnotes/entities/dash): {text_changed:,} cells")
    print(f"2. impossible DOB nulled (age<18 at debut):       {int(bad_dob.sum()):,}")
    print(f"3. impossible draft block nulled (draft>first):   {int(bad_draft.sum()):,}")
    print(f"4. draft_round backfilled from overall:           {filled:,}")
    print(f"5. impossible career span fixed from super min/max:{span_fixed:,}")

    if not apply:
        # sanity: show a couple cleaned examples
        ex = orig.loc[orig["career_history"].fillna("").str.contains("&#", regex=False), "career_history"].head(1)
        for e in ex:
            print("   e.g. career_history:", repr(e[:90]), "->", repr(clean_text(e)[:90]))
        print("\n(dry-run -- no write. re-run with --apply, then re-run build_primary_team_bio_v26 --apply)")
        return

    # ---- gated write ----
    assert len(bio) == n0, "row count changed"
    assert bio["NFL_player_id"].is_unique, "NFL_player_id not unique"
    assert list(bio.columns) == list(orig.columns), "column set changed"
    touched = set(TEXT_COLS) | {"birth_date", "first_year", "last_year"} | set(DRAFT_BLOCK)
    for c in bio.columns:
        if c in touched:
            continue
        a, b = orig[c].astype(object), bio[c].astype(object)
        same = (a.isna() & b.isna()) | (a == b)
        assert bool(same.all()), f"unintended change in {c}"

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = BIO.with_name(f"player_bio.parquet.bak_cleanup_{ts}")
    shutil.copy2(BIO, backup)
    tmp = BIO.with_suffix(".parquet.tmp")
    con.register("bio_out", bio)
    con.execute(f"COPY bio_out TO '{tmp.as_posix()}' (FORMAT PARQUET)")
    os.replace(tmp, BIO)
    print(f"\nWROTE {BIO}  backup: {backup.name}")
    print(
        "NEXT: python -m scripts.sota_recon.build_primary_team_bio_v26 --apply  (rebuild primary_* on cleaned fields)"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    run(ap.parse_args().apply)
