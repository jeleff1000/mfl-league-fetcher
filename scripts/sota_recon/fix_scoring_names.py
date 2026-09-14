"""
sota_recon/fix_scoring_names.py  --  backfill player names on scoring-decomposition inserts

build_scoring_decomposition inserted ~6,030 player-week rows without a `player` name (the
INSERT omitted the column). The scoring table carries names in description_link_texts
(same order as description_link_ids), and bio has names for known ids. This fills the gap:
name = bio.player (by NFL_player_id) else the scoring link text for that pfr_id. Gated:
nameless scoring rows -> 0, golden 21/21, row count unchanged -> backup + swap.

    python -m scripts.sota_recon.fix_scoring_names --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

SCORING = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/scoring/_combined.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
DS = "pfr_scoring_decomposition_backfill"


def _name_map() -> pd.DataFrame:
    """pfr_id -> name from the scoring table's parallel link id/text lists."""
    sc = pq.read_table(SCORING, columns=["description_link_ids",
                                         "description_link_texts"]).to_pandas()
    sc = sc.dropna(subset=["description_link_ids", "description_link_texts"])
    ids = sc.description_link_ids.str.split(";")
    txt = sc.description_link_texts.str.split(";")
    pairs = []
    for i, t in zip(ids, txt):
        for a, b in zip(i, t):
            if a and b:
                pairs.append((a, b))
    m = pd.DataFrame(pairs, columns=["id", "nm"]).drop_duplicates("id")
    return m


def apply_fix() -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    tmp_spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(tmp_spill, exist_ok=True)
    con = duckdb.connect(os.path.join(tmp_spill, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='5GB'")
    con.execute(f"SET temp_directory='{tmp_spill}'")
    con.register("nm", _name_map())
    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{v26}')")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    nameless0 = con.execute(f"SELECT COUNT(*) FROM st WHERE (player IS NULL OR player='') AND data_source='{DS}'").fetchone()[0]

    # name = bio.player by id, else scoring link text by id
    con.execute(f"""
        UPDATE st SET player = COALESCE(
            (SELECT player FROM read_parquet('{BIO}') b WHERE b.NFL_player_id = st.NFL_player_id LIMIT 1),
            (SELECT nm FROM nm WHERE nm.id = st.NFL_player_id LIMIT 1))
        WHERE (player IS NULL OR player='') AND data_source='{DS}'
    """)
    nameless1 = con.execute(f"SELECT COUNT(*) FROM st WHERE (player IS NULL OR player='') AND data_source='{DS}'").fetchone()[0]
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]

    vpath = Path(v26); tmp = vpath.with_name(vpath.stem + "_nametmp.parquet")
    reader = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), reader.schema)
    for b in reader:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(tmp_spill, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (nameless1 < nameless0)
    res = {"nameless_before": int(nameless0), "nameless_after": int(nameless1),
           "filled": int(nameless0 - nameless1), "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vpath.with_name(vpath.stem + f"_prename_backup_{stamp}.parquet")
        shutil.copy2(vpath, bk); os.replace(tmp, vpath)
        res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_fix()
        print(f"nameless {r['nameless_before']} -> {r['nameless_after']} (filled {r['filled']}); golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r['swapped'] else f"NOT swapped; temp {r['temp']}"))
    else:
        m = _name_map(); print(f"name map: {len(m)} pfr_id->name pairs")
