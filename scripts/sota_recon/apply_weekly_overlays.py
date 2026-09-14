"""THE WEEKLY PROMOTE, step 1: apply every overlay ledger to the weekly plane.

=============================== PASS ECONOMICS ===============================
THE PLANE IS PARTITIONED. The year-parts in weekly_repaired_parts/ ARE the
plane (readers: S.weekly_read_path()). A fix touching two years
rewrites two small files. The 750MB monolith is a PROMOTE-TIME artifact and
the concat REFUSES to run without SOTA_CONCAT_REASON.
A pass rewrites ~750MB / 1.17M rows x 1,082 columns and then pays the gates.
That cost is FIXED -- it does not shrink for a small change. So this file
REFUSES passes that cannot justify it (Joe 2026-08-04: "locked in as a rule
in the file so the file rejects big batches inherently so you cant drift
away"). The guard runs before any work:

  * FEWER THAN MIN_PAYLOAD_CELLS of real repair work  -> REFUSED
  * a pass whose ONLY payload is DERIVATIONS           -> REFUSED
    (derivations ride the next real pass for free -- that is the whole
     point of the registry; giving one its own rewrite is the drift)
  * every refusal names the override:
        SOTA_PASS_REASON='<why this cannot wait>'
  and every accepted pass PRINTS its payload accounting, so the cost and
  the benefit are on the record together.
=============================================================================


RESUMABLE PER-YEAR SHAPE (v4 -- after three OOM/starvation deaths on a
memory-pressured box). The unit of work is ONE YEAR: read the monolith with a
year predicate (row-group pruning), apply that year's overlay edits + the
BOS-1944 relabel, write one small year-part, verify it by KEY READBACK, move
to the next year. A crash costs one year, never the run: on restart,
finished year-parts are skipped (their .ok marker exists).

Years with NO overlay edits still get a part (plain filtered copy) so the
final concat is a pure streaming union of parts -- but they skip the
verification (nothing to verify; identity by construction of the same
filter).

Every step's memory is one year of one plane (~1-2%% of rows) + a 19k-row
overlay table. There is no step that can OOM on a starved box.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
RECEIPT = LAKE / "weekly_promote_apply_receipt.json"

# EIGHT games, not six (Joe: 'lets fix that'): wk6/wk13 phantoms hid in
# single-row weeks -- the classifier saw 7, the first doom-key carried 6,
# the orphan check found 8. Sammy Baugh's field side proves both additions.
BOS_GAMES = [(4, "PHI"), (6, "BKN"), (7, "CRD"), (8, "RAM"), (9, "BKN"),
             (10, "PHI"), (12, "NYG"), (13, "NYG")]

# DH ROW-LOCAL RECOMPUTE (2026-08-03): a rate column on a DOUBLEHEADER row
# cannot be addressed by (pid, yr, wk) overlay keys -- earlier stage-7
# rounds clobbered DH siblings through the week-keyed join. But a licensed
# rate is a FUNCTION of same-row bases, so the applier recomputes it
# row-locally wherever the week has multiple rows: guard>0 and computable
# => the recompute (at declared convention); guard>0 but uncomputable =>
# NULL (honest unknown); guard<=0 => NULL.
import json as _json
_S7REG = Path(__file__).parent / "witness_gate" / "contracts" / "stage7_formulas.v1.json"


def dh_rate_replaces() -> dict[str, str]:
    if not _S7REG.exists():
        return {}
    lic = _json.loads(_S7REG.read_text(encoding="utf-8"))["licensed"]
    out = {}
    for col, spec in lic.items():
        if "OVER (" in spec["expr"]:
            continue
        write = (f"ROUND(({spec['expr']}), 1)"
                 if spec.get("storage") == "1dp" else f"({spec['expr']})")
        out[col] = (f"CASE WHEN ({spec['guard']}) > 0 THEN {write} "
                    f"ELSE NULL END")
    return out


# CLASS-A PURGE (2026-08-03): fabricated zeros -> NULL. These columns have
# NO nonzero anywhere in the plane before 1957 -- the stat wasn't tracked;
# every earlier zero is structural fill, not an observation (Law 3: never
# COALESCE-0 an unknown into a fact). Floors MEASURED from plane + witness
# nonzero minima, receipts in stage7/classA probe logs. nflcom situational/
# splits 1923 'nonzero sacks' = the adjudicated mapping-defect family
# (dissent logged, non-blocking).
PURGE_FLOORS = {"def_sacks": 1957, "def_fumbles_forced": 1957,
                # raised 1957 -> 1970 (2026-08-03 Brown/Sayers audit): the
                # 1950s/60s "witnesses" zero-render the same cells (mutual
                # fabrication, corroboration VOID); first real nonzero
                # density is the 1970s (721 cells). The 5 genuine nonzero
                # 1950s cells survive -- only zeros purge.
                "fumbles_lost": 1970}


# FANTASY_POSITION DERIVATION (Joe 2026-08-04: "fantasy_position needs to
# be 100% populated ... QB, RB, WR, TE, OL, DL, LB, DB, K, P. Or an
# approved combo in an approved order").
# The rules already existed in position_taxonomy.v1.json -- the column was
# 29% populated and its only mapspec pointed at our OWN previous table.
# MEASURED: the taxonomy derivation reproduces 92.8% of stored values, and
# every sampled disagreement is a stored ERROR (strong safeties stored as
# OL, centers as LB, defensive ends as TE). So the taxonomy is canon:
#   fantasy_position = broad(nfl_position), else broad(position),
#   else position itself when already broad; combos map token-wise and
#   emit in the taxonomy's own broad_positions order.
_TAX = Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"


def _combo_rank_sql(token_list_sql: str) -> str:
    """Render a DuckDB list of BROAD tokens as a combo in the LOCKED order.

    THE ONE PLACE a combo is ordered (Joe 2026-08-04). The rank comes from
    COMBO_ORDER in witness_gate/position_taxonomy.py, which is fingerprint-
    pinned against the contract -- so this SQL cannot drift from the law
    without the taxonomy module refusing to import at all.

    NOT broad_positions: that list ends ...OL,DL,LB,DB,K,P and would emit
    OL,K for Groza. The law is QB,RB,WR,TE,K,LB,DL,DB,P,OL.

    DuckDB's list_sort takes no comparator lambda, so rank is imposed with
    ZERO-PADDED PREFIXES that sort lexically, then stripped back off.
    """
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.sota_recon.witness_gate.position_taxonomy import COMBO_ORDER
    rank_when = " ".join(f"WHEN '{b}' THEN '{i:02d}'" for i, b in enumerate(COMBO_ORDER))
    return (
        "list_aggregate(list_transform(list_sort(list_transform(list_distinct("
        f"list_filter({token_list_sql}, y -> y IS NOT NULL)), "
        f"x -> (CASE x {rank_when} ELSE '99' END) || ':' || x)), "
        "z -> split_part(z, ':', 2)), 'string_agg', ',')")


def fantasy_position_expr() -> str | None:
    if not _TAX.exists():
        return None
    tax = _json.loads(_TAX.read_text(encoding="utf-8"))
    d2b, broad = tax["detailed_to_broad"], tax["broad_positions"]

    def case_over(col):
        whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in d2b.items())
        return f"CASE UPPER(TRIM({col})) {whens} ELSE NULL END"

    # combo path: split a multi-token label, map each token to broad, then
    # hand the token list to the locked orderer.
    mapped = case_over("x").replace("UPPER(TRIM(x))", "TRIM(x)")
    combo = _combo_rank_sql(
        f"list_transform(str_split(UPPER(TRIM(t.position)), ','), x -> {mapped})")
    return (f"COALESCE({case_over('t.nfl_position')}, "
            f"{case_over('t.position')}, "
            f"CASE WHEN UPPER(TRIM(t.position)) IN ({', '.join(repr(b) for b in broad)}) "
            f"THEN UPPER(TRIM(t.position)) END, "
            f"{combo})")


# THE DERIVATION REGISTRY (Joe 2026-08-04: "didn't we solve that it
# shouldn't have to be batches?"). A derived column is a FUNCTION of its
# bases -- it should be materialized once and then recomputed automatically
# whenever ANY pass runs, never earn its own plane rewrite. Every entry
# here is applied on EVERY applier pass, so a new derivation costs nothing
# extra once a pass is already happening: bundle, never serialize.
#   name -> (sql expression over t.*, or a callable returning one)
def position_expr() -> str | None:
    """position = broad(nfl_position) -- our own taxonomy, applied to our
    own detailed role.

    MEASURED 2026-08-04: our position column contradicted our own taxonomy
    on 18,282 rows; PFR agreed with the TAXONOMY against our column (3-4
    outside linebackers stored as DL where LOLB/ROLB map to LB). Deriving
    position from nfl_position closes edge 2 and lifts the PFR witness
    agreement from 0.99228 toward unity. Rows whose nfl_position does not
    map keep their stored value -- never overwritten with NULL.
    """
    if not _TAX.exists():
        return None
    tax = _json.loads(_TAX.read_text(encoding="utf-8"))
    d2b, broad = tax["detailed_to_broad"], tax["broad_positions"]
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in d2b.items())
    direct = f"CASE UPPER(TRIM(t.nfl_position)) {whens} END"
    toks = ("list_distinct(list_filter(list_transform("
            "str_split_regex(UPPER(TRIM(t.nfl_position)), '[/,-]'), x -> "
            f"CASE TRIM(x) {whens} END), y -> y IS NOT NULL))")
    unanimous = f"CASE WHEN len({toks}) = 1 THEN {toks}[1] END"
    # DEF rows are the team-defense plane and keep their own label
    return (f"CASE WHEN t.position = 'DEF' THEN t.position "
            f"ELSE COALESCE({direct}, {unanimous}, t.position) END")


def _season_plane() -> str:
    """THE SEASON POSITION DECLARATION -- PFR's own per-season Pos field.

    Joe 2026-08-05: "Its not if PFR carries a line for it with our positons.
    Its if PFR SAYS Travis Hunter is WR,CB in his actual bio for that
    season", and "You see how Deion has that one official WR year? thats how
    we declare dual players" -- PFR reads RCB every year and RCB/WR in 1996.

    NOT v26's season_positions, which this replaces. That column was built
    from STAT-LINE PRESENCE, so appearing in the kicking table declared you
    a kicker: it made Frank Gifford RB,K in 1956 off ONE field-goal attempt,
    while PFR's own pos for that season reads LH even inside the kicking
    table. 1,805 "dual" seasons there; 415 real ones here.
    """
    return (LAKE / "pfr_season_position_declaration.parquet").as_posix()


_WEEKLY_COLS: set[str] = set()


def _weekly_columns() -> set[str]:
    """Columns the weekly plane actually has (cached).

    A licence that names a column this build lacks would make the whole
    derivation a binder error and take the pass down; licences are dropped
    rather than guessed.
    """
    global _WEEKLY_COLS
    if not _WEEKLY_COLS:
        import duckdb as _d
        con = _d.connect()
        try:
            _WEEKLY_COLS = {
                r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet"
                    f"('{S.weekly_read_path()}') LIMIT 0").fetchall()}
        finally:
            con.close()
    return _WEEKLY_COLS


def _refd_cols(sql: str) -> set[str]:
    """Every t.<column> a licence expression depends on."""
    import re as _re
    return set(_re.findall(r"\bt\.([A-Za-z_][A-Za-z0-9_]*)", sql))


def bio_declared_tokens_sql() -> str | None:
    """Roles the IDENTITY RECORD declares for a player, as a DuckDB list.

    Joe 2026-08-04: "His PFR and stuff should already ID him as dual-
    eligible." It does -- bio.nfl_position already reads 'WR,DB' for Travis
    Hunter and 'QB,K' for Blanda, while correctly reading plain 'WR' for
    Larry Fitzgerald. The weekly plane's own nfl_position is single-valued
    for these men, so a plane-only derivation can never see the second role.
    Read the declaration instead of inventing a snap threshold.

    Resolved from bio AT PASS TIME (not hardcoded): bio stays the source of
    truth and a new declaration is picked up by the next pass for free. The
    declared set is small by nature -- it is a curated identity fact, not a
    stat -- so inlining it costs nothing on a 1.17M-row scan.

    CAREER GRAIN, SO IT IS ONLY A CANDIDATE SET. Joe: dual eligibility is
    per season ("Blanda isnt dual eligible each year, like his bears
    years"). Every token returned here still has to be LICENSED by that
    season's own record before it reaches fantasy_position.
    """
    import duckdb as _d
    from pathlib import Path as _P
    try:
        bp = _P(S.PLAYER_BIO.path).as_posix()
    except Exception:
        return None
    try:
        con = _d.connect()
        rows = con.execute(
            f"""SELECT DISTINCT NFL_player_id, nfl_position
                FROM read_parquet('{bp}')
                WHERE nfl_position LIKE '%,%' AND NFL_player_id IS NOT NULL"""
        ).fetchall()
        con.close()
    except Exception:
        return None
    if not rows:
        return None
    whens = []
    for pid, pos in rows:
        toks = [t.strip().upper() for t in str(pos).split(",") if t.strip()]
        if not toks:
            continue
        lst = ", ".join(f"'{t}'" for t in toks)
        whens.append(f"WHEN {pid!r} THEN [{lst}]")
    if not whens:
        return None
    return f"CASE t.NFL_player_id {' '.join(whens)} ELSE [] END"


def fantasy_position_with_usage() -> str | None:
    """fantasy_position = broad position + the SPECIALIST ROLES the player
    actually filled that season.

    COOKIE GILCHRIST (Joe 2026-08-04): 1962 Buffalo -- 1,096 rushing yards
    AND 8 field goals. EVERY witness labels him "FB", including PFR's own
    KICKING table: nobody writes "FB-K". His kicking exists as USAGE, not
    as a label, so a label-only derivation can never see it. Roles are
    therefore derived from what he DID: K when the player attempted kicks
    that season, P when he punted. Season-scoped, so the combo appears in
    exactly the seasons he filled the role -- Gilchrist is RB,K in 1962
    and RB every other year.
    """
    base = position_expr()
    if not base:
        return None
    # THE COLLOQUIAL TEST (Joe 2026-08-04): a role is earned by BEING the
    # team's kicker/punter that season -- primary share AND real volume --
    # never by one attempt. Verified against the record: Blanda, Groza,
    # Cappelletti, Hornung, Summerall, Mingo, Wayne Walker, Gilchrist and
    # Augie Lio qualify; Frank Gifford (2 attempts of his team's 4, 1953),
    # Paul Sheeks and Stan Keck (2 of 2) do not. Tackles never confer a
    # role -- Fitzgerald is not a DB, Slater is not a DB.
    fga = "COALESCE(TRY_CAST(t.fg_att AS DOUBLE), 0)"
    pnt = "COALESCE(TRY_CAST(t.punts AS DOUBLE), 0)"
    # TWO DOORS (Joe 2026-08-04: "does anybody have 10+ attempts but not a
    # majority?" -- yes, 10 seasons incl. Hornung 1962 at 48%, Mingo, Wayne
    # Walker, Jack Spikes). Share alone measures a committee, not the man.
    # A player is a kicker if he was the PRIMARY kicker (majority, >=5) OR
    # kicked real volume regardless of a teammate's share (>=10).
    kicked = (f"CASE WHEN SUM({fga}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 10 "
              f"OR (SUM({fga}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 5 "
              f"AND SUM({fga}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 0.5 * "
              f"NULLIF(SUM({fga}) OVER (PARTITION BY t.nfl_team, t.year), 0)) "
              f"THEN 1 ELSE 0 END")
    punted = (f"CASE WHEN SUM({pnt}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 20 "
              f"OR (SUM({pnt}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 5 "
              f"AND SUM({pnt}) OVER (PARTITION BY t.NFL_player_id, t.year) >= 0.5 * "
              f"NULLIF(SUM({pnt}) OVER (PARTITION BY t.nfl_team, t.year), 0)) "
              f"THEN 1 ELSE 0 END")
    # THE LOCKED ORDER (Joe 2026-08-04). The old code concatenated
    # base-then-K-then-P, which is NOT the law: Groza is K,OL, not OL,K, and
    # Wayne Walker is K,LB, not LB,K. Roles and base go into one token list
    # and the locked orderer ranks them -- the base position gets no
    # positional privilege, because the rank is not primacy.
    # The base is not guaranteed to be ONE token -- position falls back to
    # t.position, which can already carry a combo ('DB,LB', 'WR,TE,DB').
    # Passed whole it ranks as an unknown single token and lands last,
    # emitting DB,LB and even the duplicate P,WR,P. SPLIT FIRST, always,
    # then let distinct+rank do their job.
    tax = _json.loads(_TAX.read_text(encoding="utf-8"))
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in tax["detailed_to_broad"].items())
    base_toks = (f"list_transform(str_split(UPPER(TRIM({base})), ','), "
                 f"x -> CASE TRIM(x) {whens} END)")
    # THE DECLARATION IS PRIMARY (Joe 2026-08-04: "the column that exists is
    # PFR telling us hes dual eligible from his position there"). Roles are
    # READ from season_positions, not inferred from thresholds.
    #
    # MEASURED against the usage rule over 112,600 player-seasons: they agree
    # on 0.99390 of the K bit, and the declaration independently reproduces
    # Gilchrist RB,K in 1962 AND NO OTHER YEAR -- the exact season the
    # colloquial test picked out. Where they part it is 684 seasons the
    # SOURCE calls K and volume misses (backup and low-volume kickers are
    # still kickers) against 3 the other way. So the declaration leads and
    # usage joins it: a role earned by real volume is kept even when the
    # source is silent, which is the only thing the threshold work still
    # has to add.
    # THE WITNESS CEILING (Joe 2026-08-04: "did you fuck anything up ... where
    # your fantasy positions now make someone dual eligible or triple
    # eligible when our witnesses say they are only single eligible? the only
    # time we can make our own rule is kicker").
    #
    # Unioning the plane's OWN label with the declaration over-fired on 2,429
    # player-seasons -- 1,090 emitted WR,DL where the witness says plain WR,
    # 620 emitted QB,RB where it says QB -- because the plane's position
    # column still carries polluted combos and the union re-imported them.
    # Only 1 of those 2,429 was the allowed K.
    #
    # So: WHERE A WITNESS DECLARES THE SEASON, THE DECLARATION IS THE WHOLE
    # TRUTH and the plane's label gets no vote. The plane label is a fallback
    # for seasons no witness covers, never an addition to one.
    #
    # KICKER IS THE ONLY ROLE WE MAY ADD OURSELVES. The punter self-rule is
    # DELETED -- it was a second invented rule and Joe authorized exactly one.
    # P now comes from witnesses alone.
    decl_toks = (f"list_transform(str_split(UPPER(TRIM(sp.sp_pos)), ','), "
                 f"x -> CASE TRIM(x) {whens} END)")
    declared = "sp.sp_pos IS NOT NULL AND TRIM(sp.sp_pos) <> ''"
    k_add = f"[CASE WHEN ({kicked}) = 1 THEN 'K' END]"
    toks = (f"CASE WHEN {declared} THEN list_concat({decl_toks}, {k_add}) "
            f"ELSE list_concat({base_toks}, {k_add}) END")
    return (f"CASE WHEN t.position = 'DEF' THEN t.position "
            f"ELSE {_combo_rank_sql(toks)} END")


DERIVATIONS: dict[str, object] = {"position": position_expr}


def register_derivations() -> dict[str, str]:
    """Resolve every registered derivation to SQL for this pass."""
    out = {}
    fp = fantasy_position_with_usage() or fantasy_position_expr()
    if fp:
        out["fantasy_position"] = f"COALESCE(NULLIF({fp}, ''), t.fantasy_position)"
    for name, spec in DERIVATIONS.items():
        e = spec() if callable(spec) else spec
        if e:
            out[name] = e
    return out


MIN_PAYLOAD_CELLS = 500


def _enforce_pass_economics(con) -> None:
    """Refuse a pass that cannot pay for itself. See PASS ECONOMICS above."""
    import os
    reason = os.environ.get("SOTA_PASS_REASON",
                            os.environ.get("SOTA_SMALL_BATCH_REASON", "")).strip()
    cells = con.execute("SELECT COUNT(*) FROM ov").fetchone()[0]
    derivs = sorted(register_derivations())
    doom = 0
    dp = LAKE / "season_conservation_doom.parquet"
    if dp.exists():
        doom = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{dp.as_posix()}')"
        ).fetchone()[0]
    payload = cells + doom
    _stage(f"pass payload: {cells} car cells + {doom} doom rows "
           f"+ {len(derivs)} derivations {derivs}")
    if reason:
        _stage(f"pass justified: {reason}")
        return
    if payload < MIN_PAYLOAD_CELLS:
        raise SystemExit(
            f"PASS REFUSED (economics): payload is {payload} cells -- below "
            f"MIN_PAYLOAD_CELLS={MIN_PAYLOAD_CELLS}. A pass costs a full "
            f"plane rewrite + gates regardless of size. Accumulate work, or "
            f"set SOTA_PASS_REASON='<why this cannot wait>'.")
    if payload == 0 and derivs:
        raise SystemExit(
            "PASS REFUSED (economics): the only payload is DERIVATIONS "
            f"{derivs}. Derivations recompute on every real pass for free -- "
            "giving one its own rewrite is exactly the drift the registry "
            "exists to prevent. Wait for real work, or set SOTA_PASS_REASON.")


def _stage(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build(con: duckdb.DuckDBPyConnection) -> dict:
    src = Path(S.latest_v26())
    out = src.with_name(src.stem + ".repaired_20260803" + src.suffix)
    parts = src.parent / "weekly_repaired_parts"
    parts.mkdir(exist_ok=True)

    overlays = sorted(LAKE.glob("weekly*overlay*.parquet")) + sorted(
        (LAKE / "engine_overlays").glob("*.parquet"))
    assert overlays, "no overlays found"
    ov_union = " UNION ALL ".join(
        f"SELECT * FROM read_parquet('{p.as_posix()}')" for p in overlays)
    _stage("loading overlays")
    con.execute(f"CREATE OR REPLACE TEMP TABLE ov AS {ov_union}")
    dup = con.execute("""
    SELECT COUNT(*) FROM (
      SELECT NFL_player_id, year, week, column_name FROM ov
      GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"{dup} cells edited by more than one repair -- refuse"
    # THE ECONOMY LAW (Joe 2026-08-04): a landing costs a full ~750MB plane
    # rewrite + gates no matter how small the change. Micro-batches are
    # therefore REFUSED -- accumulate work, or state why this one cannot
    # wait (SOTA_SMALL_BATCH_REASON). Renames/purges/doom paths write
    # without cars and set their own reason.
    _enforce_pass_economics(con)
    cols = [r[0] for r in con.execute(
        "SELECT DISTINCT column_name FROM ov").fetchall()]
    edit_years = {int(r[0]) for r in con.execute(
        "SELECT DISTINCT year FROM ov").fetchall()} | {2001, 2002}
    all_years = sorted(int(r[0]) for r in con.execute(
        f"SELECT DISTINCT year FROM read_parquet('{src.as_posix()}') "
        f"WHERE year IS NOT NULL").fetchall())
    # purge years must take the REPLACE path, never the plain copy
    max_floor = max(PURGE_FLOORS.values())
    edit_years |= {y for y in all_years if y < max_floor}
    # season-conservation doom list: (pid, year, column) whose ZERO weekly
    # cells are unverifiable against a witnessed season total
    doom_path = LAKE / "season_conservation_doom.parquet"
    doom_cols_yr: dict[int, set] = {}
    if doom_path.exists():
        for y, c in con.execute(f"""SELECT DISTINCT CAST(year AS INT),
            column_name FROM read_parquet('{doom_path.as_posix()}')""").fetchall():
            doom_cols_yr.setdefault(int(y), set()).add(c)
        edit_years |= set(doom_cols_yr)
        _stage(f"season-conservation doom: {len(doom_cols_yr)} years")
    # CATALOG CONTEXT REPAIR (2026-08-04): the game catalog is authoritative
    # (standing law); rows whose opponent/date/margin disagree with it are
    # repaired by catalog-anchored ABSOLUTE assignment (the JAX-mirror
    # pattern -- idempotent, never a toggle). Text columns cannot ride the
    # numeric overlay cars. Landing proof = catalog_identity_check >= 0.999.
    games_cat = Path(S.TEAM_GAMES.path).as_posix()
    # CTX REPAIR FROZEN + RESTORE MODE (2026-08-04): the residual opponent
    # mismatches are SYSTEMATIC WEEK-MISALIGNMENTS in ancient seasons (CHI
    # 1940s CRD<->DET in bulk) -- "same (team, year, week) = same game" is
    # FALSE there, so batch-26's ctx writes stamped wrong-game context onto
    # misaligned rows. RESTORE: context columns return to the pre_batch5
    # backup values (exact row addressing incl. team); the receipted JAX
    # mirror and BOS relabel re-derive themselves after. Week alignment is
    # an AUDIT (ESPN-legacy-week-units class), never a same-week car.
    # RE-LICENSED 2026-08-04: scoped to ALIGNED team-seasons per the
    # week-alignment audit (2,254 aligned / 55 scrambled). The batch-26
    # corruption is impossible under this scope.
    CTX_REPAIR_ENABLED = True
    align_map = LAKE / "week_alignment_map.parquet"
    backup = src.with_name(src.stem + ".pre_batch5" + src.suffix)
    if CTX_REPAIR_ENABLED:
        con.execute("""CREATE OR REPLACE TEMP TABLE ctx_restore AS
        SELECT NULL AS NFL_player_id, NULL AS year, NULL AS week,
               NULL AS team, NULL AS orig_opp, NULL AS orig_date,
               NULL AS orig_margin WHERE FALSE""")
    else:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE ctx_restore AS
        SELECT NFL_player_id, CAST(year AS INT) AS year,
               TRY_CAST(week AS INT) AS week, nfl_team AS team,
               opponent_nfl_team AS orig_opp, game_date AS orig_date,
               TRY_CAST(game_margin AS DOUBLE) AS orig_margin
        FROM read_parquet('{backup.as_posix()}')
        WHERE season_type = 'REG'
        QUALIFY COUNT(*) OVER (PARTITION BY NFL_player_id, year, week,
                               nfl_team) = 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ctx_doom AS
    SELECT t.NFL_player_id, CAST(t.year AS INT) AS year,
           TRY_CAST(t.week AS INT) AS week, t.nfl_team AS team,
           c.opponent_code AS true_opp,
           CAST(TRY_CAST(c.game_date AS DATE) AS VARCHAR) AS true_date,
           TRY_CAST(c.team_points AS DOUBLE)
             - TRY_CAST(c.opponent_points AS DOUBLE) AS true_margin,
           TRY_CAST(c2.team_fid AS DOUBLE) AS true_opp_fid
    FROM read_parquet('{src.as_posix()}') t
    JOIN (SELECT * FROM '{games_cat}' WHERE season_type='REG') c
      ON c.team_code = t.nfl_team AND CAST(c.year AS INT) = CAST(t.year AS INT)
      AND TRY_CAST(c.week AS INT) = TRY_CAST(t.week AS INT)
    JOIN (SELECT team, yr FROM read_parquet('{align_map.as_posix()}')
          WHERE verdict = 'ALIGNED') al
      ON al.team = t.nfl_team AND al.yr = CAST(t.year AS INT)
    LEFT JOIN (SELECT * FROM '{games_cat}' WHERE season_type='REG') c2
      ON c2.team_code = c.opponent_code
      AND CAST(c2.year AS INT) = CAST(t.year AS INT)
      AND TRY_CAST(c2.week AS INT) = TRY_CAST(t.week AS INT)
    WHERE t.season_type = 'REG'
      AND ((t.opponent_nfl_team IS NOT NULL
            AND LOWER(TRIM(t.opponent_nfl_team)) <> LOWER(TRIM(c.opponent_code)))
        OR (t.game_date IS NOT NULL
            AND CAST(TRY_CAST(t.game_date AS DATE) AS VARCHAR)
                <> CAST(TRY_CAST(c.game_date AS DATE) AS VARCHAR))
        OR (t.game_margin IS NOT NULL
            AND ABS(TRY_CAST(t.game_margin AS DOUBLE)
                    - (TRY_CAST(c.team_points AS DOUBLE)
                       - TRY_CAST(c.opponent_points AS DOUBLE))) > 0.001))
    QUALIFY COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.year, t.week,
                           t.nfl_team) = 1""")
    n_ctx = con.execute("SELECT COUNT(*) FROM ctx_doom").fetchone()[0]
    ctx_years = {int(r[0]) for r in con.execute(
        "SELECT DISTINCT year FROM ctx_doom").fetchall()}
    if not CTX_REPAIR_ENABLED:
        con.execute("DELETE FROM ctx_doom")
        n_ctx, ctx_years = 0, set()
        restore_years = {int(r[0]) for r in con.execute(
            "SELECT DISTINCT year FROM ctx_restore").fetchall()}
        edit_years |= restore_years
        _stage(f"ctx FROZEN; context RESTORE from pre_batch5 across "
               f"{len(restore_years)} years")
    else:
        _stage(f"catalog context repair: {n_ctx} rows across {len(ctx_years)} years")
        edit_years |= ctx_years
    # DH years too: rate columns recompute row-locally on multi-row weeks
    dh_cols = dh_rate_replaces()
    derivations = register_derivations()
    if derivations:
        edit_years |= set(all_years)     # derivations touch every year
        _stage(f"derivation registry active: {sorted(derivations)}")
    if dh_cols:
        dh_years = {int(r[0]) for r in con.execute(f"""
        SELECT DISTINCT year FROM (
          SELECT year, COUNT(*) OVER (
            PARTITION BY NFL_player_id, year, week) AS n
          FROM read_parquet('{src.as_posix()}')
          WHERE season_type = 'REG' AND year IS NOT NULL)
        WHERE n > 1""").fetchall()}
        _stage(f"DH row-local recompute years: {sorted(dh_years)}")
        edit_years |= dh_years
    # exact-count gate: expected purge cells per column, measured up front
    purge_expected = {}
    for c, floor in PURGE_FLOORS.items():
        purge_expected[c] = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')
        WHERE CAST(year AS INT) < {floor}
          AND TRY_CAST({c} AS DOUBLE) = 0""").fetchone()[0]
    _stage(f"Class-A purge expected: {purge_expected}")

    games = Path(S.TEAM_GAMES.path).as_posix()
    was_fid = con.execute(f"""SELECT ANY_VALUE(team_fid) FROM '{games}'
        WHERE team_code='WAS' AND year=1944""").fetchone()[0]
    doom = " OR ".join(
        f"(t.week={w} AND t.opponent_nfl_team='{o}')" for w, o in BOS_GAMES)
    bos = (f"(t.season_type='REG' AND t.year=1944 AND t.nfl_team='BOS' "
           f"AND ({doom}))")

    # every branch cast: round-2's 180 columns include BOOLEAN-typed plane
    # columns and CASE refuses mixed types
    case = " ".join(f"WHEN '{c}' THEN TRY_CAST(a.{c} AS DOUBLE)"
                    for c in cols)
    # DERIVATION-AWARE RESUME MARKERS (2026-08-04). The .ok marker exists so
    # a crash costs one year, not the run. But a bare marker also skipped
    # years when the DERIVATION SQL had changed -- so a rule fix silently
    # landed nothing and the pass "succeeded" in 7 seconds. That defeats the
    # registry's whole premise (derivations recompute on every pass). The
    # marker now carries a fingerprint of the resolved derivation SQL: same
    # rules => genuine resume, changed rules => that year rebuilds.
    import hashlib as _h
    from scripts.sota_recon.witness_gate import position_law as _law
    # The fingerprint must cover the derivation's INPUTS, not just its text.
    # Switching the declaration source from season_positions to PFR's own
    # per-season Pos changed no SQL string -- the expression still reads
    # sp.sp_pos -- so the markers survived and a second pass again landed
    # nothing in 10 seconds. Any change to the declaration file (path OR
    # contents) now invalidates every year.
    _decl = Path(_season_plane())
    _decl_hash = _law.content_sha256(_decl) if _decl.exists() else None
    _law_hash = _law.input_fingerprint([
        Path(_law.__file__),
        Path(__file__).parent / "witness_gate" / "position_taxonomy.py",
        Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json",
    ])
    deriv_fp = _h.sha256(
        json.dumps({"exprs": {k: str(v) for k, v in sorted(derivations.items())},
                    "declaration_sha256": _decl_hash,
                    "law_input_fingerprint": _law_hash},
                   sort_keys=True).encode()).hexdigest()[:16]
    _stage(f"derivation fingerprint {deriv_fp}")
    # THE LAW MUST BE WATCHED REFUSING BEFORE WE TRUST IT (Joe 2026-08-05:
    # "A rule on editing the file itself that cant be broken by anyone").
    # If the position gate has been stubbed, loosened or deleted, this
    # raises and NO part is written -- you cannot turn the law off and
    # still produce a plane.
    _law.self_test(con)
    _season_p = _season_plane()
    _stage("position law armed (5 article poisons refused; content-fingerprinted)")
    per_year_cells, skipped = {}, 0
    for yr in all_years:
        part = parts / f"year={yr}.parquet"
        okmark = parts / f"year={yr}.ok"
        # Overlay-aware invalidation: derivation-only markers are insufficient
        # because a newly added overlay must rebuild an otherwise identical
        # year part.  Keep the older markers reusable for ordinary reruns, but
        # force a pass when the canonical pass-success migration is present.
        overlay_rebuild = any("pass_success_down_distance" in p.name
                              for p in overlays)
        if okmark.exists() and part.exists() and not overlay_rebuild:
            prev = okmark.read_text(encoding="utf-8").strip()
            if prev.endswith(f"deriv={deriv_fp}"):
                skipped += 1
                continue
        if yr not in edit_years:
            con.execute(f"""
            COPY (SELECT * FROM read_parquet('{src.as_posix()}')
                  WHERE year = {yr})
            TO '{part.as_posix()}' (FORMAT parquet)""")
            _law.assert_frame(
                con, f"read_parquet('{part.as_posix()}')",
                season_plane=_season_p, label=f"year={yr} (plain)")
            okmark.write_text(f"plain deriv={deriv_fp}", encoding="utf-8")
            continue
        # ONE join regardless of column count (a 180-join query OOM'd
        # batch 3): the year's overlay pivots wide, then a single LEFT JOIN.
        cols_yr = [r[0] for r in con.execute(
            f"SELECT DISTINCT column_name FROM ov WHERE year = {yr}"
        ).fetchall()]
        pivot = ", ".join(
            f"MAX(CASE WHEN column_name = '{c}' THEN new_value END) AS pv_{c}"
            for c in cols_yr)
        if cols_yr:
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ovw AS
            SELECT NFL_player_id, week, {pivot}
            FROM ov WHERE year = {yr} GROUP BY 1, 2""")
        else:
            con.execute("""CREATE OR REPLACE TEMP TABLE ovw AS
            SELECT NULL AS NFL_player_id, NULL AS week WHERE FALSE""")
        purge_yr = [c for c, fl in PURGE_FLOORS.items() if yr < fl]
        emap = {}
        for c in cols_yr:
            if c in purge_yr:
                # overlay wins (witnessed repair); else fabricated 0 -> NULL
                emap[c] = (
                    f"CASE WHEN o.pv_{c} IS NOT NULL THEN o.pv_{c} "
                    f"WHEN TRY_CAST(t.{c} AS DOUBLE) = 0 THEN NULL "
                    f"ELSE TRY_CAST(t.{c} AS DOUBLE) END")
            else:
                emap[c] = f"COALESCE(o.pv_{c}, TRY_CAST(t.{c} AS DOUBLE))"
        for c in purge_yr:
            if c not in cols_yr:
                emap[c] = f"NULLIF(TRY_CAST(t.{c} AS DOUBLE), 0)"
        # SEASON-CONSERVATION DOOM (2026-08-03, Brown/Sayers spot-check): a
        # weekly ZERO contradicting a witnessed nonzero season total is
        # unverifiable -> NULL. Doom list keyed (pid, year, column); only
        # ZERO cells in listed player-seasons are touched.
        for c in doom_cols_yr.get(yr, ()):
            base = emap.get(c, f"TRY_CAST(t.{c} AS DOUBLE)")
            # overlay wins over doom -- an overlay-written cell is a
            # RECEIPTED value (batch 14 readback caught doom re-NULLing an
            # engine-written in-era zero on idempotent re-application)
            ov_guard = (f"o.pv_{c} IS NULL AND " if c in cols_yr else "")
            emap[c] = (
                f"CASE WHEN {ov_guard}d_{c}.NFL_player_id IS NOT NULL "
                f"AND TRY_CAST(t.{c} AS DOUBLE) = 0 THEN NULL "
                f"ELSE {base} END")
        # every registered derivation, every pass -- bundled by design
        for _dc, _dx in derivations.items():
            emap[_dc] = _dx
        if derivations:
            replaces = ", ".join(f"{e} AS {c}" for c, e in emap.items())
        # DH rows: rate columns recompute row-locally, overriding everything
        dh = "COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.week) > 1"
        for c, dhx in dh_cols.items():
            base = emap.get(c, f"TRY_CAST(t.{c} AS DOUBLE)")
            emap[c] = f"CASE WHEN {dh} THEN {dhx} ELSE {base} END"
        replaces = ", ".join(f"{e} AS {c}" for c, e in emap.items()) \
            or "t.year AS year"
        joins = "LEFT JOIN ovw o USING (NFL_player_id, week)"
        # SEASON-GRAIN POSITION DECLARATION (Joe 2026-08-04: "His PFR and
        # stuff should already ID him as dual-eligible"). It does, and at
        # SEASON grain: v26 season.season_positions reads 'WR,DB' for Travis
        # Hunter 2025, 'RB,K' for Gilchrist in 1962 AND NO OTHER YEAR, and
        # 'K,OL' for Groza. The weekly plane's own nfl_position is single-
        # valued, so this is the only place the second role is visible.
        # Season grain is what makes Joe's per-season law enforceable
        # without smearing a career role across every year.
        joins += (f" LEFT JOIN (SELECT NFL_player_id AS sp_pid, "
                  f"CAST(year AS INT) AS sp_yr, declared AS sp_pos "
                  f"FROM read_parquet('{_season_plane()}') "
                  f"WHERE CAST(year AS INT) = {yr} AND declared "
                  f"IS NOT NULL) sp ON sp.sp_pid = t.NFL_player_id")
        # catalog context repair for this year's mismatching rows
        if yr in ctx_years:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE ctx_y AS
            SELECT NFL_player_id, week, team, true_opp, true_date,
                   true_margin, true_opp_fid
            FROM ctx_doom WHERE year = {yr}""")
            # TEAM in the join key: exact row addressing or no repair
            joins += (" LEFT JOIN ctx_y cx ON cx.NFL_player_id = "
                      "t.NFL_player_id AND cx.week = TRY_CAST(t.week AS INT)"
                      " AND cx.team = t.nfl_team")
            for cc, ex in (("opponent_nfl_team", "cx.true_opp"),
                           ("game_date", "TRY_CAST(cx.true_date AS TIMESTAMP)"),
                           ("game_margin", "cx.true_margin"),
                           ("opponent_nfl_franchise_number",
                            "cx.true_opp_fid")):
                base = emap.get(cc, f"t.{cc}")
                emap[cc] = (f"CASE WHEN cx.NFL_player_id IS NOT NULL "
                            f"AND {ex} IS NOT NULL THEN {ex} "
                            f"ELSE {base} END")
            replaces = ", ".join(f"{e} AS {c}" for c, e in emap.items())
        # RESTORE path (dormant while ctx is licensed): context columns
        # return to backup values, exact keys
        if not CTX_REPAIR_ENABLED:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE ctxr_y AS
            SELECT NFL_player_id, week, team, orig_opp, orig_date, orig_margin
            FROM ctx_restore WHERE year = {yr}""")
            joins += (" LEFT JOIN ctxr_y cr ON cr.NFL_player_id = "
                      "t.NFL_player_id AND cr.week = TRY_CAST(t.week AS INT)"
                      " AND cr.team = t.nfl_team")
            for cc, ex in (("opponent_nfl_team", "cr.orig_opp"),
                           ("game_date", "TRY_CAST(cr.orig_date AS TIMESTAMP)"),
                           ("game_margin", "cr.orig_margin")):
                base = emap.get(cc, f"t.{cc}")
                emap[cc] = (f"CASE WHEN cr.NFL_player_id IS NOT NULL "
                            f"THEN {ex} ELSE {base} END")
            replaces = ", ".join(f"{e} AS {c}" for c, e in emap.items())

        for c in doom_cols_yr.get(yr, ()):
            con.execute(f"""CREATE OR REPLACE TEMP TABLE doomt_{c} AS
            SELECT DISTINCT NFL_player_id
            FROM read_parquet('{doom_path.as_posix()}')
            WHERE CAST(year AS INT) = {yr} AND column_name = '{c}'""")
            joins += (f" LEFT JOIN doomt_{c} d_{c} "
                      f"ON d_{c}.NFL_player_id = t.NFL_player_id")
        relabel = ""
        if yr == 1944:
            n_bos = con.execute(f"""SELECT COUNT(*)
                FROM read_parquet('{src.as_posix()}') t
                WHERE t.year = 1944 AND {bos}""").fetchone()[0]
            # idempotent: 25 = first application, 0 = already applied
            assert n_bos in (25, 0), f"BOS doom matched {n_bos}: neither state"
            if n_bos:
                relabel = (f", CASE WHEN {bos} THEN 'WAS' ELSE t.nfl_team END"
                           f" AS nfl_team, CASE WHEN {bos} THEN {was_fid}"
                           f" ELSE t.nfl_franchise_number"
                           f" END AS nfl_franchise_number")
        if yr in (2001, 2002):
            # the nflverse JAX opponent-swap mirror, re-verified per row:
            # plane team must equal the witness team's catalog opponent.
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE doom2 AS
            SELECT m.NFL_player_id, m.year, m.week,
                   m.witness_teams AS true_team, m.plane_team AS wrong_team,
                   c.team_fid AS true_fid, c2.team_fid AS wrong_fid
            FROM read_parquet(
              '{(LAKE / "roster_misallocations_1980_2025.parquet").as_posix()}') m
            JOIN '{games}' c ON c.team_code = m.witness_teams
                AND c.year = m.year AND TRY_CAST(c.week AS INT) = m.week
                AND c.season_type='REG' AND c.opponent_code = m.plane_team
            JOIN '{games}' c2 ON c2.team_code = m.plane_team
                AND c2.year = m.year AND TRY_CAST(c2.week AS INT) = m.week
                AND c2.season_type='REG'
            WHERE m.plane_team IS NOT NULL AND m.year = {yr}""")
            # ABSOLUTE assignments from catalog truth -- the swap form
            # TOGGLED on re-application: batch 3 re-ran the mirror and
            # un-swapped batch 2's scores (caught by the before/after proof
            # pack, missed by the team-only gate). Assignments are idempotent.
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE doom3 AS
            SELECT d.*, c3.team_points AS true_pts, c3.opponent_points
                   AS true_opp_pts,
                   CASE WHEN c3.is_home=1 THEN 'home' ELSE 'away' END
                       AS true_side,
                   CASE WHEN c3.result='W' THEN 1 WHEN c3.result='L' THEN 0
                        ELSE NULL END AS true_win
            FROM doom2 d
            JOIN '{games}' c3 ON c3.team_code = d.true_team
                AND c3.year = d.year AND TRY_CAST(c3.week AS INT) = d.week
                AND c3.season_type='REG'""")
            con.execute("DROP TABLE doom2")
            con.execute("ALTER TABLE doom3 RENAME TO doom2")
            mirror = """,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.true_team
                   ELSE t.nfl_team END AS nfl_team,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.wrong_team
                   ELSE t.opponent_nfl_team END AS opponent_nfl_team,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.true_fid
                   ELSE t.nfl_franchise_number END AS nfl_franchise_number,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.wrong_fid
                   ELSE t.opponent_nfl_franchise_number
                   END AS opponent_nfl_franchise_number,
              CASE WHEN d.NFL_player_id IS NOT NULL
                   THEN TRY_CAST(d.true_pts AS DOUBLE)
                   ELSE TRY_CAST(t.team_points AS DOUBLE) END AS team_points,
              CASE WHEN d.NFL_player_id IS NOT NULL
                   THEN TRY_CAST(d.true_opp_pts AS DOUBLE)
                   ELSE TRY_CAST(t.opponent_points AS DOUBLE)
                   END AS opponent_points,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.true_side
                   ELSE t.home_away END AS home_away,
              CASE WHEN d.NFL_player_id IS NOT NULL THEN d.true_win
                   ELSE TRY_CAST(t.is_win AS INT) END AS is_win,
              CASE WHEN d.NFL_player_id IS NOT NULL
                   THEN TRY_CAST(d.true_pts AS DOUBLE)
                        - TRY_CAST(d.true_opp_pts AS DOUBLE)
                   ELSE TRY_CAST(t.game_margin AS DOUBLE) END AS game_margin"""
            # the JAX mirror already carries catalog truth for these
            # columns row-exactly; ctx duplicates are dropped for 2001-02
            for _mc in ("opponent_nfl_team", "game_margin",
                        "opponent_nfl_franchise_number"):
                emap.pop(_mc, None)
            replaces = ", ".join(f"{e} AS {c}" for c, e in emap.items())                 or "t.year AS year"
            con.execute(f"""
            COPY (
              SELECT t.* REPLACE ({replaces}{mirror})
              FROM read_parquet('{src.as_posix()}') t
              {joins}
              LEFT JOIN doom2 d USING (NFL_player_id, year, week)
              WHERE t.year = {yr}
            ) TO '{part.as_posix()}' (FORMAT parquet)""")
            fixed = con.execute(f"""
            SELECT COUNT(*) FROM doom2 d
            JOIN read_parquet('{part.as_posix()}') a
              USING (NFL_player_id, year, week)
            WHERE a.nfl_team = d.true_team
              AND TRY_CAST(a.team_points AS DOUBLE) = d.true_pts
              AND TRY_CAST(a.is_win AS INT) IS NOT DISTINCT FROM d.true_win
              AND LOWER(a.home_away) = d.true_side""").fetchone()[0]
            n_doom = con.execute("SELECT COUNT(*) FROM doom2").fetchone()[0]
            assert fixed == n_doom, f"{yr} mirror: {fixed}/{n_doom}"
            print(f"  {yr}: JAX mirror applied to {fixed} rows", flush=True)
        else:
            con.execute(f"""
            COPY (
              SELECT t.* REPLACE ({replaces}{relabel})
              FROM read_parquet('{src.as_posix()}') t
              {joins}
              WHERE t.year = {yr}
            ) TO '{part.as_posix()}' (FORMAT parquet)""")
        # key readback for this year's overlay cells -- exact per cell
        total, exact = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE
          COALESCE(TRY_CAST(CASE ov.column_name {case} END AS DOUBLE), -1e18)
          = COALESCE(ov.new_value, -1e18))
        FROM ov JOIN read_parquet('{part.as_posix()}') a
          USING (NFL_player_id, week)
        WHERE ov.year = {yr}""").fetchone()
        assert exact == total, (
            f"{yr}: {total - exact} of {total} cells failed readback")
        if yr == 1944:
            # MODERNIZED (2026-08-04): the original pattern-match gate
            # false-positived once ctx repair normalized the real Yanks'
            # opponent codes to catalog form. A phantom is a doom-matching
            # row with NO catalog game -- a real Yanks game that matches
            # the catalog is the record, not a phantom.
            remaining = con.execute(f"""SELECT COUNT(*)
                FROM read_parquet('{part.as_posix()}') t
                WHERE {bos}
                  AND NOT EXISTS (
                    SELECT 1 FROM '{games}' g
                    WHERE g.team_code = t.nfl_team
                      AND CAST(g.year AS INT) = 1944
                      AND TRY_CAST(g.week AS INT) = TRY_CAST(t.week AS INT)
                      AND g.opponent_code = t.opponent_nfl_team
                      AND g.season_type = 'REG')""").fetchone()[0]
            assert remaining == 0, f"{remaining} BOS phantoms survived"
        # purge gate: no unwitnessed zero survives pre-floor -- the only
        # zeros allowed in a purged column-year are overlay-written ones
        for c in purge_yr:
            part_zero = con.execute(f"""SELECT COUNT(*)
                FROM read_parquet('{part.as_posix()}')
                WHERE TRY_CAST({c} AS DOUBLE) = 0""").fetchone()[0]
            ov_zero = con.execute(f"""SELECT COUNT(*) FROM ov
                WHERE year = {yr} AND column_name = '{c}'
                  AND new_value = 0""").fetchone()[0]
            assert part_zero == ov_zero, (
                f"{yr} {c}: {part_zero} zeros survived purge "
                f"({ov_zero} overlay-licensed)")
        per_year_cells[yr] = total
        # CHOKE POINT: a part that breaks the position law never gets
        # marked done, so a violating plane cannot come into existence.
        _law.assert_frame(
            con, f"read_parquet('{part.as_posix()}')",
            season_plane=_season_p, label=f"year={yr}")
        okmark.write_text(f"verified {total} deriv={deriv_fp}", encoding="utf-8")
        _stage(f"year {yr}: {total} cells applied + readback-exact")

    # PARTITIONED: skip the concat entirely -- the parts ARE the plane.
    # (Joe 2026-08-04: "why isn't this landed" -- because the concat is a
    # 5-minute tax that produces nothing new. When the marker is present the
    # pass ends at the parts, and the monolith is rebuilt once at promote.)
    if (parts / "PARTITIONED").exists():
        n_parts = len(list(parts.glob("year=*.parquet")))
        _stage(f"PARTITIONED: concat skipped, {n_parts} year-parts ARE the plane")
        receipt = {"wave": "weekly_promote_apply_batch5", "date": "2026-08-04",
                   "architecture": "partitioned (no concat)",
                   "source_plane": str(src), "parts_dir": str(parts),
                   "partitioned": True, "parts": n_parts,
                   "overlays": [pp.name for pp in overlays],
                   "derivations": sorted(derivations),
                   "gates": "per-year key readback exact; parts are canonical"}
        RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                           encoding="utf-8")
        return receipt
    # THE CONCAT IS NOW THE EXCEPTIONAL PATH (2026-08-04). Building the
    # 750MB monolith is only required at PROMOTE. Doing it per pass is the
    # 5-minute tax that made "simple fixes" slow -- and the mistake was
    # repeatable: batch 33 ran a full monolith pass while partitioned mode
    # sat built-but-unflipped. So the concat must now JUSTIFY ITSELF.
    import os as _os
    _cr = _os.environ.get("SOTA_CONCAT_REASON", "").strip()
    if not _cr:
        raise SystemExit(
            "CONCAT REFUSED: the monolith is a promote-time artifact, not a "
            "per-pass one. The year-parts are complete and readable via "
            "S.weekly_read_path(). Mark the parts PARTITIONED to use "
            "them directly, or set SOTA_CONCAT_REASON='promote|<why>' if a "
            "single file is genuinely required now.")
    _stage(f"concat justified: {_cr}")
    _stage("concat: streaming union of parts")
    # COLUMN RENAMES RIDE THE CONCAT (2026-08-04, Joe: "this is obviously a
    # poor way to do this"). A standalone rename rewrote 1.17M rows x 1,082
    # columns for two names. The applier ALREADY rewrites the whole plane
    # every batch -- so renames are a PROJECTION on a write we are already
    # paying for: zero extra I/O. Parts keep the OLD names (overlay cars key
    # on them); the rename happens once, at the union.
    RENAMES = {"total_tds_scored": "total_tds_scored",
               "total_tds_accounted_for": "total_tds_accounted_for"}
    part0 = (parts / f"year={all_years[0]}.parquet").as_posix()
    part_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{part0}') LIMIT 0").fetchall()]
    RENAMES = {k: v for k, v in RENAMES.items() if k in part_cols}
    if RENAMES:
        # already-renamed parts are a NO-OP, not a failure: the guard fired
        # on batch 33 because batch 32 had already renamed. Idempotence.
        RENAMES = {k: v for k, v in RENAMES.items()
                   if v not in part_cols and k in part_cols}
        proj = ", ".join(f'"{c}" AS "{RENAMES[c]}"' if c in RENAMES
                         else f'"{c}"' for c in part_cols)   # ORDER KEPT
        _stage(f"concat carries renames: {RENAMES}")
    else:
        proj = "*"
    union = " UNION ALL ".join(
        f"SELECT {proj} FROM read_parquet('{(parts / f'year={y}.parquet').as_posix()}')"
        for y in all_years)
    # ROW_GROUP_SIZE (2026-08-04): the concat OOM'd on batch 31 -- default
    # groups buffer ~123k rows x 1,082 columns (~1GB) before flushing. The
    # campaign learned this on the year-parts writes; the concat never got
    # the same treatment.
    con.execute(f"""COPY ({union}) TO '{out.as_posix()}'
        (FORMAT parquet, ROW_GROUP_SIZE 20000)""")
    n_in = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')"
                       ).fetchone()[0]
    n_out = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')"
                        ).fetchone()[0]
    null_year = con.execute(f"""SELECT COUNT(*)
        FROM read_parquet('{src.as_posix()}') WHERE year IS NULL""").fetchone()[0]
    assert n_in == n_out + null_year, (
        f"rows {n_in} -> {n_out} (+{null_year} null-year) -- refuse")

    totals = dict(con.execute(
        "SELECT column_name, COUNT(*) FROM ov GROUP BY 1").fetchall())
    receipt = {
        "wave": "weekly_promote_apply_batch5", "date": "2026-08-03",
        "class_a_purge": {"floors": PURGE_FLOORS,
                          "expected_zero_cells": purge_expected,
                          "rule": "no nonzero in plane before floor => "
                                  "pre-floor zeros are fill, not facts"},
        "architecture": "resumable per-year loop (v4, starvation-proof)",
        "source_plane": str(src), "repaired_plane": str(out),
        "parts_dir": str(parts),
        "overlays": [p.name for p in overlays],
        "rows": n_in, "null_year_rows_dropped": null_year,
        "years_processed": len(all_years) - skipped,
        "years_resumed_from_marker": skipped,
        "cells_applied_by_year": {str(k): v for k, v in per_year_cells.items()},
        "per_column_overlay_rows": {k: int(v) for k, v in totals.items()},
        "gates": ("one-repair-per-cell, doom==21, per-year key readback "
                  "exact, 1944 phantoms zero, global rows reconciled"),
        "authorization": "promote pre-authorized (Joe 2026-08-02)",
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute("PRAGMA disable_progress_bar")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    print(json.dumps(build(con), indent=2, default=str))
