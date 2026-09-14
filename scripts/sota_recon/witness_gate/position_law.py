"""THE POSITION LAW, ENFORCED AT WRITE TIME.

Joe 2026-08-05: "make sure rules are fully law and theres no way to edit the
table and break the law. A rule on editing the file itself that cant be
broken by anyone."

A test that runs afterwards is not a law -- it is a report. The law here is
enforced at the CHOKE POINT: no year-part is marked done until it passes,
so a violating plane cannot come into existence in the first place.

FOUR ARTICLES
  1  VOCABULARY   every token is a broad position from the taxonomy
  2  ORDER        every combo obeys the locked rank
                  QB,RB,WR,TE,K,LB,DL,DB,P,OL -- absolute, not primacy
  3  CEILING      no role a witness didn't declare, except K (the one role
                  our own rule may add)
  4  CONSISTENCY  position agrees with broad(nfl_position)

WHY THIS CANNOT BE SWITCHED OFF. Deleting or neutering the check is itself
fatal: self_test() feeds known-bad rows through the very same checker and
aborts the pass if they are NOT caught. A gate does not exist until watched
refusing, so the pass watches it refuse on every run before trusting it.
And the law's text is fingerprint-pinned across two files (see
position_taxonomy.COMBO_ORDER), so it cannot be quietly reworded either.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .position_taxonomy import (
    BROAD_POSITIONS,
    COMBO_ORDER,
    COMBO_SEPARATOR,
    _COMBO_RANK,
)


class PositionLawViolation(AssertionError):
    """Raised when a plane (or a year-part of one) breaks the position law."""


# K is the ONE role our own rule may add on top of a witness declaration
# (Joe 2026-08-04: "the only time we can make our own rule is kicker").
SELF_MADE_ROLES = ("K",)


def content_sha256(path: str | Path) -> str:
    """Hash file bytes; size and mtime are not content identities."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def input_fingerprint(paths: list[str | Path]) -> str:
    payload = [(str(Path(path)), content_sha256(path)) for path in paths]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def is_ordered(value: object) -> bool:
    """True when a fantasy_position value obeys vocabulary + order.

    Single tokens are ordered by definition. DEF is a legal standalone value
    (the team-defense plane) but has no rank, so it can never combine.
    """
    if value is None:
        return False
    toks = [t.strip().upper() for t in str(value).split(COMBO_SEPARATOR) if t.strip()]
    if not toks or len(toks) != len(set(toks)):
        return False
    if set(toks) - BROAD_POSITIONS:
        return False
    if len(toks) == 1:
        return True
    ranks = [_COMBO_RANK.get(t, 99) for t in toks]
    return ranks == sorted(ranks) and 99 not in ranks


def check_frame(con, relation: str, season_plane: str | None = None,
                where: str = "") -> list[str]:
    """Audit a plane/part for every article. Returns violation strings.

    `relation` is anything DuckDB can select from: a table name or a
    read_parquet(...) expression.
    """
    out: list[str] = []
    w = f"WHERE {where}" if where else ""

    # --- articles 1 + 2: vocabulary and order -------------------------
    rows = con.execute(
        f"SELECT DISTINCT fantasy_position FROM {relation} {w}"
    ).fetchall()
    bad = sorted({str(r[0]) for r in rows
                  if r[0] is not None and not is_ordered(r[0])})
    if bad:
        out.append(
            f"ORDER/VOCABULARY: {len(bad)} values break the locked rank "
            f"{','.join(COMBO_ORDER)}: {bad[:12]}")

    # --- article 4: position == broad(nfl_position) --------------------
    # Only single-valued positions are definitional here; combos are
    # governed by the ceiling instead.
    # AN UNMAPPED LABEL IS NOT A CLAIM. _broad_sql passes a cross-broad
    # two-way label straight through -- 'FL/LE' (flanker / left end) comes
    # back as the literal string 'FL/LE', not a position. Comparing our WR
    # against that scored 12 rows of 1954 as a contradiction when the mapper
    # had simply expressed no opinion. Only compare where the mapper
    # actually produced a broad position; the rest is the ceiling's business.
    try:
        from ..vouch_2024 import _broad_sql
        b = _broad_sql("nfl_position")
        vocab = ", ".join(f"'{p}'" for p in sorted(BROAD_POSITIONS))
        n, ok = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE position = ({b}))
        FROM {relation}
        WHERE nfl_position IS NOT NULL AND position IS NOT NULL
          AND ({b}) IN ({vocab}) AND position NOT LIKE '%,%'
          {('AND ' + where) if where else ''}""").fetchone()
        if ok != n:
            out.append(f"CONSISTENCY: position != broad(nfl_position) on "
                       f"{n - ok}/{n} rows (exact definitional equality required)")
    except Exception as exc:                       # pragma: no cover
        out.append(f"CONSISTENCY: check could not run ({exc})")

    # --- article 3: the witness ceiling --------------------------------
    if season_plane:
        allowed = ", ".join(f"['{r}']" for r in SELF_MADE_ROLES)
        n_over, ex_fp, ex_decl = con.execute(f"""
        WITH d AS (SELECT NFL_player_id AS pid, CAST(year AS INT) AS yr,
                          declared AS decl
                   FROM {('read_parquet(' + repr(str(season_plane)) + ')' if str(season_plane).lower().endswith('.parquet') else str(season_plane))}
                   WHERE declared IS NOT NULL),
        x AS (SELECT DISTINCT d.pid, d.yr, t.fantasy_position AS fp, d.decl
              FROM {relation} t
              JOIN d ON d.pid = t.NFL_player_id
                    AND d.yr = CAST(t.year AS INT)
              WHERE t.fantasy_position IS NOT NULL {('AND ' + where) if where else ''}),
        y AS (SELECT *, list_filter(str_split(fp, ','),
                     tok -> NOT list_contains(str_split(decl, ','), tok)) AS extra
              FROM x)
        SELECT COUNT(*), ANY_VALUE(fp), ANY_VALUE(decl) FROM y
        WHERE len(extra) > 0 AND extra NOT IN ({allowed})""").fetchone()
        if n_over:
            out.append(
                f"CEILING: {n_over} player-seasons claim a role no witness "
                f"declared (e.g. {ex_fp!r} vs declared {ex_decl!r}); only "
                f"{'/'.join(SELF_MADE_ROLES)} may be added by our own rule")
    return out


def assert_frame(con, relation: str, season_plane: str | None = None,
                 where: str = "", label: str = "plane") -> None:
    """Enforce the law. Raises rather than reports."""
    v = check_frame(con, relation, season_plane, where)
    if v:
        raise PositionLawViolation(
            f"POSITION LAW BROKEN in {label}:\n  " + "\n  ".join(v))


def self_test(con) -> None:
    """Watch the law refuse known-bad rows BEFORE trusting it.

    Neutering check_frame -- stubbing it, deleting an article, loosening a
    threshold -- makes this fail, and the caller aborts. That is what makes
    the rule unbreakable rather than merely present: you cannot turn the law
    off without the writer refusing to run.
    """
    cases = {
        "out-of-order combo (OL,K must be K,OL)":
            ("OL,K", "OL", "OL"),
        "token outside the broad vocabulary":
            ("WR,SAFETY", "WR", "WR"),
        "duplicate token":
            ("K,K", "K", "K"),
        "position and detailed-role inconsistency":
            ("QB", "RB", "QB"),
    }
    for label, (fp, pos, nflpos) in cases.items():
        con.execute("CREATE OR REPLACE TEMP TABLE _law_probe AS "
                    "SELECT * FROM (VALUES ('X', 2000, 1, ?, ?, ?)) "
                    "AS t(NFL_player_id, year, week, fantasy_position, "
                    "position, nfl_position)", [fp, pos, nflpos])
        if not check_frame(con, "_law_probe"):
            raise PositionLawViolation(
                f"THE LAW IS NOT ENFORCING: check_frame accepted {label} "
                f"({fp!r}). The position gate has been disabled or broken -- "
                f"refusing to write a plane that nothing is guarding.")
    con.execute("""CREATE OR REPLACE TEMP TABLE _law_decl AS
                   SELECT * FROM (VALUES ('X', 2000, 'WR'))
                   AS d(NFL_player_id, year, declared)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _law_probe AS
                   SELECT * FROM (VALUES ('X', 2000, 1, 'WR,DB', 'WR', 'WR'))
                   AS t(NFL_player_id, year, week, fantasy_position,
                        position, nfl_position)""")
    if not check_frame(con, "_law_probe", season_plane="_law_decl"):
        raise PositionLawViolation(
            "THE LAW IS NOT ENFORCING: check_frame accepted a role above the witness ceiling")
    con.execute("DROP TABLE IF EXISTS _law_probe")
    con.execute("DROP TABLE IF EXISTS _law_decl")


__all__ = ["PositionLawViolation", "check_frame", "assert_frame",
           "self_test", "is_ordered", "SELF_MADE_ROLES", "content_sha256",
           "input_fingerprint"]
