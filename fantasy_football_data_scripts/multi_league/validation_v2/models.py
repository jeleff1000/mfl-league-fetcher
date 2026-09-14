from __future__ import annotations
from dataclasses import dataclass, field

VALID_SEVERITIES = {"BLOCKER", "ERROR", "WARNING", "INFO"}
VALID_BATCH_GROUPS = {"core", "quality", "analytics"}
VALID_COST_TIERS = {"cheap", "moderate", "expensive"}

VALIDATOR_VERSION = "2026.05.17.1"


@dataclass
class Check:
    name: str
    page: str
    table: str
    severity: str
    description: str
    sql_expr: str | None = None
    sql_full: str | None = None
    threshold: int = 0
    feature: str | None = None
    batch_group: str = "core"
    depends_on: list[str] | None = None
    cost_tier: str = "cheap"
    chunked: bool = False
    chunk_size: int = 20
    fix_action: str | None = None
    debug_sql: str | None = None

    def validate(self) -> None:
        if not self.sql_expr and not self.sql_full:
            raise ValueError(f"Check {self.name}: must have sql_expr or sql_full")
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(f"Check {self.name}: invalid severity {self.severity!r}")
        if self.batch_group not in VALID_BATCH_GROUPS:
            raise ValueError(f"Check {self.name}: invalid batch_group {self.batch_group!r}")
        if self.cost_tier not in VALID_COST_TIERS:
            raise ValueError(f"Check {self.name}: invalid cost_tier {self.cost_tier!r}")


@dataclass
class CheckResult:
    check: Check
    db_name: str
    fail_count: int
    passed: bool
    skipped: bool = False
    suppressed: bool = False
    suppressed_by: str | None = None
    errored: bool = False
    transient_error: bool = False
    error_message: str | None = None


@dataclass
class Manifest:
    all_leagues: list[str] = field(default_factory=list)
    median_leagues: list[str] = field(default_factory=list)
    consolation_leagues: list[str] = field(default_factory=list)
    keeper_leagues: list[str] = field(default_factory=list)
    dynasty_leagues: list[str] = field(default_factory=list)
    faab_leagues: list[str] = field(default_factory=list)
    espn_leagues: list[str] = field(default_factory=list)
    yahoo_leagues: list[str] = field(default_factory=list)
    sleeper_leagues: list[str] = field(default_factory=list)
    multi_year_leagues: list[str] = field(default_factory=list)
    full_import_leagues: list[str] = field(default_factory=list)
    sim_leagues: list[str] = field(default_factory=list)
    settings: dict = field(default_factory=dict)
    skip_lists: dict[str, set[str]] = field(default_factory=dict)

    def leagues_for_feature(self, feature: str | None) -> list[str]:
        if feature is None:
            return self.all_leagues
        return getattr(self, f"{feature}_leagues", self.all_leagues)

    def skip_set_for(self, depends_on: list[str] | None) -> set[str]:
        if not depends_on:
            return set()
        result: set[str] = set()
        for dep in depends_on:
            result |= self.skip_lists.get(dep, set())
        return result

    def update_skip_lists(self, results: list[CheckResult]) -> None:
        for r in results:
            if not r.passed and not r.skipped and not r.errored:
                self.skip_lists.setdefault(r.check.name, set()).add(r.db_name)
