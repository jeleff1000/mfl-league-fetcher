import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.retransform import get_dependents


def test_matchup_to_player_cascade_recomputes_optimal_then_matchup_rollups():
    cascade = get_dependents("matchup_to_player")

    assert "matchup_to_player" in cascade
    assert "compute_manager_optimal" in cascade
    assert "player_to_matchup" in cascade
    assert cascade.index("matchup_to_player") < cascade.index("compute_manager_optimal")
    assert cascade.index("compute_manager_optimal") < cascade.index("player_to_matchup")
