# NFL Data Management Scripts

Scripts for managing the shared NFL reference data (super table, player IDs).

## Contents

- `build_nfl_super_table.py` - Build unified NFL stats super table
- `build_player_id_bridge.py` - Create cross-reference player ID mappings
- `nfl_franchises.py` - NFL team/franchise reference data

## NFL Super Table

The super table (`md:___ops.nfl_historical.nfl_player_stats_all`) is:
- Shared across all fantasy leagues
- Updated once per weekly update cycle
- Joined via `player_week` composite key

## Usage

These scripts are typically run once or during major data updates:

```bash
# Rebuild super table (caution: long-running)
python nfl_data/build_nfl_super_table.py

# Create player ID bridge
python nfl_data/build_player_id_bridge.py
```

## Note

The `update_nfl_super_table.py` in `multi_league/data_fetchers/` is the
**active script** used by the pipeline. These scripts are for initial setup.
