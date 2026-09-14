# Archived Transformation Scripts

This directory contains transformation scripts that are no longer used in the active pipeline.

## Archived Files

### 1. `aggregate_player_season_v2.py`
- **Archived Date**: December 22, 2025
- **Reason**: Not called anywhere in codebase (only referenced in old documentation)
- **Functionality**: Created season-level player summaries
- **Replaced By**: Functionality now handled by other aggregations in the pipeline
- **Last Known Status**: Only referenced in `docs/archive/` files, never imported or executed

### 2. `keeper_economics_v2.py`
- **Archived Date**: December 22, 2025
- **Reason**: Duplicate/obsolete version
- **Functionality**: Calculated keeper prices using league rules from context
- **Replaced By**: `player/keeper_economics.py` (actively used in PASS 3 of initial_import_v2.py)
- **Last Known Status**: Only referenced in `docs/archive/` files, never imported or executed

## Why Archive Instead of Delete?

These files are archived (not deleted) because:
1. **Historical Reference**: They may contain useful logic or patterns for future development
2. **Documentation**: They help understand the evolution of the codebase
3. **Safety**: Easy to recover if we discover they were needed for something unexpected

## Active Transformation Pipeline

For the current active transformations, see:
- `initial_import_v2.py` - Main pipeline that calls all active transformations
- `TRANSFORMATION_AUDIT.md` (root directory) - Complete audit of transformation usage

---

**Note**: If you need to restore any of these files, simply move them back to their original location.
