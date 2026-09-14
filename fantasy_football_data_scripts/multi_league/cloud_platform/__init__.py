"""
Cloud Platform Integration Modules

NOTE: Renamed from 'platform' to 'cloud_platform' to avoid shadowing
Python's built-in 'platform' module.

This package provides platform-specific integration modules for:
- Database backend operations (MotherDuck or Fly.io, controlled by DATABASE_BACKEND env var)
- Staging data integration for external uploads

Modules:
    motherduck_sync: MotherDuck database operations (legacy, used when DATABASE_BACKEND=motherduck)
    staging_integration: External staging data handling

Usage:
    from multi_league.cloud_platform import MotherDuckSync, StagingIntegration

    # Database operations (backend-aware via DATABASE_BACKEND env var)
    md = MotherDuckSync(token=os.environ.get('MOTHERDUCK_TOKEN'))
    md.upload_dataframe(df, 'player_fantasy', 'my_league')

    # Staging data
    staging = StagingIntegration()
    staging_years = staging.discover_staging_settings(ctx)
"""

# Placeholder imports - will be populated as modules are implemented
# from .motherduck_sync import MotherDuckSync
# from .staging_integration import StagingIntegration

__all__ = [
    # 'MotherDuckSync',
    # 'StagingIntegration',
]
