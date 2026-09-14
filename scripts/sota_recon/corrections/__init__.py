"""Deterministic, idempotent, replayable correction layer for the v26 super table.

Each correction is a build step that re-applies cleanly every rebuild. Run with:
    python -m scripts.sota_recon.corrections.run_corrections [--dry-run]
"""
