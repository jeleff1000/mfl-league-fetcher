"""Make scripts/ importable for tests in this package.

scripts/ uses date-suffixed filenames (e.g., audit_pts_def_formulas_2026_04_29.py)
which makes name collisions with other path-importable modules unlikely. If a
collision ever occurs, prefer renaming the script over restructuring the import.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent.parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
