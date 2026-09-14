"""
Shared validation logic for validate_all_*_leagues.py scripts.

Provides:
- parse_error_types(): Extract error types from validator output
- validate_league(): Run validator on a single league database
- run_validation(): Main loop - validate a list of leagues and print summary
"""

import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def parse_error_types(output: str) -> dict:
    """Parse individual error/warning/info messages from validation output.

    Returns dict with keys 'ERROR', 'WARNING', 'INFO', each mapping to a list
    of error type strings like '[matchup] p_final_sum'.
    """
    result = {"ERROR": [], "WARNING": [], "INFO": []}
    current_section = None
    # Pattern: leading whitespace, [table] check_name: message...
    issue_pattern = re.compile(r"^\s+\[(\w+)\]\s+(\w+):")

    for line in output.split("\n"):
        stripped = line.strip()
        if stripped == "ERRORS:":
            current_section = "ERROR"
        elif stripped == "WARNINGS:":
            current_section = "WARNING"
        elif stripped == "INFO:":
            current_section = "INFO"
        elif stripped.startswith("=") or stripped.startswith("Tables checked:") or stripped.startswith("Status:"):
            current_section = None
        elif current_section and (match := issue_pattern.match(line)):
            table = match.group(1)
            check = match.group(2)
            error_type = f"[{table}] {check}"
            result[current_section].append(error_type)

    return result


def validate_league(db_name: str) -> dict:
    """Run validation on a single league and return results."""
    cmd = [sys.executable, "-m", "multi_league.validation.validate_motherduck", db_name]

    empty_types = {"ERROR": [], "WARNING": [], "INFO": []}

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(Path(__file__).parent.parent / "fantasy_football_data_scripts"),
        )

        output = result.stdout + result.stderr

        # Parse results
        errors = 0
        warnings = 0
        info = 0
        for line in output.split("\n"):
            if "Errors:" in line and "Warnings:" in line:
                # Parse: "Errors: 0, Warnings: 2, Info: 0"
                parts = line.split(",")
                for part in parts:
                    if "Errors:" in part:
                        errors = int(part.split(":")[1].strip())
                    elif "Warnings:" in part:
                        warnings = int(part.split(":")[1].strip())
                    elif "Info:" in part:
                        info = int(part.split(":")[1].strip())

        # Parse individual error types
        error_types = parse_error_types(output)

        return {
            "success": result.returncode == 0,
            "errors": errors,
            "warnings": warnings,
            "info": info,
            "error_types": error_types,
            "output": output,
        }

    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "errors": -1,
            "warnings": -1,
            "info": -1,
            "error_types": empty_types,
            "output": "TIMEOUT",
        }
    except Exception as e:
        return {
            "success": False,
            "errors": -1,
            "warnings": -1,
            "info": -1,
            "error_types": empty_types,
            "output": str(e),
        }


def run_validation(leagues: list, platform: str, verbose: bool = False) -> int:
    """Validate a list of leagues and print results.

    Args:
        leagues: List of dicts with 'league_name' and 'database_name' keys.
        platform: Display name for the platform (e.g. 'Sleeper', 'Yahoo').
        verbose: If True, show full validation output per league.

    Returns:
        Exit code (0 = all passed, 1 = failures).
    """
    print(f"Validating {len(leagues)} {platform} leagues...")
    print("=" * 80)

    results = []
    for i, league in enumerate(leagues, 1):
        league_name = league.get("league_name", "Unknown")
        db_name = league.get("database_name", "")

        # Sanitize league name for console output (remove emojis/unicode that cp1252 can't encode)
        safe_name = league_name.encode("ascii", errors="replace").decode("ascii")
        if verbose:
            print(f"\n[{i}/{len(leagues)}] Validating: {safe_name} ({db_name})")
        else:
            print(f"[{i:2}/{len(leagues)}] {safe_name[:40]:40} ", end="", flush=True)

        result = validate_league(db_name)
        results.append({"league_name": league_name, "database_name": db_name, **result})

        if verbose:
            print(result.get("output", "No output"))
            print()
        else:
            if result["success"]:
                status = f"[OK] {result['errors']}E {result['warnings']}W {result['info']}I"
            else:
                status = "[FAIL]"
            print(status)

    # Summary
    print("\n" + "=" * 80)
    print(f"VALIDATION SUMMARY ({platform})")
    print("=" * 80)

    total_errors = 0
    total_warnings = 0
    total_info = 0
    failed_to_run = []
    leagues_with_errors = []
    leagues_passed = []

    # Aggregate error types: {(error_type, severity): {'leagues': set, 'occurrences': int}}
    error_type_agg = defaultdict(lambda: {"leagues": set(), "occurrences": 0})

    for r in results:
        if r["errors"] == -1:
            failed_to_run.append(r["league_name"])
            continue

        total_errors += r["errors"]
        total_warnings += r["warnings"]
        total_info += r["info"]

        for severity in ("ERROR", "WARNING", "INFO"):
            for error_type in r.get("error_types", {}).get(severity, []):
                key = (error_type, severity)
                error_type_agg[key]["leagues"].add(r["league_name"])
                error_type_agg[key]["occurrences"] += 1

        if r["errors"] > 0:
            leagues_with_errors.append((r["league_name"], r["errors"], r["warnings"]))
        else:
            leagues_passed.append((r["league_name"], r["warnings"]))

    print(f"\nTotal leagues: {len(leagues)}")
    print(f"Passed (0 errors): {len(leagues_passed)}")
    print(f"With errors: {len(leagues_with_errors)}")
    print(f"Failed to run: {len(failed_to_run)}")
    print(f"\nTotal errors: {total_errors}")
    print(f"Total warnings: {total_warnings}")
    print(f"Total info: {total_info}")

    if failed_to_run:
        print("\n[X] FAILED TO RUN:")
        for name in failed_to_run:
            print(f"  - {name}")

    if leagues_with_errors:
        print("\n[!] LEAGUES WITH ERRORS:")
        for name, err_count, warn_count in sorted(leagues_with_errors, key=lambda x: -x[1]):
            print(f"  - {name}: {err_count} errors, {warn_count} warnings")

    if leagues_passed:
        print("\n[OK] PASSED LEAGUES:")
        for name, warn_count in sorted(leagues_passed, key=lambda x: -x[1]):
            if warn_count > 0:
                print(f"  - {name}: {warn_count} warnings")
            else:
                print(f"  - {name}")

    # Error Type Summary
    print("\n" + "=" * 80)
    print(f"ERROR TYPE SUMMARY ({platform}, across all leagues)")
    print("=" * 80)

    for severity, label in [("ERROR", "ERRORS"), ("WARNING", "WARNINGS"), ("INFO", "INFO")]:
        entries = []
        for (error_type, sev), data in error_type_agg.items():
            if sev == severity:
                entries.append((error_type, len(data["leagues"]), data["occurrences"], sorted(data["leagues"])))

        if not entries:
            continue

        entries.sort(key=lambda x: -x[1])
        unique_types = len(entries)
        total_occurrences = sum(e[2] for e in entries)

        print(f"\n{label} by type ({unique_types} unique types, {total_occurrences} total occurrences):")
        for error_type, league_count, occurrences, league_names in entries:
            padded_type = f"  {error_type} "
            dots = "." * max(1, 60 - len(padded_type))
            suffix = f" {league_count} leagues"
            if occurrences != league_count:
                suffix += f" ({occurrences} occurrences)"
            print(f"{padded_type}{dots}{suffix}")
            for name in league_names:
                print(f"      - {name}")

    if failed_to_run or leagues_with_errors:
        return 1
    return 0
