"""
Parallel Year Import Support

Provides utilities for importing multiple years in parallel while
respecting API rate limits and optimizing for performance.
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class YearBatch:
    """A batch of years to process together."""

    years: list[int]
    batch_id: int

    @property
    def start_year(self) -> int:
        return min(self.years)

    @property
    def end_year(self) -> int:
        return max(self.years)


def create_year_batches(start_year: int, end_year: int, batch_size: int = 3) -> list[YearBatch]:
    """
    Split a year range into batches for parallel processing.

    Args:
        start_year: First year to import
        end_year: Last year to import
        batch_size: Number of years per batch (default 3)

    Returns:
        List of YearBatch objects
    """
    years = list(range(start_year, end_year + 1))
    batches = []

    for i in range(0, len(years), batch_size):
        batch_years = years[i : i + batch_size]
        batches.append(YearBatch(years=batch_years, batch_id=i // batch_size))

    return batches


def process_years_parallel(
    years: list[int],
    process_func: Callable[[int], Any],
    max_workers: int = 3,
    delay_between_years: float = 0.5,
    on_year_complete: Callable[[int, Any], None] | None = None,
    on_year_error: Callable[[int, Exception], None] | None = None,
) -> dict[int, Any]:
    """
    Process multiple years in parallel with controlled concurrency.

    Args:
        years: List of years to process
        process_func: Function that takes a year and returns result
        max_workers: Maximum concurrent workers (careful with API limits)
        delay_between_years: Delay between starting each year (seconds)
        on_year_complete: Callback when a year completes successfully
        on_year_error: Callback when a year fails

    Returns:
        Dict mapping year to result (or exception if failed)
    """
    results = {}

    def process_with_delay(year: int, delay: float) -> tuple[int, Any]:
        if delay > 0:
            time.sleep(delay)
        return year, process_func(year)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all years with staggered delays
        futures = {}
        for i, year in enumerate(years):
            delay = i * delay_between_years
            future = executor.submit(process_with_delay, year, delay)
            futures[future] = year

        # Collect results
        for future in as_completed(futures):
            year = futures[future]
            try:
                _, result = future.result()
                results[year] = result
                if on_year_complete:
                    on_year_complete(year, result)
                logger.info(f"Year {year} completed successfully")
            except Exception as e:
                results[year] = e
                if on_year_error:
                    on_year_error(year, e)
                logger.error(f"Year {year} failed: {e}")

    return results


class ParallelYearImporter:
    """
    Manages parallel import of multiple years with progress tracking.
    """

    def __init__(
        self,
        start_year: int,
        end_year: int,
        max_concurrent: int = 2,
        batch_size: int = 3,
        api_rate_limit: float = 4.0,  # Yahoo API limit
    ):
        """
        Args:
            start_year: First year to import
            end_year: Last year to import
            max_concurrent: Max concurrent year imports
            batch_size: Years per batch (for reporting)
            api_rate_limit: API calls per second limit
        """
        self.start_year = start_year
        self.end_year = end_year
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        self.api_rate_limit = api_rate_limit

        self.years = list(range(start_year, end_year + 1))
        self.batches = create_year_batches(start_year, end_year, batch_size)

        self.completed_years: list[int] = []
        self.failed_years: list[int] = []
        self.results: dict[int, Any] = {}

    @property
    def total_years(self) -> int:
        return len(self.years)

    @property
    def progress_pct(self) -> float:
        if self.total_years == 0:
            return 100.0
        return (len(self.completed_years) / self.total_years) * 100

    @property
    def current_year(self) -> int | None:
        """Return the most recently started year."""
        in_progress = set(self.years) - set(self.completed_years) - set(self.failed_years)
        return max(in_progress) if in_progress else None

    def import_all(
        self,
        import_func: Callable[[int], Any],
        on_progress: Callable[[int, int, float], None] | None = None,
    ) -> dict[int, Any]:
        """
        Import all years using parallel processing.

        Args:
            import_func: Function that imports a single year
            on_progress: Callback(current_year, total_years, progress_pct)

        Returns:
            Dict mapping year to result
        """
        # Calculate optimal delay to respect rate limits
        # If we have 2 concurrent workers making ~4 API calls/sec each,
        # we need to stagger starts to not exceed total rate limit
        delay_between = max(0.5, 1.0 / (self.api_rate_limit / self.max_concurrent))

        def handle_complete(year: int, result: Any):
            self.completed_years.append(year)
            self.results[year] = result
            if on_progress:
                on_progress(year, self.total_years, self.progress_pct)

        def handle_error(year: int, error: Exception):
            self.failed_years.append(year)
            self.results[year] = error
            logger.error(f"Failed to import year {year}: {error}")

        results = process_years_parallel(
            years=self.years,
            process_func=import_func,
            max_workers=self.max_concurrent,
            delay_between_years=delay_between,
            on_year_complete=handle_complete,
            on_year_error=handle_error,
        )

        return results

    def import_batched(
        self,
        import_batch_func: Callable[[YearBatch], Any],
        on_batch_complete: Callable[[YearBatch, Any], None] | None = None,
    ) -> list[Any]:
        """
        Import years in batches (useful for grouped operations).

        Args:
            import_batch_func: Function that imports a batch of years
            on_batch_complete: Callback when batch completes

        Returns:
            List of batch results
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {executor.submit(import_batch_func, batch): batch for batch in self.batches}

            for future in as_completed(futures):
                batch = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.completed_years.extend(batch.years)

                    if on_batch_complete:
                        on_batch_complete(batch, result)

                    logger.info(f"Batch {batch.batch_id} ({batch.start_year}-{batch.end_year}) completed")
                except Exception as e:
                    self.failed_years.extend(batch.years)
                    logger.error(f"Batch {batch.batch_id} failed: {e}")

        return results


def run_parallel_nfl_fetch(
    years: list[int],
    data_dir: Path,
    max_workers: int = 4,
) -> dict[int, bool]:
    """
    Fetch NFL data for multiple years in parallel.

    NFLverse data doesn't have the same rate limits as Yahoo API,
    so we can be more aggressive with parallelism.

    Args:
        years: Years to fetch
        data_dir: Directory to save data
        max_workers: Number of parallel workers

    Returns:
        Dict mapping year to success status
    """
    from multi_league.data_fetchers.nfl_data import fetch_nfl_data_for_year

    def fetch_year(year: int) -> bool:
        try:
            fetch_nfl_data_for_year(year, data_dir)
            return True
        except Exception as e:
            logger.error(f"Failed to fetch NFL data for {year}: {e}")
            return False

    return process_years_parallel(
        years=years,
        process_func=fetch_year,
        max_workers=max_workers,
        delay_between_years=0.1,  # Minimal delay for NFLverse
    )
