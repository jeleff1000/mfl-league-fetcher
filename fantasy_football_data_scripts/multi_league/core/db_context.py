"""
Lightweight database context for scripts that operate directly on
Fly.io or a local DuckDB file.

Replaces the need for LeagueContext + local parquet files when a script
only needs to read/write tables in the database.

Usage (remote via Fly.io):
    from multi_league.core.db_context import DbContext

    db = DbContext("my_league")
    matchup_df = db.read_table("matchup")
    # ... transform ...
    db.write_table("matchup", result_df)
    db.close()

Usage (local DuckDB):
    db = DbContext("my_league", data_dir="/path/to/data")
    # Connects to /path/to/data/my_league.duckdb via ATTACH alias
    matchup_df = db.read_table("matchup")

Or as a context manager:
    with DbContext("my_league") as db:
        matchup_df = db.read_table("matchup")
        db.write_table("matchup", result_df)
"""

import json
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from multi_league.core.local_db import _TABLE_COLUMN_TYPES, _TABLE_DDL


def _infer_sql_type(series: pd.Series) -> str:
    dtype = series.dtype
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    return "VARCHAR"


class DbContext:
    """Database context for read/write -- connects to Fly.io or a local DuckDB file.

    When data_dir is provided, connects to a local DuckDB file using the ATTACH
    alias pattern so qualified SQL like ``public.table`` resolves correctly.
    When data_dir is None, uses the Fly.io DuckDB server via FlyReader.
    """

    def __init__(self, db_name: str, token: str | None = None, data_dir: str | None = None):
        self.db_name = db_name
        self._is_local = data_dir is not None

        if self._is_local:
            # -- Local DuckDB mode --
            local_path = Path(data_dir) / f"{db_name}.duckdb"
            if not local_path.exists():
                raise FileNotFoundError(
                    f"Local DuckDB file not found: {local_path}\n" f"Run a full import first to create it."
                )
            # In-memory connection + ATTACH so public.table resolves to db_name.public.table
            self.conn = duckdb.connect(":memory:")
            self.conn.execute(f"ATTACH '{local_path}' AS \"{db_name}\"")
            self.conn.execute(f'USE "{db_name}"')
            self.conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        else:
            # -- Fly.io mode --
            from multi_league.core.db_reader import get_reader

            self._fly_reader = get_reader()
            self.conn = None  # No DuckDB connection on Fly backend

        # Create a temp directory for any scripts that need local file paths
        self._temp_dir = tempfile.mkdtemp(prefix=f"league_{db_name}_")
        self._data_directory = Path(self._temp_dir)
        self._settings_cache: dict[int, dict] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass

    # -- Table I/O --

    def read_table(self, table: str, where: str = "", columns: str = "*") -> pd.DataFrame:
        """Read a table from the database into a DataFrame."""
        if self._is_local:
            sql = f"SELECT {columns} FROM public.{table}"
            if where:
                sql += f" WHERE {where}"
            return self.conn.execute(sql).fetchdf()

        # Fly.io path
        sql = f"SELECT {columns} FROM public.{table}"
        clauses = [f"db_name = '{self.db_name}'"]
        if where:
            clauses.append(f"({where})")
        if table in _TABLE_DDL:
            sql += " WHERE " + " AND ".join(clauses)
        elif where:
            sql += f" WHERE {where}"
        return self._fly_reader.query_df(sql, database=self.db_name)

    def _ensure_canonical_table(self, table: str):
        if table not in _TABLE_DDL:
            return
        ddl = _TABLE_DDL[table](self.db_name).replace(f'"{self.db_name}".public.', "public.")
        self.conn.execute(ddl)

    def _validate_canonical_columns(self, table: str, columns: list[str]):
        expected_types = _TABLE_COLUMN_TYPES.get(table)
        if not expected_types:
            return
        unexpected = sorted(set(columns) - set(expected_types))
        if unexpected:
            raise ValueError(f"Refusing to write non-canonical columns to public.{table}: {', '.join(unexpected)}")

    def _ensure_update_columns_exist(self, table: str, df: pd.DataFrame, update_cols: list[str]):
        existing = {r[0] for r in self.conn.execute(f"DESCRIBE public.{table}").fetchall()}
        expected_types = _TABLE_COLUMN_TYPES.get(table, {})
        for col in update_cols:
            if col in existing:
                continue
            sql_type = expected_types.get(col) or _infer_sql_type(df[col])
            self.conn.execute(f'ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS "{col}" {sql_type}')

    def write_table(self, table: str, df: pd.DataFrame, mode: str = "replace"):
        """Write a DataFrame to a database table.

        Args:
            table: Table name (in public schema)
            df: DataFrame to write
            mode: 'replace' drops and recreates, 'append' inserts
        """
        if not self._is_local and table not in _TABLE_DDL:
            raise NotImplementedError(f"DbContext does not support ad-hoc writes for {table}")

        if table in _TABLE_DDL:
            df = df.copy()
            df["db_name"] = self.db_name

        self.conn.register("_write_df", df)
        try:
            if table in _TABLE_DDL:
                self._validate_canonical_columns(table, list(df.columns))
                self._ensure_canonical_table(table)

                if mode == "replace":
                    self.conn.execute(f"DELETE FROM public.{table} WHERE db_name = ?", [self.db_name])
                elif mode != "append":
                    raise ValueError(f"Unsupported write mode: {mode}")

                insert_cols = [col for col in _TABLE_COLUMN_TYPES[table] if col in df.columns]
                if insert_cols:
                    col_list = ", ".join(f'"{c}"' for c in insert_cols)
                    self.conn.execute(f"INSERT INTO public.{table} ({col_list}) SELECT {col_list} FROM _write_df")
                return

            if not self._is_local:
                raise NotImplementedError(f"DbContext does not support ad-hoc writes for {table}")

            if mode == "replace":
                self.conn.execute(f"DROP TABLE IF EXISTS public.{table}")
                self.conn.execute(f"CREATE TABLE public.{table} AS SELECT * FROM _write_df")
            elif mode == "append":
                self.conn.execute(f"INSERT INTO public.{table} SELECT * FROM _write_df")
            else:
                raise ValueError(f"Unsupported write mode: {mode}")
        finally:
            self.conn.unregister("_write_df")

    def update_columns(
        self,
        table: str,
        df: pd.DataFrame,
        key_cols: list[str],
        update_cols: list[str] | None = None,
    ):
        """UPDATE specific columns in a table from a DataFrame.

        Only touches the columns the caller produced -- leaves all other
        enrichment columns untouched.

        Args:
            table: Table name (in public schema)
            df: DataFrame with key columns + columns to update
            key_cols: Columns to join on (e.g., ['year', 'week', 'manager'])
            update_cols: Columns to SET. If None, all non-key columns in df.
        """
        if update_cols is None:
            update_cols = [c for c in df.columns if c not in key_cols]
        if not update_cols:
            return

        if not self._is_local and table not in _TABLE_DDL:
            raise NotImplementedError(f"DbContext only supports canonical table updates: {table}")

        self._validate_canonical_columns(table, [*key_cols, *update_cols])
        self._ensure_canonical_table(table)
        self._ensure_update_columns_exist(table, df, update_cols)

        # Register df as temp table and UPDATE via join
        self.conn.register("_update_df", df)
        set_clause = ", ".join(f'"{c}" = u."{c}"' for c in update_cols)
        join_clause = " AND ".join(f't."{k}" = u."{k}"' for k in key_cols)
        params: list[str] = []
        where_clause = join_clause
        if not self._is_local:
            where_clause = f"t.db_name = ? AND {join_clause}" if join_clause else "t.db_name = ?"
            params.append(self.db_name)

        sql = f"""
            UPDATE public.{table} t
            SET {set_clause}
            FROM _update_df u
            WHERE {where_clause}
        """
        if params:
            self.conn.execute(sql, params)
        else:
            self.conn.execute(sql)
        self.conn.unregister("_update_df")

    def table_exists(self, table: str) -> bool:
        """Check if a table exists in the database."""
        if not self._is_local and self.conn is None:
            # Fly path
            try:
                self._fly_reader.query(f"DESCRIBE public.{table}", database=self.db_name)
                return True
            except Exception:
                return False

        if self._is_local:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = ? AND table_schema = 'public' AND table_name = ?",
                [self.db_name, table],
            ).fetchone()
            return result[0] > 0 if result else False

        try:
            self.conn.execute(f"DESCRIBE public.{table}").fetchall()
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: list = None):
        """Execute raw SQL."""
        if not self._is_local and self.conn is None:
            # Fly path -- substitute params inline
            if params:
                for p in params:
                    if p is None:
                        sql = sql.replace("?", "NULL", 1)
                    elif isinstance(p, (int, float, bool)):
                        sql = sql.replace("?", str(p), 1)
                    else:
                        escaped = str(p).replace("'", "''")
                        sql = sql.replace("?", f"'{escaped}'", 1)
            return self._fly_reader.query(sql, database=self.db_name)

        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    # -- Metadata (from league_settings) --

    def get_settings(self) -> dict[int, dict]:
        """Load league_settings from the database, keyed by year."""
        if self._settings_cache is not None:
            return self._settings_cache

        self._settings_cache = {}
        if not self.table_exists("league_settings"):
            return self._settings_cache

        df = self.read_table("league_settings")
        for _, row in df.iterrows():
            year = row.get("year") or row.get("season")
            if year:
                self._settings_cache[int(year)] = row.to_dict()

        return self._settings_cache

    @property
    def league_name(self) -> str:
        """Derive league name from database name."""
        name = self.db_name
        if name.startswith("l_") and len(name) > 2 and name[2].isdigit():
            name = name[2:]
        return name.replace("_", " ").title()

    @property
    def years(self) -> list[int]:
        """Get all years from league_settings or matchup table."""
        settings = self.get_settings()
        if settings:
            return sorted(settings.keys())
        # Fallback to matchup table
        if self.conn is None:
            rows = self._fly_reader.query(
                "SELECT DISTINCT year FROM public.matchup ORDER BY year",
                database=self.db_name,
            )
            return [r["year"] for r in rows if r.get("year")]
        result = self.conn.execute("SELECT DISTINCT year FROM public.matchup ORDER BY year").fetchall()
        return [r[0] for r in result if r[0]]

    @property
    def start_year(self) -> int:
        return min(self.years) if self.years else 2020

    @property
    def end_year(self) -> int:
        return max(self.years) if self.years else 2025

    @property
    def data_directory(self) -> Path:
        """Temp directory for scripts that need local file paths."""
        return self._data_directory

    def ensure_settings_dir(self) -> Path:
        """Download league_settings JSON files to temp dir for scripts that need them."""
        settings_dir = self._data_directory / "league_settings"
        settings_dir.mkdir(parents=True, exist_ok=True)

        settings = self.get_settings()
        for year, row in settings.items():
            settings_file = settings_dir / f"league_settings_{year}.json"
            if not settings_file.exists():
                with open(settings_file, "w") as f:
                    # Clean numpy types
                    clean = {}
                    for k, v in row.items():
                        if pd.isna(v):
                            clean[k] = None
                        elif hasattr(v, "item"):  # numpy scalar
                            clean[k] = v.item()
                        else:
                            clean[k] = v
                    json.dump(clean, f, indent=2, default=str)

        return settings_dir
