from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re
import os
import asyncio

import sqlalchemy
from sqlalchemy.engine import Engine, Result
from sqlalchemy import text

from google.cloud.sql.connector import Connector

from utils import load_db_secret_list

class CloudSQLPostgres:
    """
    Reusable Cloud SQL Postgres connector that returns a SQLAlchemy Engine.
    Use as a context manager to ensure clean shutdown.

    NOTE:
      - This class is synchronous for normal calls.
      - The ONE async/concurrent method kept is: execute_sql_n_concurrent_async(...)
      - SQLAlchemy+pg8000 is blocking, so concurrency is implemented via threads (asyncio.to_thread).
    """

    def __init__(
        self,
        *,
        instance_connection_name: str,  # "PROJECT:REGION:INSTANCE"
        user: str,
        password: str,
        database: str,
        pool_size: int = 12,
        pool_pre_ping: bool = True,
    ):
        # Basic validation (replaces the removed config class validator)
        if not isinstance(instance_connection_name, str) or not instance_connection_name.strip():
            raise ValueError("instance_connection_name must be a non-empty string")
        if not re.match(r"^[^:]+:[^:]+:[^:]+$", instance_connection_name):
            raise ValueError("instance_connection_name must be PROJECT:REGION:INSTANCE")

        if not isinstance(user, str) or not user.strip():
            raise ValueError("user must be a non-empty string")
        if not isinstance(password, str):
            raise ValueError("password must be a string")
        if not isinstance(database, str) or not database.strip():
            raise ValueError("database must be a non-empty string")

        if not isinstance(pool_size, int) or pool_size <= 0:
            raise ValueError("pool_size must be an int >= 1")

        self._instance_connection_name = instance_connection_name
        self._user = user
        self._password = password
        self._database = database
        self._pool_size = pool_size
        self._pool_pre_ping = bool(pool_pre_ping)

        self._connector: Optional[Connector] = None
        self._engine: Optional[Engine] = None

    def check_reachable(self) -> tuple[bool, str]:
        """
        Check whether the database is reachable.

        Returns:
            (True, "OK") if reachable
            (False, "<error message>") otherwise
        """
        try:
            engine = self.engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, "OK"
        except Exception as exc:
            return False, str(exc)

    def engine(self) -> Engine:
        """
        Lazily create and cache a SQLAlchemy Engine using the Cloud SQL Connector.
        """
        if self._engine is not None:
            return self._engine

        self._connector = Connector()

        def getconn():
            # Returns a raw pg8000 connection created via Cloud SQL Connector
            return self._connector.connect(
                self._instance_connection_name,
                "pg8000",
                user=self._user,
                password=self._password,
                db=self._database,
            )

        self._engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            pool_size=self._pool_size,
            creator=getconn,
            pool_pre_ping=self._pool_pre_ping,
        )
        return self._engine

    def close(self) -> None:
        """
        Dispose engine pool and close Cloud SQL connector.
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        if self._connector is not None:
            self._connector.close()
            self._connector = None

    def __enter__(self) -> "CloudSQLPostgres":
        self.engine()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def execute_sql(self, sql: str) -> List[Tuple[Any, ...]]:
        """
        Execute a SQL statement and return the result rows.

        For SELECT queries:
            returns List[Tuple]
        For non-SELECT queries:
            returns an empty list

        Raises:
            Exception on execution failure
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")

        engine = self.engine()
        with engine.connect() as conn:
            result: Result = conn.execute(text(sql))

            if result.returns_rows:
                return result.fetchall()

            conn.commit()
            return []

    def execute_sql_dict(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL statement and return rows as dictionaries.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")

        engine = self.engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql))

            if not result.returns_rows:
                conn.commit()
                return []

            return [dict(row._mapping) for row in result.fetchall()]

    async def execute_sql_n_concurrent_async(
        self,
        sql: str,
        times: int,
        workers: int = 10,
        *,
        autocommit: bool = False,
        return_rows: bool = False,
    ) -> List[List[Tuple[Any, ...]]]:
        """
        Run the same SQL `times` times using `workers` concurrent workers.

        Under the hood, SQLAlchemy+pg8000 is blocking, so each worker runs in a thread.

        Args:
            sql: SQL to execute.
            times: Total number of executions across all workers.
            workers: Number of concurrent workers (connections).
            autocommit: If True, uses isolation_level="AUTOCOMMIT".
                        If False (default), commits after each non-SELECT statement.
            return_rows: If True, collects and returns rows for SELECT queries.
                         For pressure testing, leave False to avoid memory overhead.

        Returns:
            A list with length = number of workers actually used.
            Each element is that worker's collected rows-per-execution (usually empty).
            If return_rows=False, each worker returns a list of empty tuples-lists.
        """
        if times < 0:
            raise ValueError("times must be >= 0")
        if workers <= 0:
            raise ValueError("workers must be >= 1")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")

        if times == 0:
            return []

        # Don’t spawn more workers than work items.
        worker_count = min(workers, times)

        # Split `times` into near-equal chunks
        base = times // worker_count
        rem = times % worker_count
        chunks = [base + (1 if i < rem else 0) for i in range(worker_count)]

        engine = self.engine()
        stmt = text(sql)

        def _run_worker(n: int) -> List[Tuple[Any, ...]]:
            collected: List[Tuple[Any, ...]] = []
            if n == 0:
                return collected

            with engine.connect() as conn:
                if autocommit:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")

                for _ in range(n):
                    result: Result = conn.execute(stmt)

                    if result.returns_rows:
                        if return_rows:
                            # Flattening to tuples per row (keeps it simple + lightweight)
                            # If you prefer List[List[Tuple]] per execution, say so.
                            rows = result.fetchall()
                            # Store as a "marker tuple" per execution? No: keep per-row tuples.
                            # We'll append all rows for this execution, separated by a sentinel if desired.
                            # For drop-in compatibility with your previous shape, keep it per-execution:
                            # -> but the signature here returns List[List[Tuple]] (per worker, per execution),
                            # so we need per execution list. We'll adapt below.
                            raise RuntimeError("Internal error: per-execution shape handled below.")
                        else:
                            # keep overhead low
                            pass
                    else:
                        if not autocommit:
                            conn.commit()

            return collected

        # --- Per-execution shape (matches your earlier semantics) ---
        # Each worker returns: List[execution_index] -> List[row_tuple]
        def _run_worker_per_exec(n: int) -> List[List[Tuple[Any, ...]]]:
            per_exec: List[List[Tuple[Any, ...]]] = []
            if n == 0:
                return per_exec

            with engine.connect() as conn:
                if autocommit:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")

                for _ in range(n):
                    result: Result = conn.execute(stmt)

                    if result.returns_rows:
                        if return_rows:
                            per_exec.append(result.fetchall())
                        else:
                            per_exec.append([])
                    else:
                        if not autocommit:
                            conn.commit()
                        per_exec.append([])

            return per_exec

        tasks = [asyncio.to_thread(_run_worker_per_exec, n) for n in chunks if n > 0]
        if not tasks:
            return []

        return await asyncio.gather(*tasks)

    def read_pg_stat_statements_heavy_wal(self) -> List[Dict[str, Any]]:
        """
        Execute ./sql_scripts/pg_stat_statements_heavy_wal.sql and return rows as dicts.
        """
        mother_dir = os.path.dirname(os.path.abspath(__file__))
        sql_content_path = os.path.join(
            mother_dir, "sql_scripts", "pg_stat_statements_heavy_wal.sql"
        )

        with open(sql_content_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        engine = self.engine()
        with engine.connect() as conn:
            results = conn.execute(text(sql_content)).mappings().all()

        return [dict(r) for r in results]

    def read_pg_stat_statements_top_queries(self) -> List[Dict[str, Any]]:
        """
        Execute ./sql_scripts/pg_stat_statements_top_queries.sql and return rows as dicts.
        """
        mother_dir = os.path.dirname(os.path.abspath(__file__))
        sql_content_path = os.path.join(
            mother_dir, "sql_scripts", "pg_stat_statements_top_queries.sql"
        )

        with open(sql_content_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        engine = self.engine()
        with engine.connect() as conn:
            results = conn.execute(text(sql_content)).mappings().all()

        return [dict(r) for r in results]


def main():
    # Prefer environment variables over hardcoding secrets
    dbs = load_db_secret_list(r"C:\Users\kaiyi\Desktop\github\psql-cli\src\data\db-secrets.json")
    instance_connection_name = dbs[0]["instance_connection_name"]
    user = dbs[0]["username"]
    password = dbs[0]["password"]
    database = dbs[0]["database"]

    with CloudSQLPostgres(
        instance_connection_name=instance_connection_name,
        user=user,
        password=password,
        database=database,
        pool_size=12,
    ) as db:
        ok, msg = db.check_reachable()
        print("reachable:", ok, msg)

        # Example: run a report SQL file
        # rows = db.read_pg_stat_statements_top_queries()
        # for r in rows:
        #     print(r)

        # Example: pressure test concurrently (async)
        async def run_pressure():
            results = await db.execute_sql_n_concurrent_async(
                "SELECT 1",
                times=10000,
                workers=10,
                return_rows=True,
            )
            # results[worker_idx][exec_idx] -> list of row tuples
            print("workers:", len(results))
            print("first worker, first exec:", results[0][0] if results and results[0] else None)

        asyncio.run(run_pressure())


if __name__ == "__main__":
    main()
