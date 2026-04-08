"""
etl/extractors/sql_extractor.py
Extrae datos desde cualquier base de datos vía sqlite3 (o sqlalchemy si disponible).
Soporta paginación automática para tablas grandes.
"""
from __future__ import annotations
import logging
import sqlite3
from typing import Any, Dict, Iterator, List, Optional, Tuple

from etl.core.base import Extractor

log = logging.getLogger(__name__)


class SqliteExtractor(Extractor[Dict]):
    """
    Extrae datos de una base de datos SQLite.
    Soporta paginación automática, parámetros y subqueries.

    Ejemplo:
        extractor = SqliteExtractor(
            db_path="data/warehouse.db",
            query="SELECT * FROM orders WHERE status = :status AND created_at > :since",
            params={"status": "completed", "since": "2024-01-01"},
            chunk_size=5000,
        )
    """

    def __init__(
        self,
        db_path: str,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000,
        count_query: Optional[str] = None,
    ):
        self.db_path = db_path
        self.query = query
        self.params = params or {}
        self.chunk_size = chunk_size
        self.count_query = count_query

    @property
    def name(self) -> str:
        return f"SqliteExtractor({self.db_path})"

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def count(self) -> Optional[int]:
        if not self.count_query:
            return None
        with self._connect() as conn:
            row = conn.execute(self.count_query, self.params).fetchone()
            return row[0] if row else 0

    def extract(self) -> Iterator[List[Dict]]:
        log.info("Ejecutando query en %s", self.db_path)
        conn = self._connect()
        try:
            cursor = conn.execute(self.query, self.params)
            columns = [d[0] for d in cursor.description]

            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                chunk = [dict(zip(columns, row)) for row in rows]
                log.debug("Chunk extraído: %d registros", len(chunk))
                yield chunk
        finally:
            conn.close()


class SqliteIncrementalExtractor(Extractor[Dict]):
    """
    Extracción incremental: solo trae registros nuevos desde la última ejecución.
    Guarda el watermark (último valor extraído) en una tabla de control.

    Ejemplo:
        extractor = SqliteIncrementalExtractor(
            db_path="data/warehouse.db",
            source_table="events",
            watermark_column="created_at",
            watermark_db="data/etl_control.db",
            pipeline_name="events_to_dw",
        )
    """

    def __init__(
        self,
        db_path: str,
        source_table: str,
        watermark_column: str,
        watermark_db: str,
        pipeline_name: str,
        chunk_size: int = 1000,
        extra_columns: str = "*",
    ):
        self.db_path = db_path
        self.source_table = source_table
        self.watermark_column = watermark_column
        self.watermark_db = watermark_db
        self.pipeline_name = pipeline_name
        self.chunk_size = chunk_size
        self.extra_columns = extra_columns
        self._new_watermark: Optional[Any] = None

    @property
    def name(self) -> str:
        return f"SqliteIncrementalExtractor({self.source_table})"

    def _get_watermark(self) -> Optional[Any]:
        """Lee el último watermark guardado."""
        conn = sqlite3.connect(self.watermark_db)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_watermarks (
                    pipeline_name TEXT PRIMARY KEY,
                    watermark_value TEXT,
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.commit()
            row = conn.execute(
                "SELECT watermark_value FROM etl_watermarks WHERE pipeline_name = ?",
                (self.pipeline_name,)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def save_watermark(self, value: Any) -> None:
        """Guarda el nuevo watermark tras una ejecución exitosa."""
        conn = sqlite3.connect(self.watermark_db)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO etl_watermarks (pipeline_name, watermark_value, updated_at)
                VALUES (?, ?, datetime('now'))
            """, (self.pipeline_name, str(value)))
            conn.commit()
            log.info("Watermark guardado: %s = %s", self.pipeline_name, value)
        finally:
            conn.close()

    def extract(self) -> Iterator[List[Dict]]:
        watermark = self._get_watermark()
        log.info(
            "Extracción incremental de '%s' desde watermark=%s",
            self.source_table, watermark
        )

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            if watermark is not None:
                query = (
                    f"SELECT {self.extra_columns} FROM {self.source_table} "
                    f"WHERE {self.watermark_column} > ? "
                    f"ORDER BY {self.watermark_column} ASC"
                )
                cursor = conn.execute(query, (watermark,))
            else:
                query = (
                    f"SELECT {self.extra_columns} FROM {self.source_table} "
                    f"ORDER BY {self.watermark_column} ASC"
                )
                cursor = conn.execute(query)

            columns = [d[0] for d in cursor.description]
            last_value = watermark

            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                chunk = [dict(zip(columns, row)) for row in rows]
                last_value = chunk[-1][self.watermark_column]
                self._new_watermark = last_value
                yield chunk

            if self._new_watermark is not None:
                self.save_watermark(self._new_watermark)
                log.info("Nuevo watermark: %s", self._new_watermark)
        finally:
            conn.close()
