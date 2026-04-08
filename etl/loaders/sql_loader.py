"""
etl/loaders/sql_loader.py
Loaders para bases de datos SQLite (patrón replicable a PostgreSQL/MSSQL).
Soporta: INSERT, INSERT OR REPLACE (upsert), INSERT OR IGNORE.
"""
from __future__ import annotations
import logging
import sqlite3
from typing import Dict, List, Optional

from etl.core.base import Loader

log = logging.getLogger(__name__)


class SqliteLoader(Loader[Dict]):
    """
    Carga registros en SQLite usando INSERT OR REPLACE (upsert por defecto).

    El esquema se crea automáticamente en el primer load si create_table=True.

    Ejemplo:
        loader = SqliteLoader(
            db_path="data/warehouse.db",
            table="orders",
            mode="replace",    # "insert", "replace", "ignore"
        )
    """

    def __init__(
        self,
        db_path: str,
        table: str,
        mode: str = "replace",
        create_table: bool = True,
        batch_size: int = 500,
        primary_keys: list = None,
    ):
        if mode not in ("insert", "replace", "ignore"):
            raise ValueError(f"mode debe ser 'insert', 'replace' o 'ignore', no '{mode}'")
        self.db_path = db_path
        self.table = table
        self.mode = mode
        self.create_table = create_table
        self.batch_size = batch_size
        self.primary_keys = primary_keys or []
        self._conn: Optional[sqlite3.Connection] = None
        self._table_created = False

    @property
    def name(self) -> str:
        return f"SqliteLoader({self.table}, mode={self.mode})"

    def begin(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        log.info("SqliteLoader: conexión abierta a %s", self.db_path)

    def _ensure_table(self, columns: List[str]) -> None:
        if self._table_created or not self.create_table:
            return
        col_defs = []
        for c in columns:
            if self.primary_keys and c in self.primary_keys:
                col_defs.append(f'"{c}" TEXT NOT NULL')
            else:
                col_defs.append(f'"{c}" TEXT')
        if self.primary_keys:
            pk = ", ".join(f'"{k}"' for k in self.primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk})")
        cols_def = ", ".join(col_defs)
        self._conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{self.table}" ({cols_def})'
        )
        self._conn.commit()
        self._table_created = True
        log.info("Tabla '%s' verificada/creada", self.table)

    def load(self, records: List[Dict]) -> int:
        if not records:
            return 0

        columns = list(records[0].keys())
        self._ensure_table(columns)

        placeholders = ", ".join("?" * len(columns))
        cols_quoted = ", ".join(f'"{c}"' for c in columns)

        verb = {
            "insert": "INSERT",
            "replace": "INSERT OR REPLACE",
            "ignore": "INSERT OR IGNORE",
        }[self.mode]

        sql = f'{verb} INTO "{self.table}" ({cols_quoted}) VALUES ({placeholders})'
        loaded = 0

        # Procesar en micro-batches para no crear transacciones enormes
        for i in range(0, len(records), self.batch_size):
            micro_batch = records[i: i + self.batch_size]
            values = [[r.get(c) for c in columns] for r in micro_batch]
            try:
                self._conn.executemany(sql, values)
                loaded += len(micro_batch)
            except Exception as e:  # noqa: BLE001
                log.error("Error insertando micro-batch en '%s': %s", self.table, e)
                raise

        return loaded

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()
            log.info("SqliteLoader: commit realizado en '%s'", self.table)

    def rollback(self) -> None:
        if self._conn:
            self._conn.rollback()
            log.warning("SqliteLoader: rollback realizado en '%s'", self.table)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()


class CsvFileLoader(Loader[Dict]):
    """
    Escribe registros en un archivo CSV.
    Útil para staging, reportes, y exportaciones.

    Ejemplo:
        loader = CsvFileLoader("output/orders_2024.csv", mode="overwrite")
    """

    def __init__(
        self,
        output_path: str,
        mode: str = "overwrite",  # "overwrite" | "append"
        delimiter: str = ",",
        encoding: str = "utf-8",
    ):
        self.output_path = output_path
        self.mode = mode
        self.delimiter = delimiter
        self.encoding = encoding
        self._file = None
        self._writer = None
        self._header_written = False

    @property
    def name(self) -> str:
        import os
        return f"CsvFileLoader({os.path.basename(self.output_path)})"

    def begin(self) -> None:
        import csv
        file_mode = "w" if self.mode == "overwrite" else "a"
        self._file = open(self.output_path, file_mode,
                          newline="", encoding=self.encoding)
        self._writer = csv.DictWriter(self._file, fieldnames=[], delimiter=self.delimiter)
        # Los fieldnames se establecen en el primer load
        self._header_written = (self.mode == "append")
        log.info("CsvFileLoader: abierto %s (modo=%s)", self.output_path, self.mode)

    def load(self, records: List[Dict]) -> int:
        if not records:
            return 0

        import csv
        if not self._header_written:
            self._writer.fieldnames = list(records[0].keys())
            self._writer.writeheader()
            self._header_written = True
        elif not self._writer.fieldnames:
            self._writer.fieldnames = list(records[0].keys())

        self._writer.writerows(records)
        return len(records)

    def commit(self) -> None:
        if self._file:
            self._file.flush()

    def rollback(self) -> None:
        import os
        if self._file:
            self._file.close()
            self._file = None
        if self.mode == "overwrite" and os.path.exists(self.output_path):
            os.remove(self.output_path)
            log.warning("CsvFileLoader: archivo eliminado por rollback")

    def __del__(self):
        if self._file:
            self._file.close()
