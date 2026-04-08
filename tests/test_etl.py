"""
tests/test_etl.py
Tests del framework ETL.
Ejecutar: python -m unittest tests.test_etl -v
"""
import csv
import io
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.core.base import (
    ChainTransformer,
    IdentityTransformer,
    Loader,
    Pipeline,
    RunResult,
)
from etl.extractors.csv_extractor import CsvExtractor, InMemoryCsvExtractor
from etl.extractors.sql_extractor import SqliteExtractor, SqliteIncrementalExtractor
from etl.loaders.sql_loader import CsvFileLoader, SqliteLoader
from etl.transformers.data_transformers import (
    ColumnMapper,
    ComputedColumns,
    DuplicateFilter,
    NullFilter,
    TypeCaster,
    ValueNormalizer,
)


# ── Helpers ───────────────────────────────────────────────────────

def make_csv_file(tmp_dir: str, filename: str, rows: list, headers: list) -> str:
    path = os.path.join(tmp_dir, filename)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)
    return path


def make_sqlite_db(tmp_dir: str, table: str, rows: list) -> str:
    db_path = os.path.join(tmp_dir, "test.db")
    conn = sqlite3.connect(db_path)
    if rows:
        cols = list(rows[0].keys())
        cols_def = ", ".join(f"{c} TEXT" for c in cols)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_def})")
        placeholders = ", ".join("?" * len(cols))
        conn.executemany(
            f"INSERT INTO {table} VALUES ({placeholders})",
            [[r[c] for c in cols] for r in rows]
        )
        conn.commit()
    conn.close()
    return db_path


SAMPLE_ROWS = [
    {"order_id": "1", "customer": "Alice", "amount": "100.50", "status": "Active"},
    {"order_id": "2", "customer": "Bob",   "amount": "200.00", "status": "active"},
    {"order_id": "3", "customer": "Carol", "amount": "abc",    "status": "INACTIVE"},
    {"order_id": "4", "customer": "",      "amount": "50.00",  "status": "active"},
    {"order_id": "2", "customer": "Bob",   "amount": "210.00", "status": "active"},  # dup
]


# ── Tests del Pipeline base ───────────────────────────────────────

class TestRunResult(unittest.TestCase):
    def test_success_property(self):
        r = RunResult("test", "success", rows_extracted=10, rows_loaded=10)
        self.assertTrue(r.success)

    def test_failed_property(self):
        r = RunResult("test", "failed")
        self.assertFalse(r.success)

    def test_partial_not_success(self):
        r = RunResult("test", "partial", rows_failed=2)
        self.assertFalse(r.success)

    def test_str_representation(self):
        r = RunResult("mypipe", "success", rows_extracted=100, rows_loaded=95, rows_failed=5)
        s = str(r)
        self.assertIn("mypipe", s)
        self.assertIn("100", s)


class TestChainTransformer(unittest.TestCase):
    def test_chain_applies_in_order(self):
        """Verifica que los transformers se aplican en secuencia."""
        records = [{"name": "  Alice  ", "value": "10"}]

        chain = ChainTransformer(
            ValueNormalizer(strip=["name"]),
            TypeCaster(types={"value": "int"}),
        )
        result = chain.transform(records)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[0]["value"], 10)

    def test_chain_name(self):
        chain = ChainTransformer(
            IdentityTransformer(),
            ColumnMapper(rename={"a": "b"}),
        )
        self.assertIn("→", chain.name)

    def test_empty_input(self):
        chain = ChainTransformer(IdentityTransformer())
        self.assertEqual(chain.transform([]), [])


# ── Tests Extractors ──────────────────────────────────────────────

class TestInMemoryCsvExtractor(unittest.TestCase):
    CSV_DATA = "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Carol,300\n"

    def test_extracts_all_rows(self):
        extractor = InMemoryCsvExtractor(self.CSV_DATA)
        all_rows = []
        for chunk in extractor.extract():
            all_rows.extend(chunk)
        self.assertEqual(len(all_rows), 3)

    def test_chunk_size_respected(self):
        extractor = InMemoryCsvExtractor(self.CSV_DATA, chunk_size=2)
        chunks = list(extractor.extract())
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0]), 2)
        self.assertEqual(len(chunks[1]), 1)

    def test_fields_correct(self):
        extractor = InMemoryCsvExtractor(self.CSV_DATA)
        rows = list(extractor.extract())[0]
        self.assertEqual(rows[0]["id"], "1")
        self.assertEqual(rows[0]["name"], "Alice")

    def test_empty_csv(self):
        extractor = InMemoryCsvExtractor("id,name\n")
        chunks = list(extractor.extract())
        self.assertEqual(chunks, [])


class TestCsvExtractor(unittest.TestCase):
    def test_reads_csv_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            headers = ["order_id", "customer", "amount"]
            make_csv_file(tmp, "orders.csv",
                          [{"order_id": "1", "customer": "A", "amount": "10"},
                           {"order_id": "2", "customer": "B", "amount": "20"}],
                          headers)
            extractor = CsvExtractor(os.path.join(tmp, "orders.csv"))
            rows = []
            for chunk in extractor.extract():
                rows.extend(chunk)
            self.assertEqual(len(rows), 2)

    def test_reads_directory_of_csvs(self):
        with tempfile.TemporaryDirectory() as tmp:
            headers = ["id", "value"]
            make_csv_file(tmp, "file1.csv", [{"id": "1", "value": "a"}], headers)
            make_csv_file(tmp, "file2.csv", [{"id": "2", "value": "b"},
                                              {"id": "3", "value": "c"}], headers)
            extractor = CsvExtractor(tmp)
            rows = []
            for chunk in extractor.extract():
                rows.extend(chunk)
            self.assertEqual(len(rows), 3)

    def test_missing_file_raises(self):
        extractor = CsvExtractor("/does/not/exist.csv")
        with self.assertRaises(FileNotFoundError):
            list(extractor.extract())

    def test_strips_whitespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            make_csv_file(tmp, "data.csv",
                          [{"name": "  Alice  ", "value": "  100  "}],
                          ["name", "value"])
            extractor = CsvExtractor(os.path.join(tmp, "data.csv"), strip_whitespace=True)
            rows = list(extractor.extract())[0]
            self.assertEqual(rows[0]["name"], "Alice")
            self.assertEqual(rows[0]["value"], "100")


class TestSqliteExtractor(unittest.TestCase):
    def test_extracts_all_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{"id": str(i), "val": f"v{i}"} for i in range(10)]
            db = make_sqlite_db(tmp, "items", rows)
            extractor = SqliteExtractor(db, "SELECT * FROM items", chunk_size=3)
            result = []
            for chunk in extractor.extract():
                result.extend(chunk)
            self.assertEqual(len(result), 10)

    def test_respects_chunk_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{"id": str(i), "val": f"v{i}"} for i in range(7)]
            db = make_sqlite_db(tmp, "items", rows)
            extractor = SqliteExtractor(db, "SELECT * FROM items", chunk_size=3)
            chunks = list(extractor.extract())
            sizes = [len(c) for c in chunks]
            self.assertEqual(sum(sizes), 7)
            self.assertEqual(max(sizes), 3)

    def test_parameterized_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{"id": "1", "status": "active"},
                    {"id": "2", "status": "inactive"},
                    {"id": "3", "status": "active"}]
            db = make_sqlite_db(tmp, "items", rows)
            extractor = SqliteExtractor(
                db,
                "SELECT * FROM items WHERE status = :status",
                params={"status": "active"}
            )
            result = []
            for chunk in extractor.extract():
                result.extend(chunk)
            self.assertEqual(len(result), 2)


class TestSqliteIncrementalExtractor(unittest.TestCase):
    def test_initial_load_gets_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{"id": str(i), "created_at": f"2024-01-{i:02d}"} for i in range(1, 6)]
            db = make_sqlite_db(tmp, "events", rows)
            watermark_db = os.path.join(tmp, "watermarks.db")

            extractor = SqliteIncrementalExtractor(
                db_path=db, source_table="events",
                watermark_column="created_at", watermark_db=watermark_db,
                pipeline_name="test_pipe"
            )
            result = []
            for chunk in extractor.extract():
                result.extend(chunk)
            self.assertEqual(len(result), 5)

    def test_incremental_skips_old_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{"id": str(i), "created_at": f"2024-01-{i:02d}"} for i in range(1, 6)]
            db = make_sqlite_db(tmp, "events", rows)
            watermark_db = os.path.join(tmp, "watermarks.db")

            # Primera extracción
            ext1 = SqliteIncrementalExtractor(
                db_path=db, source_table="events",
                watermark_column="created_at", watermark_db=watermark_db,
                pipeline_name="test_pipe"
            )
            for _ in ext1.extract():
                pass

            # Añadir nuevos registros
            conn = sqlite3.connect(db)
            conn.execute("INSERT INTO events VALUES ('6', '2024-01-06')")
            conn.execute("INSERT INTO events VALUES ('7', '2024-01-07')")
            conn.commit()
            conn.close()

            # Segunda extracción — solo los nuevos
            ext2 = SqliteIncrementalExtractor(
                db_path=db, source_table="events",
                watermark_column="created_at", watermark_db=watermark_db,
                pipeline_name="test_pipe"
            )
            result = []
            for chunk in ext2.extract():
                result.extend(chunk)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["id"], "6")


# ── Tests Transformers ────────────────────────────────────────────

class TestColumnMapper(unittest.TestCase):
    def test_rename(self):
        records = [{"CustomerID": "1", "OrderDate": "2024-01-01"}]
        result = ColumnMapper(rename={"CustomerID": "customer_id",
                                       "OrderDate": "order_date"}).transform(records)
        self.assertIn("customer_id", result[0])
        self.assertNotIn("CustomerID", result[0])

    def test_keep_only(self):
        records = [{"a": 1, "b": 2, "c": 3}]
        result = ColumnMapper(keep=["a", "b"]).transform(records)
        self.assertNotIn("c", result[0])

    def test_defaults_filled(self):
        records = [{"name": "Alice"}]
        result = ColumnMapper(defaults={"source": "crm"}).transform(records)
        self.assertEqual(result[0]["source"], "crm")


class TestTypeCaster(unittest.TestCase):
    def test_casts_int(self):
        records = [{"qty": "10"}, {"qty": "20"}]
        result = TypeCaster(types={"qty": "int"}).transform(records)
        self.assertEqual(result[0]["qty"], 10)

    def test_casts_float(self):
        records = [{"price": "9.99"}]
        result = TypeCaster(types={"price": "float"}).transform(records)
        self.assertAlmostEqual(result[0]["price"], 9.99)

    def test_invalid_int_dropped(self):
        records = [{"qty": "abc"}, {"qty": "5"}]
        result = TypeCaster(types={"qty": "int"}, on_error="drop").transform(records)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["qty"], 5)

    def test_bool_cast(self):
        records = [{"active": "true"}, {"active": "false"}, {"active": "1"}]
        result = TypeCaster(types={"active": "bool"}).transform(records)
        self.assertTrue(result[0]["active"])
        self.assertFalse(result[1]["active"])
        self.assertTrue(result[2]["active"])

    def test_datetime_cast(self):
        records = [{"date": "2024-01-15"}]
        result = TypeCaster(types={"date": "datetime:%Y-%m-%d"}).transform(records)
        import pandas as pd
        self.assertIsInstance(result[0]["date"], pd.Timestamp)


class TestDuplicateFilter(unittest.TestCase):
    def test_removes_duplicates(self):
        records = [
            {"id": "1", "v": "a"},
            {"id": "2", "v": "b"},
            {"id": "1", "v": "c"},  # dup
        ]
        result = DuplicateFilter(subset=["id"], keep="last").transform(records)
        self.assertEqual(len(result), 2)
        # keep="last" -> id=1 debe tener v="c"
        ids = {r["id"]: r["v"] for r in result}
        self.assertEqual(ids["1"], "c")

    def test_no_duplicates_unchanged(self):
        records = [{"id": str(i)} for i in range(5)]
        result = DuplicateFilter(subset=["id"]).transform(records)
        self.assertEqual(len(result), 5)


class TestNullFilter(unittest.TestCase):
    def test_drops_nulls(self):
        import pandas as pd
        records = [
            {"id": "1", "name": "Alice"},
            {"id": None,  "name": "Bob"},
            {"id": "3", "name": None},
        ]
        result = NullFilter(required=["id", "name"]).transform(records)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "1")


class TestComputedColumns(unittest.TestCase):
    def test_adds_computed_column(self):
        records = [{"qty": 2.0, "price": 5.0}]
        import pandas as pd
        result = ComputedColumns({
            "revenue": lambda df: df["qty"] * df["price"]
        }).transform(records)
        self.assertAlmostEqual(result[0]["revenue"], 10.0)


# ── Tests Loaders ─────────────────────────────────────────────────

class TestSqliteLoader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "test.db")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _loader(self, mode="replace"):
        return SqliteLoader(self.db_path, "items", mode=mode, create_table=True)

    def test_loads_records(self):
        loader = self._loader()
        loader.begin()
        n = loader.load([{"id": "1", "val": "a"}, {"id": "2", "val": "b"}])
        loader.commit()
        self.assertEqual(n, 2)

        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)

    def test_replace_mode(self):
        loader = SqliteLoader(self.db_path, "items", mode="replace",
                              primary_keys=["id"])
        loader.begin()
        loader.load([{"id": "1", "val": "original"}])
        loader.commit()
        loader.begin()
        loader.load([{"id": "1", "val": "updated"}])
        loader.commit()

        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT val FROM items WHERE id='1'").fetchall()
        conn.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "updated")

    def test_rollback(self):
        loader = self._loader()
        loader.begin()
        loader.load([{"id": "99", "val": "x"}])
        loader.rollback()

        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS items (id TEXT, val TEXT)")
        count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_invalid_mode_raises(self):
        with self.assertRaises(ValueError):
            SqliteLoader(self.db_path, "items", mode="invalid")


class TestCsvFileLoader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.out_path = os.path.join(self.tmp, "output.csv")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_writes_csv(self):
        loader = CsvFileLoader(self.out_path)
        loader.begin()
        loader.load([{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}])
        loader.commit()

        with open(self.out_path) as f:
            content = f.read()
        self.assertIn("Alice", content)
        self.assertIn("Bob", content)

    def test_header_written(self):
        loader = CsvFileLoader(self.out_path)
        loader.begin()
        loader.load([{"col1": "v1", "col2": "v2"}])
        loader.commit()
        with open(self.out_path) as f:
            header = f.readline().strip()
        self.assertIn("col1", header)


# ── Integration Tests ─────────────────────────────────────────────

class TestFullPipeline(unittest.TestCase):
    """Test de integración: pipeline completo de extremo a extremo."""

    def test_csv_to_sqlite_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Preparar CSV de origen con datos reales y sucios
            csv_content = (
                "Order ID,Customer,Amount,Status\n"
                "1,Alice,100.00,Active\n"
                "2,Bob,200.50,active\n"
                "3,,50.00,active\n"         # nulo en Customer
                "4,Carol,abc,pending\n"      # amount inválido
                "1,Alice,105.00,Active\n"    # duplicado de order_id 1
            )
            csv_path = os.path.join(tmp, "orders.csv")
            with open(csv_path, "w") as f:
                f.write(csv_content)

            db_path = os.path.join(tmp, "dw.db")

            from etl.core.base import ChainTransformer, Pipeline
            from etl.extractors.csv_extractor import CsvExtractor
            from etl.loaders.sql_loader import SqliteLoader
            from etl.transformers.data_transformers import (
                ColumnMapper, DuplicateFilter, NullFilter, TypeCaster
            )

            pipeline = Pipeline(
                name="test_integration",
                extractor=CsvExtractor(csv_path, chunk_size=10),
                transformer=ChainTransformer(
                    ColumnMapper(rename={
                        "Order ID": "order_id",
                        "Customer": "customer",
                        "Amount":   "amount",
                        "Status":   "status",
                    }),
                    NullFilter(required=["customer"]),
                    TypeCaster(types={"order_id": "int", "amount": "float"}, on_error="drop"),
                    DuplicateFilter(subset=["order_id"], keep="last"),
                ),
                loader=SqliteLoader(db_path, "orders", mode="replace"),
                fail_fast=False,
            )

            result = pipeline.run()

            # Verificar resultado
            self.assertIn(result.status, ("success", "partial"))
            self.assertGreater(result.rows_extracted, 0)
            self.assertGreater(result.rows_loaded, 0)

            # Verificar datos en DB
            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT * FROM orders").fetchall()
            conn.close()

            # 5 extraídos - 1 nulo - 1 duplicado (kept last) - 1 invalid amount = 2 válidos
            # nulo (fila 3) eliminado por NullFilter
            # amount inválido (fila 4) eliminado por TypeCaster
            # duplicado 1 (fila 1) reemplazado por fila 5 (keep=last)
            # resultado esperado: order_id 2 y 1 (actualizado) → 2 registros
            self.assertEqual(len(rows), 2)

    def test_pipeline_continues_on_chunk_error(self):
        """Con fail_fast=False el pipeline continúa si un chunk falla."""
        calls = []

        class FlakyLoader(Loader):
            def begin(self): pass
            def commit(self): pass
            def rollback(self): pass
            def load(self, records):
                calls.append(len(records))
                if len(calls) == 2:
                    raise RuntimeError("Error en chunk 2")
                return len(records)

        csv_content = "\n".join(
            ["id,val"] + [f"{i},{i*10}" for i in range(30)]
        )
        pipeline = Pipeline(
            name="flaky_test",
            extractor=InMemoryCsvExtractor(csv_content, chunk_size=10),
            transformer=IdentityTransformer(),
            loader=FlakyLoader(),
            fail_fast=False,
        )
        result = pipeline.run()
        self.assertEqual(result.status, "partial")
        self.assertEqual(len(calls), 3)  # 3 chunks procesados


if __name__ == "__main__":
    unittest.main(verbosity=2)
