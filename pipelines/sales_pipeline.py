"""
pipelines/sales_pipeline.py
Pipeline de ventas: CSV de transacciones → limpiar/enriquecer → SQLite DW

Este pipeline:
1. Lee CSVs de ventas desde un directorio (soporta múltiples archivos por mes)
2. Limpia y valida los datos (tipos, nulos, duplicados)
3. Calcula métricas derivadas (revenue, margen)
4. Carga en la tabla 'fact_sales' del Data Warehouse

Ejecutar:
    python -m pipelines.sales_pipeline --source data/raw/sales/ --db data/warehouse.db
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from etl.core.base import ChainTransformer, Pipeline
from etl.extractors.csv_extractor import CsvExtractor
from etl.loaders.sql_loader import SqliteLoader
from etl.transformers.data_transformers import (
    ColumnMapper,
    ComputedColumns,
    DuplicateFilter,
    NullFilter,
    TypeCaster,
    ValueNormalizer,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ── Configuración del transformer específico de ventas ────────────

COLUMN_MAPPING = {
    "Order ID":      "order_id",
    "Customer ID":   "customer_id",
    "Product":       "product_name",
    "Category":      "category",
    "Quantity":      "quantity",
    "Unit Price":    "unit_price",
    "Order Date":    "order_date",
    "Country":       "country",
    "Status":        "status",
}

REQUIRED_COLUMNS = list(COLUMN_MAPPING.values())

TYPE_SPECS = {
    "order_id":    "int",
    "customer_id": "int",
    "quantity":    "float",
    "unit_price":  "float",
    "order_date":  "datetime",
}


def build_pipeline(source_dir: str, db_path: str, chunk_size: int = 2000) -> Pipeline:
    """Construye el pipeline de ventas con todos sus componentes."""

    extractor = CsvExtractor(
        path=source_dir,
        glob_pattern="*.csv",
        chunk_size=chunk_size,
    )

    transformer = ChainTransformer(
        # 1. Renombrar columnas al esquema del DW
        ColumnMapper(
            rename=COLUMN_MAPPING,
            keep=REQUIRED_COLUMNS,
        ),
        # 2. Eliminar registros con campos obligatorios nulos
        NullFilter(required=["order_id", "customer_id", "order_date", "quantity", "unit_price"]),
        # 3. Castear tipos
        TypeCaster(types=TYPE_SPECS, on_error="drop"),
        # 4. Normalizar texto
        ValueNormalizer(
            lowercase=["status", "category"],
            strip=["product_name", "country"],
            replacements={
                "status": {
                    "Completed": "completed",
                    "COMPLETED": "completed",
                    "Cancelled": "cancelled",
                    "CANCELLED": "cancelled",
                    "Pending": "pending",
                }
            }
        ),
        # 5. Eliminar duplicados por order_id
        DuplicateFilter(subset=["order_id"], keep="last"),
        # 6. Calcular métricas derivadas
        ComputedColumns({
            "revenue":     lambda df: (df["quantity"] * df["unit_price"]).round(2),
            "year_month":  lambda df: pd.to_datetime(df["order_date"]).dt.to_period("M").astype(str),
            "loaded_at":   lambda df: pd.Timestamp.utcnow().isoformat(),
        }),
    )

    loader = SqliteLoader(
        db_path=db_path,
        table="fact_sales",
        mode="replace",
        create_table=True,
    )

    return Pipeline(
        name="sales_etl",
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        fail_fast=False,  # Continúa aunque falle un chunk
        log_every_n_chunks=5,
    )


def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL de ventas")
    parser.add_argument("--source", required=True, help="Directorio con CSVs de ventas")
    parser.add_argument("--db", default="data/warehouse.db", help="Ruta al Data Warehouse SQLite")
    parser.add_argument("--chunk-size", type=int, default=2000)
    args = parser.parse_args()

    source = Path(args.source)
    if not source.exists():
        log.error("Directorio de origen no existe: %s", source)
        sys.exit(1)

    pipeline = build_pipeline(str(source), args.db, args.chunk_size)
    result = pipeline.run()

    print(f"\n{'='*60}")
    print(f"  {result}")
    print(f"{'='*60}")

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
