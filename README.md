# python-etl-framework

Framework ETL Python para pipelines de datos en producción. Patrón Extractor → Transformer → Loader con chunking, manejo de errores, watermarks incrementales y métricas de ejecución.

## Componentes

**Extractors** — `etl/extractors/`
- `CsvExtractor` — CSV locales o directorios completos, soporta gzip
- `InMemoryCsvExtractor` — CSV desde string (webhooks, tests)
- `SqliteExtractor` — SQLite con paginación automática y parámetros
- `SqliteIncrementalExtractor` — Extracción incremental con watermark persistente

**Transformers** — `etl/transformers/`
- `ColumnMapper` — renombrar, filtrar, defaults
- `TypeCaster` — casteo de tipos con manejo de errores
- `DuplicateFilter` — deduplicación por clave
- `NullFilter` — eliminar registros con campos obligatorios vacíos
- `ValueNormalizer` — normalización de texto
- `ComputedColumns` — columnas calculadas con lambdas
- `ChainTransformer` — encadena múltiples transformers

**Loaders** — `etl/loaders/`
- `SqliteLoader` — INSERT / INSERT OR REPLACE / INSERT OR IGNORE con batch
- `CsvFileLoader` — escritura CSV con modo overwrite/append

## Uso

```python
from etl.core.base import ChainTransformer, Pipeline
from etl.extractors.csv_extractor import CsvExtractor
from etl.loaders.sql_loader import SqliteLoader
from etl.transformers.data_transformers import ColumnMapper, TypeCaster, NullFilter

pipeline = Pipeline(
    name="orders_etl",
    extractor=CsvExtractor("data/orders/", chunk_size=5000),
    transformer=ChainTransformer(
        ColumnMapper(rename={"Order ID": "order_id", "Amount": "amount"}),
        NullFilter(required=["order_id"]),
        TypeCaster(types={"order_id": "int", "amount": "float"}),
    ),
    loader=SqliteLoader("data/warehouse.db", "fact_orders",
                        mode="replace", primary_keys=["order_id"]),
    fail_fast=False,
)

result = pipeline.run()
print(result)  # [orders_etl] SUCCESS | extracted=12400 loaded=12380 failed=20 duration=3.2s
```

## Tests

```bash
python -m unittest tests.test_etl -v
```

## Pipeline de ejemplo

```bash
python -m pipelines.sales_pipeline --source data/raw/sales/ --db data/warehouse.db
```
