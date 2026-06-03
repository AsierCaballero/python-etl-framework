# python-etl-framework

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A modular ETL framework for production data pipelines in Python. Follows the Extractor -> Transformer -> Loader pattern with chunked processing, error handling, incremental watermarks, and execution metrics.

## Features

- **Pluggable architecture** -- each pipeline is composed of independent extractor, transformer, and loader components
- **Chunked processing** -- streams data in configurable chunks, never loading the full dataset into memory
- **Incremental extraction** -- built-in watermark persistence for delta loads from SQLite sources
- **Graceful error handling** -- configurable `fail_fast` mode; per-chunk error recovery with partial success reporting
- **Rich transformers** -- rename, type-cast, deduplicate, filter nulls, normalize text, compute columns, or chain any combination
- **Batch loading** -- micro-batch inserts with upsert (INSERT OR REPLACE / INSERT OR IGNORE) support
- **Execution metrics** -- each run returns a `RunResult` with extracted/loaded/failed counts and duration
- **Docker support** -- ready-to-use container image for scheduled pipelines

## Quick Start

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

## Project Structure

```
python-etl-framework/
├── etl/
│   ├── core/            # Base classes: Pipeline, Extractor, Transformer, Loader, RunResult
│   ├── extractors/      # CsvExtractor, InMemoryCsvExtractor, SqliteExtractor, SqliteIncrementalExtractor
│   ├── transformers/    # ColumnMapper, TypeCaster, DuplicateFilter, NullFilter, ValueNormalizer, ComputedColumns
│   └── loaders/         # SqliteLoader, CsvFileLoader
├── pipelines/           # Ready-to-run pipeline examples (e.g. sales_pipeline)
├── tests/               # Unit and integration tests
├── Dockerfile           # Production container (python:3.11-slim)
├── Makefile             # test, lint, format, clean targets
└── pyproject.toml       # Ruff configuration
```

### Components

| Layer | Component | Description |
|---|---|---|
| **Extractors** | `CsvExtractor` | Reads CSV files or directories (supports gzip, custom delimiters) |
| | `InMemoryCsvExtractor` | Reads CSV from a string (webhooks, tests) |
| | `SqliteExtractor` | Paginated SQLite queries with parameter binding |
| | `SqliteIncrementalExtractor` | Delta extraction with persistent watermarks |
| **Transformers** | `ColumnMapper` | Rename, keep/drop columns, set defaults |
| | `TypeCaster` | Type coercion (int, float, bool, datetime, str) with error handling |
| | `DuplicateFilter` | Deduplication by key columns |
| | `NullFilter` | Remove records with empty required fields |
| | `ValueNormalizer` | Text normalization (lower/upper, strip, replacements) |
| | `ComputedColumns` | Lambda-based computed columns |
| | `ChainTransformer` | Sequentially apply multiple transformers |
| **Loaders** | `SqliteLoader` | INSERT / INSERT OR REPLACE / INSERT OR IGNORE with configurable batch size |
| | `CsvFileLoader` | CSV output with overwrite or append mode |

## Development

```bash
# Run tests
python -m unittest tests.test_etl -v

# Lint and format
make lint
make format

# Run the example pipeline
python -m pipelines.sales_pipeline --source data/raw/sales/ --db data/warehouse.db

# Docker
docker build -t python-etl-framework .
docker run python-etl-framework
```

## License

MIT
