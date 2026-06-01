.PHONY: test lint format clean run

test:
	python -m unittest tests.test_etl -v

lint:
	ruff check .

format:
	ruff format .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .venv/ .pytest_cache/

run:
	python -m pipelines.sales_pipeline --source data/raw/sales/ --db data/warehouse.db
