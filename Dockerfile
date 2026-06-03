FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY etl/ etl/
COPY pipelines/ pipelines/

ENTRYPOINT ["python"]
CMD ["-m", "pipelines.sales_pipeline"]
