# S3 Parquet ETL Pipeline — Watermark-Based CDC

ETL pipeline for processing ~1M records/day from S3 Parquet files with change-data-capture (CDC) update detection.

## Architecture

```
S3 (raw/) → Watermark CDC filter → Polars batch processor → PostgreSQL upsert
                                   ↘ Dead-letter errors → S3 (.failed/)
S3 (.watermark/) ← Atomic update after successful run
```

## Stack

- **Python 3.11** · **boto3** · **Polars** · **pyarrow** · **psycopg2-binary** · **Apache Airflow**

## Project Structure

```
etl/
  config.py          — S3 bucket, DB, batch size config
  s3_client.py       — boto3 S3 operations (list, read Parquet, watermark I/O)
  watermark.py      — CDC watermark store (read/update atomically)
  parquet_processor.py — Polars batch reader + deduplication
  database.py        — PostgreSQL bulk upsert via execute_values
  failed_handler.py  — Dead-letter logging to s3://bucket/.failed/
scripts/
  process_batch.py  — Standalone batch processor (no Airflow)
airflow/
  dag.py             — Daily DAG: list → process → watermark → cleanup
tests/
  test_etl.py        — Unit tests for core ETL functions
requirements.txt
```

## Key Design Decisions

**Watermark-based CDC** — No full re-scans. Last run timestamp stored in `s3://bucket/.watermark/last_run.json`. Only files where `LastModified > watermark` are processed.

**Polars over Pandas** — 2-5x faster on large Parquet, lower memory. 100k-row batch chunks to avoid OOM on 1M record files.

**Atomic watermark update** — Copy new watermark → rename over old (S3 rename is atomic for same-key overwrites).

**Bulk upsert** — PostgreSQL `execute_values` for INSERT ON CONFLICT DO UPDATE. 100x faster than row-by-row.

## Setup

```bash
pip install -r requirements.txt

# Configure environment
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET=your-bucket
export PG_HOST=localhost
export PG_DATABASE=etl_db
export PG_USER=...
export PG_PASSWORD=...

# Run standalone processor
python scripts/process_batch.py

# Or trigger via Airflow
airflow dags trigger s3_parquet_etl_dag
```

## Testing

```bash
pytest tests/test_etl.py -v
```