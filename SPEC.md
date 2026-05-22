# SPEC: S3 Parquet ETL Pipeline — ~1M Items/Day with CDC Update Detection

## 1. Project Overview

**Client:** Upwork — data engineering project
**Goal:** Build a Python-based ETL pipeline on AWS that processes ~1 million items per day from S3 Parquet files, with daily update detection (CDC pattern).
**Stack:** Python, AWS (S3, Lambda/ECS or EC2, SQS/SNS), Apache Parquet, boto3, Pandas/Polars, optional: dbt, Airflow for orchestration.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        S3 DATA LAKE                             │
│  s3://{bucket}/raw/   →  Parquet files deposited daily         │
│  s3://{bucket}/raw/   →  ~1M records/file                       │
└────────────┬──────────────────────┬─────────────────────────────┘
             │                      │ (optional: S3 Event Notification)
             ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                              │
│  • boto3 S3 ListObjectsV2 + filter by date prefix             │
│  • Watermark store: last_processed_timestamp (JSON in S3)     │
│  • Detect new/updated files via LastModified comparison        │
│  • Parquet schema validation on pull                           │
└────────────┬───────────────────────────────────────────────────┘
             │  filtered new/updated files
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PROCESSING LAYER                              │
│  • pandas / polars read Parquet batches                        │
│  • Deduplicate on composite key (e.g., id + updated_at)        │
│  • Type casting, null handling, schema normalization            │
│  • Upsert logic: INSERT new, UPDATE changed records             │
└────────────┬───────────────────────────────────────────────────┘
             │  cleaned records
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   OUTPUT LAYER                                 │
│  • Write processed Parquet to s3://{bucket}/processed/         │
│  • Write to PostgreSQL/RDS (optional) for query access        │
│  • Write watermark update to s3://{bucket}/.watermark        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Workstreams

### 3.1 Watermark-Based CDC (Update Detection)
- Store last successful run timestamp in `s3://{bucket}/.watermark/last_run.json`
- On each run: list S3 objects with prefix `raw/`, compare `LastModified` vs watermark
- Only process files where `LastModified > last_watermark`
- Update watermark atomically after successful processing (rename-overwrite pattern)
- **No full re-scans** — watermark ensures O(changed files) not O(all files)

### 3.2 S3 Parquet Ingestion
- Use `boto3.list_objects_v2` with `Prefix=raw/` + `StartAfter` for pagination
- Read Parquet with `pandas.read_parquet` or `polars.read_parquet` (，后者 faster for large files)
- Batch reads: 100k rows per chunk to avoid OOM on 1M record files
- Schema inference with explicit dtype overrides (nulls become wrong type in Parquet)

### 3.3 Deduplication & Upsert
- Composite key: `id` + `updated_at` (or `last_modified` from Parquet metadata)
- Sort by key + updated_at, drop duplicates keeping latest
- Upsert into target: PostgreSQL via `psycopg2.extras.execute_values` (bulk upsert)
- Alternative: write deduplicated Parquet to `processed/` prefix (append-only)

### 3.4 Error Handling & Observability
- Failed files logged to `s3://{bucket}/.failed/{date}/{filename}`
- CloudWatch metrics: records processed, failed, latency per run
- Lambda/ECS task: timeout 15min, memory 2GB+ for 1M record files
- Dead letter queue (SQS) for records that fail schema validation

### 3.5 Orchestration (Airflow)
- DAG: daily schedule (or triggered via S3 event → SQS → Airflow sensor)
- Tasks: `list_new_files` → `process_batch` (map) → `update_watermark` → `cleanup`
- Use Airflow S3KeySensor or S3EventSensor for trigger
- dbt for downstream Gold layer transformations

---

## 4. Data Model

### Raw Parquet Schema (input from client)
```
filename: {id: int64, name: string, email: string, status: string,
           updated_at: timestamp, payload: string}
```

### Processed Schema (output)
```
filename: {id: int64, name: string, email: string, status: string,
           updated_at: timestamp, payload: string,
           _etl_processed_at: timestamp, _etl_batch_id: string}
```

### Watermark JSON
```json
{
  "last_successful_run": "2025-05-21T00:00:00Z",
  "last_processed_file": "raw/2025-05-21/data.parquet",
  "records_processed": 987432
}
```

### Failed Files Log
```
s3://{bucket}/.failed/{YYYY-MM-DD}/{filename}.error
  → contains: error_type, error_message, record_count, timestamp
```

---

## 5. Technical Decisions

1. **Polars over Pandas for large files** — 2-5x faster on 1M+ row Parquet, lower memory usage
2. **Watermark stored in S3 as JSON** — no external DB needed for CDC state; atomic update via copy-then-rename
3. **boto3 over AWS Data Wrangler** — lighter dependency, full control over S3 pagination
4. **Batch upserts via `execute_values`** — PostgreSQL bulk upsert beats row-by-row by 100x
5. **S3 -> PostgreSQL as primary target** — enables SQL queries on processed data; Parquet-only is fallback
6. **Lambda vs ECS** — Lambda preferred for <15min runs; ECS if processing exceeds 15min (1M records ≈ 5-10min)
7. **Airflow DAG over Step Functions** — lower cost, our team knows it, easy to add dbt later

---

## 6. Out of Scope
- Client's data source (external API, database) — files arrive on S3, we don't build that
- Frontend/UI — processing is silent/scheduled
- Real-time streaming (Kinesis/Firehose) — batch daily as described
- Multi-region replication
- dbt Gold layer (add later if pipeline stabilizes)

---

## 7. Success Metrics
- All new/updated Parquet files processed within 1 daily run
- Zero duplicate records in target database (dedup verified via composite key)
- Watermark advances correctly — no re-processing of already-handled files
- Processing latency: <15 minutes for 1M record file (Lambda memory tuning required)
- Error rate: <0.1% records failing schema validation → logged to `.failed/`