"""PostgreSQL database operations for ETL pipeline."""

import logging
from datetime import datetime
from typing import Any, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from .config import Config

logger = logging.getLogger(__name__)


class DatabaseUpserter:
    """Handles PostgreSQL bulk upsert operations."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize database upsert handler.

        Args:
            config: ETL configuration.
        """
        self.config = config or Config()
        self._connection = None

    def connect(self) -> psycopg2.extensions.connection:
        """Get or create database connection.

        Returns:
            PostgreSQL connection.
        """
        if self._connection is None or self._connection.closed:
            dsn = self.config.get_postgres_dsn()
            self._connection = psycopg2.connect(
                host=dsn["host"],
                port=dsn["port"],
                database=dsn["database"],
                user=dsn["user"],
                password=dsn["password"],
            )
            self._connection.autocommit = False
        return self._connection

    def close(self) -> None:
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None

    def create_table_if_not_exists(self) -> bool:
        """Create target table if it doesn't exist.

        Returns:
            True if successful.
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS records (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            status VARCHAR(50),
            updated_at TIMESTAMP,
            payload TEXT,
            _etl_processed_at TIMESTAMP,
            _etl_batch_id VARCHAR(50)
        )
        """
        conn = None
        try:
            conn = self.connect()
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            if conn:
                conn.rollback()
            return False

    def upsert_batch(
        self,
        df,
        table_name: Optional[str] = None,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        """Bulk upsert DataFrame using execute_values.

        Args:
            df: Polars DataFrame with records to upsert.
            table_name: Target table name. Uses config default if None.
            batch_size: Rows per upsert batch.

        Returns:
            Stats dict with upsert results.
        """
        table_name = table_name or self.config.POSTGRES_TABLE
        stats = {
            "rows_upserted": 0,
            "rows_failed": 0,
            "batches": 0,
        }

        records = []
        conn = None
        try:
            conn = self.connect()

            # Convert DataFrame to list of tuples
            records = df.to_dicts()
            if not records:
                logger.info("No records to upsert")
                return stats

            # Build column names (excluding id for UPDATE SET, but id is in VALUES)
            columns = [c for c in df.columns if c != "id"]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

            insert_sql = sql.SQL("""
                INSERT INTO {table} (id, {columns})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET {set_clause}
            """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                set_clause=sql.SQL(set_clause),
            )

            # Process in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                values = []
                for record in batch:
                    row = []
                    for col in df.columns:
                        val = record.get(col)
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        row.append(val)
                    values.append(tuple(row))

                try:
                    with conn.cursor() as cur:
                        execute_values(cur, insert_sql.as_string(conn), values)
                    stats["rows_upserted"] += len(batch)
                    stats["batches"] += 1
                except Exception as e:
                    logger.error(f"Batch upsert failed: {e}")
                    stats["rows_failed"] += len(batch)

            conn.commit()
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            if conn:
                conn.rollback()
            stats["rows_failed"] = len(records) if records else 0

        return stats

    def upsert_parquet_fallback(
        self,
        df,
        s3_client,
        key_suffix: str = "deduplicated.parquet",
    ) -> Optional[str]:
        """Write deduplicated Parquet to S3 as fallback when DB fails.

        Args:
            df: Polars DataFrame.
            s3_client: S3 client instance.
            key_suffix: Suffix for the output file key.

        Returns:
            S3 key of written file or None if failed.
        """
        from datetime import date
        import io

        try:
            # Write to processed prefix with date-based folder
            today = date.today().isoformat()
            key = f"{self.config.processed_prefix}{today}/{key_suffix}"

            # Convert DataFrame to Parquet bytes
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            buffer.seek(0)

            if s3_client.write_object(key, buffer.getvalue(), "application/parquet"):
                return key
            return None
        except Exception as e:
            logger.error(f"Failed to write fallback Parquet: {e}")
            return None


# Import needed for type hints
from typing import Optional