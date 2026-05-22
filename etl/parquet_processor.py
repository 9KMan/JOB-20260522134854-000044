"""Parquet file processing with Polars."""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Generator, Iterator

import polars as pl

from .config import Config

logger = logging.getLogger(__name__)

# Expected schema for incoming Parquet files
EXPECTED_SCHEMA = {
    "id": pl.Int64,
    "name": pl.String,
    "email": pl.String,
    "status": pl.String,
    "updated_at": pl.String,  # ISO format string, will cast to datetime
    "payload": pl.String,
}

# Metadata columns added during processing
METADATA_COLUMNS = ["_etl_processed_at", "_etl_batch_id"]


class ParquetProcessor:
    """Processes Parquet files for ETL pipeline."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize processor.

        Args:
            config: ETL configuration.
        """
        self.config = config or Config()

    def read_parquet_bytes(self, data: bytes) -> pl.LazyFrame:
        """Read Parquet bytes into a Polars LazyFrame.

        Args:
            data: Parquet file bytes.

        Returns:
            Polars LazyFrame.
        """
        import io
        return pl.scan_parquet(io.BytesIO(data))

    def read_parquet_file(self, file_path: str) -> pl.LazyFrame:
        """Read Parquet file from local path.

        Args:
            file_path: Local file path.

        Returns:
            Polars LazyFrame.
        """
        return pl.scan_parquet(file_path)

    def yield_batches(
        self,
        df: pl.LazyFrame,
        batch_size: int = 100000,
    ) -> Generator[pl.DataFrame, None, None]:
        """Yield DataFrames in batches.

        Args:
            df: Polars LazyFrame.
            batch_size: Number of rows per batch.

        Yields:
            DataFrames of batch_size rows.
        """
        # Collect in batches using slice
        total_rows = df.select(pl.count()).collect().item()
        offset = 0

        while offset < total_rows:
            batch_df = df.slice(offset, batch_size).collect()
            yield batch_df
            offset += batch_size

    def yield_batches_from_bytes(
        self,
        data: bytes,
        batch_size: Optional[int] = None,
    ) -> Generator[pl.DataFrame, None, None]:
        """Yield batches from Parquet bytes.

        Args:
            data: Parquet file bytes.
            batch_size: Rows per batch. Uses config default if None.

        Yields:
            DataFrames of batch_size rows.
        """
        batch_size = batch_size or self.config.BATCH_SIZE
        df = self.read_parquet_bytes(data)
        yield from self.yield_batches(df, batch_size)

    def deduplicate(
        self,
        df: pl.DataFrame,
        composite_key: tuple[str, str] = ("id", "updated_at"),
    ) -> pl.DataFrame:
        """Deduplicate DataFrame on composite key keeping latest.

        Args:
            df: Input DataFrame.
            composite_key: Tuple of column names for deduplication.

        Returns:
            Deduplicated DataFrame.
        """
        id_col, updated_col = composite_key

        # Sort by id and updated_at descending
        sorted_df = df.sort([updated_col, id_col], descending=[True, False])

        # Drop duplicates keeping first (which is latest due to sort)
        deduplicated = sorted_df.unique(subset=[id_col], keep="first")

        return deduplicated

    def normalize_schema(
        self,
        df: pl.DataFrame,
        schema: dict[str, pl.DataType] = EXPECTED_SCHEMA,
    ) -> pl.DataFrame:
        """Normalize DataFrame to expected schema.

        Args:
            df: Input DataFrame.
            schema: Expected schema dict.

        Returns:
            Normalized DataFrame.
        """
        # Cast columns to expected types
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    if dtype == pl.String and df[col].dtype != pl.String:
                        df = df.with_columns(pl.col(col).cast(pl.String))
                    elif dtype == pl.Int64 and df[col].dtype != pl.Int64:
                        df = df.with_columns(pl.col(col).cast(pl.Int64))
                    elif dtype == pl.Datetime or dtype == pl.String:
                        # Handle timestamp strings
                        if col == "updated_at" and df[col].dtype == pl.String:
                            df = df.with_columns(
                                pl.col(col).str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")
                            )
                except Exception as e:
                    logger.warning(f"Failed to cast column {col}: {e}")

        return df

    def add_metadata(
        self,
        df: pl.DataFrame,
        batch_id: Optional[str] = None,
    ) -> pl.DataFrame:
        """Add ETL metadata columns to DataFrame.

        Args:
            df: Input DataFrame.
            batch_id: Optional batch ID. Generated if not provided.

        Returns:
            DataFrame with metadata columns.
        """
        batch_id = batch_id or str(uuid.uuid4())
        processed_at = datetime.now(timezone.utc).isoformat()

        return df.with_columns([
            pl.lit(processed_at).alias("_etl_processed_at"),
            pl.lit(batch_id).alias("_etl_batch_id"),
        ])

    def process_batch(
        self,
        data: bytes,
        batch_id: Optional[str] = None,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Process Parquet bytes through full pipeline.

        Args:
            data: Parquet file bytes.
            batch_id: Optional batch ID.

        Returns:
            Tuple of (processed DataFrame, stats dict).
        """
        batch_id = batch_id or str(uuid.uuid4())
        stats = {
            "batch_id": batch_id,
            "input_rows": 0,
            "output_rows": 0,
            "duplicates_removed": 0,
        }

        # Read and collect
        df = self.read_parquet_bytes(data).collect()
        stats["input_rows"] = len(df)

        # Normalize schema
        df = self.normalize_schema(df)

        # Deduplicate
        original_count = len(df)
        df = self.deduplicate(df)
        stats["duplicates_removed"] = original_count - len(df)

        # Add metadata
        df = self.add_metadata(df, batch_id)
        stats["output_rows"] = len(df)

        return df, stats

    def process_file_batched(
        self,
        data: bytes,
        batch_size: Optional[int] = None,
    ) -> Iterator[tuple[pl.DataFrame, dict[str, Any]]]:
        """Process Parquet file in batches.

        Args:
            data: Parquet file bytes.
            batch_size: Rows per batch.

        Yields:
            Tuples of (batch DataFrame, batch stats).
        """
        batch_size = batch_size or self.config.BATCH_SIZE
        batch_id = str(uuid.uuid4())
        offset = 0
        batch_num = 0

        df = self.read_parquet_bytes(data).collect()
        total_rows = len(df)

        while offset < total_rows:
            batch_df = df.slice(offset, batch_size)
            batch_num += 1

            # Normalize and deduplicate each batch
            batch_df = self.normalize_schema(batch_df)
            original_count = len(batch_df)
            batch_df = self.deduplicate(batch_df)

            # Add metadata
            batch_df = self.add_metadata(batch_df, f"{batch_id}_b{batch_num}")

            stats = {
                "batch_id": batch_id,
                "batch_num": batch_num,
                "input_rows": original_count,
                "output_rows": len(batch_df),
                "duplicates_removed": original_count - len(batch_df),
            }

            yield batch_df, stats
            offset += batch_size


# Import Optional for type hints
from typing import Optional