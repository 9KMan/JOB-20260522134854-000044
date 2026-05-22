"""Unit tests for ETL pipeline components."""

import io
import json
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


class TestWatermarkStore:
    """Tests for WatermarkStore."""

    def test_watermark_read_nonexistent(self):
        """Test reading watermark when file doesn't exist."""
        from etl.watermark import WatermarkStore
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)
        watermark_store = WatermarkStore(s3_client)

        # Mock S3 client to raise NoSuchKey
        s3_client.client.get_object = MagicMock(
            side_effect=Exception("NoSuchKey")
        )

        result = watermark_store.read()
        assert result is None

    def test_watermark_read_success(self):
        """Test successful watermark read."""
        from etl.watermark import WatermarkStore
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)

        expected_data = {
            "last_successful_run": "2025-05-21T00:00:00Z",
            "last_processed_file": "raw/2025-05-21/data.parquet",
            "records_processed": 987432,
        }

        # Mock S3 response
        s3_client.client.get_object = MagicMock(
            return_value={
                "Body": io.BytesIO(json.dumps(expected_data).encode())
            }
        )

        watermark_store = WatermarkStore(s3_client)
        result = watermark_store.read()

        assert result == expected_data

    def test_watermark_update(self):
        """Test watermark update with atomic copy-rename."""
        from etl.watermark import WatermarkStore
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)
        watermark_store = WatermarkStore(s3_client)

        # Track calls
        written_keys = []
        copied_keys = []
        deleted_keys = []

        def mock_write(Bucket, Key, Body, ContentType=None):
            written_keys.append(Key)
            return True

        def mock_copy(CopySource):
            copied_keys.append(CopySource)
            return True

        def mock_delete(Bucket, Key):
            deleted_keys.append(Key)
            return True

        s3_client.write_object = mock_write
        s3_client.copy_object = mock_copy
        s3_client.delete_object = mock_delete

        result = watermark_store.update(
            last_processed_file="raw/test.parquet",
            records_processed=1000,
        )

        assert result is True
        # Should write temp, copy to dest, delete temp
        assert len(written_keys) >= 1
        assert len(copied_keys) >= 1
        assert len(deleted_keys) >= 1


class TestS3Client:
    """Tests for S3Client."""

    def test_list_files_since_watermark(self):
        """Test filtering files by watermark timestamp."""
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)

        # Mock paginator response
        test_objects = [
            {
                "Key": "raw/2025-05-20/file1.parquet",
                "LastModified": datetime(2025, 5, 20, 10, 0, 0),
                "Size": 1000,
            },
            {
                "Key": "raw/2025-05-21/file2.parquet",
                "LastModified": datetime(2025, 5, 21, 10, 0, 0),
                "Size": 2000,
            },
            {
                "Key": "raw/2025-05-22/file3.parquet",
                "LastModified": datetime(2025, 5, 22, 10, 0, 0),
                "Size": 3000,
            },
        ]

        s3_client.list_objects_paginated = MagicMock(
            return_value=[test_objects]
        )

        watermark = {
            "last_successful_run": "2025-05-21T00:00:00Z",
            "last_processed_file": "",
            "records_processed": 0,
        }

        files = s3_client.list_files_since_watermark(watermark)

        # Should only return files newer than watermark
        assert len(files) == 1
        assert files[0]["Key"] == "raw/2025-05-22/file3.parquet"

    def test_list_files_excludes_watermark_prefix(self):
        """Test that watermark files are excluded from listing."""
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)

        test_objects = [
            {
                "Key": ".watermark/last_run.json",
                "LastModified": datetime.now(),
                "Size": 100,
            },
            {
                "Key": "raw/2025-05-22/file.parquet",
                "LastModified": datetime.now(),
                "Size": 1000,
            },
        ]

        s3_client.list_objects_paginated = MagicMock(
            return_value=[test_objects]
        )

        files = s3_client.list_files_since_watermark(None)

        assert all(".watermark" not in f["Key"] for f in files)
        assert len(files) == 1


class TestParquetProcessor:
    """Tests for ParquetProcessor."""

    def test_deduplication(self):
        """Test deduplication on composite key."""
        from etl.parquet_processor import ParquetProcessor

        processor = ParquetProcessor()

        # Create test data with duplicates
        df = pl.DataFrame({
            "id": [1, 1, 2, 2, 3],
            "updated_at": [
                "2025-05-20T10:00:00",
                "2025-05-21T10:00:00",  # Latest for id=1
                "2025-05-20T10:00:00",
                "2025-05-21T10:00:00",  # Latest for id=2
                "2025-05-22T10:00:00",
            ],
            "name": ["a", "b", "c", "d", "e"],
            "email": ["a@test", "b@test", "c@test", "d@test", "e@test"],
            "status": ["active"] * 5,
            "payload": ["p1", "p2", "p3", "p4", "p5"],
        })

        deduplicated = processor.deduplicate(df, ("id", "updated_at"))

        # Should have 3 rows (one per id)
        assert len(deduplicated) == 3
        # Should keep latest updated_at per id
        assert deduplicated.filter(pl.col("id") == 1)["updated_at"][0] == "2025-05-21T10:00:00"
        assert deduplicated.filter(pl.col("id") == 2)["updated_at"][0] == "2025-05-21T10:00:00"

    def test_add_metadata(self):
        """Test adding ETL metadata columns."""
        from etl.parquet_processor import ParquetProcessor

        processor = ParquetProcessor()

        df = pl.DataFrame({
            "id": [1, 2],
            "name": ["a", "b"],
        })

        result = processor.add_metadata(df, "test-batch-123")

        assert "_etl_processed_at" in result.columns
        assert "_etl_batch_id" in result.columns
        assert result["_etl_batch_id"][0] == "test-batch-123"

    def test_process_batch(self):
        """Test full batch processing pipeline."""
        from etl.parquet_processor import ParquetProcessor

        processor = ParquetProcessor()

        # Create test parquet data
        df = pl.DataFrame({
            "id": [1, 1, 2],
            "updated_at": ["2025-05-20T10:00:00", "2025-05-21T10:00:00", "2025-05-20T10:00:00"],
            "name": ["a", "b", "c"],
            "email": ["a@test", "b@test", "c@test"],
            "status": ["active"] * 3,
            "payload": ["p1", "p2", "p3"],
        })

        # Write to bytes
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)
        parquet_bytes = buffer.getvalue()

        # Process
        result_df, stats = processor.process_batch(parquet_bytes)

        assert stats["input_rows"] == 3
        assert stats["output_rows"] == 2  # After dedup
        assert stats["duplicates_removed"] == 1
        assert "_etl_batch_id" in result_df.columns


class TestDatabaseUpserter:
    """Tests for DatabaseUpserter."""

    def test_upsert_parquet_fallback(self):
        """Test fallback Parquet write when DB fails."""
        from etl.database import DatabaseUpserter
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)
        db_upsert = DatabaseUpserter(config)

        df = pl.DataFrame({
            "id": [1, 2],
            "name": ["a", "b"],
            "email": ["a@test", "b@test"],
            "status": ["active"] * 2,
            "updated_at": ["2025-05-20T10:00:00", "2025-05-21T10:00:00"],
            "payload": ["p1", "p2"],
        })

        written_key = []
        def mock_write(key, data, content_type=None):
            written_key.append(key)
            return True

        s3_client.write_object = mock_write

        result = db_upsert.upsert_parquet_fallback(df, s3_client)

        assert result is not None
        assert len(written_key) == 1
        assert "processed" in written_key[0]


class TestFailedFileHandler:
    """Tests for FailedFileHandler."""

    def test_write_error_log(self):
        """Test error log writing to S3."""
        from etl.failed_handler import FailedFileHandler
        from etl.s3_client import S3Client
        from etl.config import Config

        config = Config()
        s3_client = S3Client(config)
        handler = FailedFileHandler(s3_client, config)

        written_data = []
        def mock_write(key, data, content_type=None):
            written_data.append((key, data))
            return True

        s3_client.write_object = mock_write

        result = handler.write_error_log(
            filename="test.parquet",
            error_type="ProcessingError",
            error_message="Test error",
            record_count=100,
        )

        assert result is True
        assert len(written_data) == 1

        # Parse and verify JSON
        key, data = written_data[0]
        error_log = json.loads(data)

        assert error_log["error_type"] == "ProcessingError"
        assert error_log["error_message"] == "Test error"
        assert error_log["record_count"] == 100
        assert "timestamp" in error_log


class TestConfig:
    """Tests for Config."""

    def test_watermark_path(self):
        """Test watermark path property."""
        from etl.config import Config

        config = Config()
        config.S3_BUCKET = "test-bucket"

        expected = "s3://test-bucket/.watermark/last_run.json"
        assert config.watermark_path == expected

    def test_postgres_dsn(self):
        """Test PostgreSQL DSN generation."""
        from etl.config import Config

        config = Config()
        config.POSTGRES_HOST = "localhost"
        config.POSTGRES_PORT = 5432
        config.POSTGRES_DB = "testdb"
        config.POSTGRES_USER = "testuser"
        config.POSTGRES_PASSWORD = "testpass"

        dsn = config.get_postgres_dsn()

        assert dsn["host"] == "localhost"
        assert dsn["port"] == 5432
        assert dsn["database"] == "testdb"
        assert dsn["user"] == "testuser"
        assert dsn["password"] == "testpass"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])