import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    PROJECT_ID, BQ_DATASET, BQ_RAW_TABLE,
    RAW_BUCKET, PROCESSED_BUCKET,
    QUARANTINE_BUCKET, SOURCE_FILE_NAME
)


def create_dataset_if_not_exists():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset {BQ_DATASET} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Dataset {BQ_DATASET} created successfully")


def load_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
    print(f"\n📤 Loading {len(df)} rows to BigQuery...")
    print(f"   Table: {table_ref}")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    try:
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()
        print(f"✅ Successfully loaded {len(df)} rows to BigQuery!")
        return True
    except Exception as e:
        print(f"❌ BigQuery load failed: {str(e)}")
        raise


def validate_load(df):
    client = bigquery.Client(project=PROJECT_ID)
    print(f"\n🔍 Running post load validation...")
    source_rows = len(df)
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
        WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
    """
    result = client.query(query).to_dataframe()
    bq_rows = result["row_count"][0]
    print(f"   Source rows   : {source_rows}")
    print(f"   BigQuery rows : {bq_rows}")
    if bq_rows >= source_rows:
        print(f"✅ Post load validation passed!")
        return True
    else:
        print(f"❌ Row count mismatch! Missing {source_rows - bq_rows} rows!")
        return False


def move_file_to_processed():
    client = storage.Client()
    print(f"\n📦 Moving file to processed bucket...")
    source_bucket = client.bucket(RAW_BUCKET)
    source_blob = source_bucket.blob(SOURCE_FILE_NAME)
    destination_bucket = client.bucket(PROCESSED_BUCKET)
    source_bucket.copy_blob(source_blob, destination_bucket, SOURCE_FILE_NAME)
    source_blob.delete()
    print(f"✅ File moved from raw → processed")


def move_file_to_quarantine():
    client = storage.Client()
    print(f"\n🚨 Moving file to quarantine bucket...")
    source_bucket = client.bucket(RAW_BUCKET)
    source_blob = source_bucket.blob(SOURCE_FILE_NAME)
    destination_bucket = client.bucket(QUARANTINE_BUCKET)
    source_bucket.copy_blob(source_blob, destination_bucket, SOURCE_FILE_NAME)
    source_blob.delete()
    print(f"✅ File moved from raw → quarantine")


if __name__ == "__main__":
    from ingestion.fetch_data import ingest_data
    from validator.quality_checks import run_all_checks
    df = ingest_data()
    passed = run_all_checks(df)
    if passed:
        create_dataset_if_not_exists()
        load_to_bigquery(df)
        validate_load(df)
        move_file_to_processed()
    else:
        move_file_to_quarantine()