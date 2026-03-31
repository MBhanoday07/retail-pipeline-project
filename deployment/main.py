import pandas as pd
import requests
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import os

# ─────────────────────────────────────
# CONFIG
# ─────────────────────────────────────
PROJECT_ID = "retail-pipeline-project"
RAW_BUCKET = "retail-pipeline-raw-shadow"
QUARANTINE_BUCKET = "retail-pipeline-quarantine-shadow"
PROCESSED_BUCKET = "retail-pipeline-processed-shadow"
BQ_DATASET = "retail_pipeline"
BQ_RAW_TABLE = "raw_sales"
CURRENCY_API_URL = "https://api.exchangerate-api.com/v4/latest/USD"


# ─────────────────────────────────────
# INGESTION
# ─────────────────────────────────────
def read_csv_from_gcs(file_name):
    print(f"📥 Reading {file_name} from GCS...")
    client = storage.Client()
    bucket = client.bucket(RAW_BUCKET)
    blob = bucket.blob(file_name)
    content = blob.download_as_text(encoding="latin-1")
    df = pd.read_csv(StringIO(content))
    print(f"✅ Read {len(df)} rows and {len(df.columns)} columns")
    return df


def fetch_exchange_rates():
    print("💱 Fetching exchange rates...")
    response = requests.get(CURRENCY_API_URL, timeout=10)
    if response.status_code == 200:
        rates = response.json()["rates"]
        print("✅ Exchange rates fetched!")
        return rates
    raise Exception(f"API failed: {response.status_code}")


def merge_with_exchange_rates(df, rates):
    print("🔀 Merging with exchange rates...")
    df["SALES_USD"] = df["SALES"].apply(
        lambda x: round(x / rates.get("USD", 1.0), 2)
    )
    df["EXCHANGE_RATE_DATE"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    df["INGESTION_TIMESTAMP"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    print("✅ Merge complete!")
    return df


# ─────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────
def run_all_checks(df):
    print("\n🔍 Running data quality checks...")
    all_passed = True

    # Row count check
    if len(df) < 100:
        print(f"❌ Row count failed: {len(df)} rows")
        all_passed = False
    else:
        print(f"✅ Row count OK: {len(df)} rows")

    # Column check
    required = ["ORDERNUMBER", "SALES", "ORDERDATE", "COUNTRY", "QUANTITYORDERED"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        all_passed = False
    else:
        print(f"✅ All required columns present")

    # Null check
    for col in ["ORDERNUMBER", "SALES", "ORDERDATE"]:
        null_pct = (df[col].isnull().sum() / len(df)) * 100
        if null_pct > 5:
            print(f"❌ Null check failed: {col} has {null_pct:.2f}% nulls")
            all_passed = False
        else:
            print(f"✅ Null check OK: {col}")

    # Duplicate check
    dupes = df.duplicated().sum()
    if dupes > 0:
        print(f"❌ Found {dupes} duplicate rows")
        all_passed = False
    else:
        print(f"✅ No duplicates found")

    # Negative sales check
    neg = (df["SALES"] < 0).sum()
    if neg > 0:
        print(f"❌ Found {neg} negative sales")
        all_passed = False
    else:
        print(f"✅ No negative sales")

    return all_passed


# ─────────────────────────────────────
# BIGQUERY
# ─────────────────────────────────────
def delete_existing_data_for_today():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
        WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
    """
    try:
        result = client.query(query).to_dataframe()
        existing_rows = result["row_count"][0]
        if existing_rows > 0:
            print(f"⚠️ Found {existing_rows} existing rows — deleting...")
            delete_query = f"""
                DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
                WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
            """
            client.query(delete_query).result()
            print(f"✅ Existing data deleted!")
    except Exception:
        print("ℹ️ Table doesn't exist yet — skipping delete")


def create_dataset_if_not_exists():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Dataset created")


def load_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
    print(f"📤 Loading {len(df)} rows to {table_ref}...")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows to BigQuery!")


def validate_load(df):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
        WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
    """
    result = client.query(query).to_dataframe()
    bq_rows = result["row_count"][0]
    source_rows = len(df)
    print(f"   Source rows   : {source_rows}")
    print(f"   BigQuery rows : {bq_rows}")
    if bq_rows >= source_rows:
        print("✅ Validation passed!")
        return True
    print("❌ Row count mismatch!")
    return False


# ─────────────────────────────────────
# FILE MOVEMENT
# ─────────────────────────────────────
def move_file(source_bucket_name, dest_bucket_name, file_name):
    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    dest_bucket = client.bucket(dest_bucket_name)
    source_bucket.copy_blob(source_blob, dest_bucket, file_name)
    source_blob.delete()
    print(f"✅ Moved {file_name} → {dest_bucket_name}")


# ─────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────
def run_pipeline(event, context):
    """
    Cloud Function entry point
    Triggered when file lands in GCS bucket
    """
    print("\n" + "="*50)
    print("PIPELINE TRIGGERED")
    print("="*50)

    file_name = event.get("name")
    bucket_name = event.get("bucket")

    print(f"📁 File   : {file_name}")
    print(f"🪣 Bucket : {bucket_name}")

    if not file_name.endswith(".csv"):
        print("⚠️ Not a CSV — skipping")
        return

    try:
        # STEP 1: INGEST
        print("\n📥 STEP 1: INGESTION")
        df = read_csv_from_gcs(file_name)
        rates = fetch_exchange_rates()
        df = merge_with_exchange_rates(df, rates)

        # STEP 2: VALIDATE
        print("\n🔍 STEP 2: VALIDATION")
        passed = run_all_checks(df)

        if passed:
            # STEP 3: LOAD
            print("\n📤 STEP 3: LOADING")
            create_dataset_if_not_exists()
            delete_existing_data_for_today()
            load_to_bigquery(df)

            # STEP 4: VALIDATE LOAD
            print("\n✅ STEP 4: POST LOAD VALIDATION")
            validate_load(df)

            # STEP 5: MOVE TO PROCESSED
            print("\n📦 STEP 5: MOVE TO PROCESSED")
            move_file(RAW_BUCKET, PROCESSED_BUCKET, file_name)

            print("\n" + "="*50)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            print("="*50)

        else:
            # QUARANTINE
            print("\n🚨 QUARANTINING FILE")
            move_file(RAW_BUCKET, QUARANTINE_BUCKET, file_name)
            print("\n" + "="*50)
            print("❌ PIPELINE FAILED — FILE QUARANTINED")
            print("="*50)

    except Exception as e:
        print(f"\n💥 PIPELINE CRASHED: {str(e)}")
        raise