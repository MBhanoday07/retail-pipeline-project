import pandas as pd
import requests
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import os

# ─────────────────────────────────────
# CONFIG
# ─────────────────────────────────────
PROJECT_ID        = "retail-pipeline-project"
RAW_BUCKET        = "retail-pipeline-raw-shadow"
QUARANTINE_BUCKET = "retail-pipeline-quarantine-shadow"
PROCESSED_BUCKET  = "retail-pipeline-processed-shadow"
BQ_DATASET        = "retail_pipeline"
BQ_RAW_TABLE      = "raw_sales"
CURRENCY_API_URL  = "https://api.exchangerate-api.com/v4/latest/USD"

# Required columns after normalization
REQUIRED_COLUMNS = ["ORDERNUMBER", "SALES", "ORDERDATE", "COUNTRY"]


# ─────────────────────────────────────
# INGESTION
# ─────────────────────────────────────
def read_csv_from_gcs(file_name):
    print(f"📥 Reading {file_name} from GCS...")
    client = storage.Client()
    bucket = client.bucket(RAW_BUCKET)
    blob = bucket.blob(file_name)
    try:
        content = blob.download_as_text(encoding="utf-8")
        print("   Encoding: UTF-8")
    except Exception:
        content = blob.download_as_text(encoding="latin-1")
        print("   Encoding: latin-1 (fallback)")
    df = pd.read_csv(StringIO(content))
    print(f"✅ Read {len(df)} rows and {len(df.columns)} columns")
    print(f"   Original columns: {df.columns.tolist()}")
    return df


def normalize_columns(df):
    """
    Normalizes column names to standard schema.
    Priority order matters - first match wins.
    Prevents duplicate column name conflicts.
    """
    print("\n🔄 Normalizing column names...")

    # Priority list - order matters!
    # First match for each standard name wins
    PRIORITY_MAPPING = [
        ("ORDERNUMBER"    , "ORDERNUMBER"),
        ("Order_ID"       , "ORDERNUMBER"),
        ("OrderID"        , "ORDERNUMBER"),
        ("order_id"       , "ORDERNUMBER"),
        ("Row_ID"         , "ROW_ID"),
        ("SALES"          , "SALES"),
        ("Sales"          , "SALES"),
        ("sale_amount"    , "SALES"),
        ("Revenue"        , "SALES"),
        ("Amount"         , "SALES"),
        ("ORDERDATE"      , "ORDERDATE"),
        ("Order_Date"     , "ORDERDATE"),
        ("Order Date"     , "ORDERDATE"),
        ("order_date"     , "ORDERDATE"),
        ("OrderDate"      , "ORDERDATE"),
        ("COUNTRY"        , "COUNTRY"),
        ("Country"        , "COUNTRY"),
        ("country"        , "COUNTRY"),
        ("CITY"           , "CITY"),
        ("City"           , "CITY"),
        ("QUANTITYORDERED", "QUANTITYORDERED"),
        ("Quantity"       , "QUANTITYORDERED"),
        ("PRODUCTLINE"    , "PRODUCTLINE"),
        ("Category"       , "PRODUCTLINE"),
        ("Sub_Category"   , "SUB_CATEGORY"),
        ("Product_Name"   , "PRODUCT_NAME"),
        ("DEALSIZE"       , "DEALSIZE"),
        ("Segment"        , "DEALSIZE"),
        ("STATUS"         , "STATUS"),
        ("Status"         , "STATUS"),
        ("Ship_Mode"      , "SHIP_MODE"),
    ]

    # Build rename dict - only rename if not already taken
    rename_dict = {}
    used_targets = set(df.columns.tolist())

    for source, target in PRIORITY_MAPPING:
        if source in df.columns:
            if source == target:
                used_targets.add(target)
            elif target not in used_targets:
                rename_dict[source] = target
                used_targets.add(target)

    df = df.rename(columns=rename_dict)
    print(f"✅ Normalized columns: {df.columns.tolist()}")
    return df


def fetch_exchange_rates():
    print("💱 Fetching exchange rates...")
    max_retries = 3
    retry_count = 0
    while retry_count < max_retries:
        try:
            print(f"   Attempt {retry_count + 1} of {max_retries}")
            response = requests.get(CURRENCY_API_URL, timeout=10)
            if response.status_code == 200:
                rates = response.json()["rates"]
                print("✅ Exchange rates fetched!")
                return rates
            else:
                print(f"❌ API status: {response.status_code}")
        except requests.exceptions.Timeout:
            print("⏰ Timeout!")
        except requests.exceptions.ConnectionError:
            print("🔌 Connection error!")
        retry_count += 1
        if retry_count < max_retries:
            print("⏳ Retrying...")
    raise Exception("❌ Currency API failed after 3 retries!")


def merge_with_exchange_rates(df, rates):
    print("🔀 Merging with exchange rates...")
    df["SALES_USD"] = df["SALES"].apply(
        lambda x: round(float(x) / rates.get("USD", 1.0), 2)
    )
    df["EXCHANGE_RATE_DATE"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    df["INGESTION_TIMESTAMP"] = pd.Timestamp.now()
    print("✅ Added SALES_USD, EXCHANGE_RATE_DATE, INGESTION_TIMESTAMP")
    return df


# ─────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────
def run_all_checks(df):
    print("\n" + "="*50)
    print("STARTING DATA QUALITY CHECKS")
    print("="*50)
    all_passed = True
    total_rows = len(df)

    # Check 1: Row count
    if total_rows == 0:
        print(f"❌ Row Count     : File is empty!")
        all_passed = False
    elif total_rows < 100:
        print(f"❌ Row Count     : Too few rows: {total_rows}")
        all_passed = False
    else:
        print(f"✅ Row Count     : {total_rows} rows found")

    # Check 2: Required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print(f"❌ Column Check  : Missing columns: {missing}")
        all_passed = False
    else:
        print(f"✅ Column Check  : All required columns present")

    # Check 3: Null check - using int() to get scalar value
    for col in REQUIRED_COLUMNS:
        if col in df.columns:
            null_count = int(df[col].isnull().sum())
            null_pct = round((null_count / total_rows) * 100, 2)
            if null_pct > 5:
                print(f"❌ Null Check    : {col} has {null_pct}% nulls")
                all_passed = False
            else:
                print(f"✅ Null Check    : {col} OK ({null_pct}% nulls)")

    # Check 4: Duplicate rows
    dupes = int(df.duplicated().sum())
    dupe_pct = round((dupes / total_rows) * 100, 2)
    if dupe_pct > 5:
        print(f"❌ Duplicate     : {dupes} duplicates ({dupe_pct}%)")
        all_passed = False
    else:
        print(f"✅ Duplicate     : {dupes} duplicates ({dupe_pct}%)")

    # Check 5: Negative sales
    if "SALES" in df.columns:
        neg = int((df["SALES"] < 0).sum())
        if neg > 0:
            print(f"❌ Negative Sales: {neg} negative values!")
            all_passed = False
        else:
            print(f"✅ Negative Sales: No negative values")

    print("="*50)
    if all_passed:
        print("OVERALL: ✅ ALL CHECKS PASSED — LOADING TO BIGQUERY")
    else:
        print("OVERALL: ❌ CHECKS FAILED — MOVING TO QUARANTINE")
    print("="*50)
    return all_passed


# ─────────────────────────────────────
# BIGQUERY
# ─────────────────────────────────────
def delete_existing_data_for_today():
    client = bigquery.Client(project=PROJECT_ID)
    print(f"\n🔍 Checking for existing data today...")
    check_query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
        WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
    """
    try:
        result = client.query(check_query).to_dataframe()
        existing_rows = int(result["row_count"][0])
        if existing_rows > 0:
            print(f"⚠️ Found {existing_rows} existing rows — deleting...")
            delete_query = f"""
                DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
                WHERE DATE(INGESTION_TIMESTAMP) = CURRENT_DATE()
            """
            client.query(delete_query).result()
            print(f"✅ Existing data deleted!")
        else:
            print(f"✅ No existing data — safe to load!")
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
    print(f"\n📤 Loading {len(df)} rows to BigQuery...")
    print(f"   Table: {table_ref}")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="INGESTION_TIMESTAMP"
        ),
        clustering_fields=["COUNTRY"]
    )
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()
    print(f"✅ Loaded {len(df)} rows!")
    print(f"   Partitioned by : INGESTION_TIMESTAMP (daily)")
    print(f"   Clustered by   : COUNTRY")


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
    bq_rows = int(result["row_count"][0])
    print(f"   Source rows   : {source_rows}")
    print(f"   BigQuery rows : {bq_rows}")
    if bq_rows >= source_rows:
        print(f"✅ Validation passed!")
        return True
    else:
        print(f"❌ Row count mismatch! Missing {source_rows - bq_rows} rows!")
        return False


# ─────────────────────────────────────
# FILE MOVEMENT
# ─────────────────────────────────────
def move_file(source_bucket_name, dest_bucket_name, file_name):
    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    if not source_blob.exists():
        print(f"⚠️ File not found in {source_bucket_name} — skipping")
        return
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
    Triggered automatically when file lands in GCS
    Handles any CSV with flexible schema normalization
    """
    print("\n" + "="*50)
    print("PIPELINE TRIGGERED")
    print("="*50)

    file_name   = event.get("name")
    bucket_name = event.get("bucket")

    print(f"📁 File   : {file_name}")
    print(f"🪣 Bucket : {bucket_name}")

    if not file_name.endswith(".csv"):
        print("⚠️ Not a CSV file — skipping")
        return

    try:
        # STEP 1: INGEST
        print("\n📥 STEP 1: INGESTION")
        df = read_csv_from_gcs(file_name)

        # STEP 2: NORMALIZE SCHEMA
        print("\n🔄 STEP 2: SCHEMA NORMALIZATION")
        df = normalize_columns(df)

        # STEP 3: CURRENCY ENRICHMENT
        print("\n💱 STEP 3: CURRENCY ENRICHMENT")
        rates = fetch_exchange_rates()
        df = merge_with_exchange_rates(df, rates)

        # STEP 4: DATA QUALITY CHECKS
        print("\n🔍 STEP 4: DATA QUALITY CHECKS")
        passed = run_all_checks(df)

        if passed:
            # STEP 5: LOAD TO BIGQUERY
            print("\n📤 STEP 5: LOADING TO BIGQUERY")
            create_dataset_if_not_exists()
            delete_existing_data_for_today()
            load_to_bigquery(df)

            # STEP 6: POST LOAD VALIDATION
            print("\n✅ STEP 6: POST LOAD VALIDATION")
            validate_load(df)

            # STEP 7: MOVE TO PROCESSED
            print("\n📦 STEP 7: MOVE TO PROCESSED")
            move_file(RAW_BUCKET, PROCESSED_BUCKET, file_name)

            print("\n" + "="*50)
            print("✅ PIPELINE COMPLETED SUCCESSFULLY")
            print("="*50)

        else:
            print("\n🚨 QUARANTINING FILE")
            move_file(RAW_BUCKET, QUARANTINE_BUCKET, file_name)
            print("\n" + "="*50)
            print("❌ PIPELINE FAILED — FILE QUARANTINED")
            print("="*50)

    except Exception as e:
        print(f"\n💥 PIPELINE CRASHED: {str(e)}")
        raise