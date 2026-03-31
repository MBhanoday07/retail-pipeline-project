import pandas as pd
import requests
from google.cloud import storage
from io import StringIO
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    RAW_BUCKET,
    SOURCE_FILE_NAME,
    CURRENCY_API_URL
)


def read_csv_from_gcs():
    print(f"📥 Reading {SOURCE_FILE_NAME} from GCS bucket: {RAW_BUCKET}")
    try:
        client = storage.Client()
        bucket = client.bucket(RAW_BUCKET)
        blob = bucket.blob(SOURCE_FILE_NAME)
        content = blob.download_as_text(encoding="latin-1")
        df = pd.read_csv(StringIO(content))
        print(f"✅ Successfully read {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"❌ Failed to read from GCS: {str(e)}")
        raise


def fetch_exchange_rates():
    print(f"💱 Fetching live exchange rates...")
    max_retries = 3
    retry_count = 0
    while retry_count < max_retries:
        try:
            print(f"   Attempt {retry_count + 1} of {max_retries}")
            response = requests.get(CURRENCY_API_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                rates = data["rates"]
                print(f"✅ Exchange rates fetched successfully")
                print(f"   Sample rates → EUR: {rates.get('EUR')} | GBP: {rates.get('GBP')} | INR: {rates.get('INR')}")
                return rates
            else:
                print(f"❌ API returned status: {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"⏰ Request timed out!")
        except requests.exceptions.ConnectionError:
            print(f"🔌 Connection error!")
        retry_count += 1
        if retry_count < max_retries:
            print(f"⏳ Retrying...")
    raise Exception("❌ Currency API failed after 3 retries!")


def merge_with_exchange_rates(df, rates):
    print(f"🔀 Merging sales data with exchange rates...")
    def convert_to_usd(amount):
        usd_rate = rates.get("USD", 1.0)
        return round(amount / usd_rate, 2)
    df["SALES_USD"] = df["SALES"].apply(convert_to_usd)
    df["EXCHANGE_RATE_DATE"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    df["INGESTION_TIMESTAMP"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ Added SALES_USD, EXCHANGE_RATE_DATE, INGESTION_TIMESTAMP columns")
    return df


def ingest_data():
    print("\n" + "="*50)
    print("STARTING INGESTION")
    print("="*50)
    df = read_csv_from_gcs()
    rates = fetch_exchange_rates()
    df = merge_with_exchange_rates(df, rates)
    print("\n✅ INGESTION COMPLETE")
    print(f"   Total rows    : {len(df)}")
    print(f"   Total columns : {len(df.columns)}")
    return df


if __name__ == "__main__":
    df = ingest_data()
    print("\n📊 Sample Data (first 3 rows):")
    print(df[["ORDERNUMBER", "SALES", "SALES_USD", "INGESTION_TIMESTAMP"]].head(3))