import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.fetch_data import ingest_data
from validator.quality_checks import run_all_checks
from loader.bq_loader import (
    create_dataset_if_not_exists,
    load_to_bigquery,
    validate_load,
    move_file_to_processed,
    move_file_to_quarantine
)


def run_pipeline(event, context):
    print("\n" + "="*50)
    print("PIPELINE TRIGGERED")
    print("="*50)

    file_name = event.get("name")
    bucket_name = event.get("bucket")

    print(f"📁 File    : {file_name}")
    print(f"🪣 Bucket  : {bucket_name}")

    if not file_name.endswith(".csv"):
        print(f"⚠️ Not a CSV file — skipping")
        return

    try:
        print("\n📥 STEP 1: INGESTION")
        df = ingest_data()

        print("\n🔍 STEP 2: DATA QUALITY CHECKS")
        passed = run_all_checks(df)

        if passed:
            print("\n📤 STEP 3: LOADING TO BIGQUERY")
            create_dataset_if_not_exists()
            load_to_bigquery(df)

            print("\n✅ STEP 4: POST LOAD VALIDATION")
            validation_passed = validate_load(df)

            if validation_passed:
                print("\n📦 STEP 5: MOVE TO PROCESSED")
                move_file_to_processed()
                print("\n" + "="*50)
                print("✅ PIPELINE COMPLETED SUCCESSFULLY")
                print("="*50)
            else:
                print("\n❌ Post load validation failed!")
        else:
            print("\n🚨 STEP 3B: MOVING TO QUARANTINE")
            move_file_to_quarantine()
            print("\n" + "="*50)
            print("❌ PIPELINE FAILED — FILE QUARANTINED")
            print("="*50)

    except Exception as e:
        print(f"\n💥 PIPELINE CRASHED: {str(e)}")
        raise


if __name__ == "__main__":
    mock_event = {
        "name": "sales_data_sample.csv",
        "bucket": "retail-pipeline-raw-shadow"
    }
    mock_context = {}
    run_pipeline(mock_event, mock_context)