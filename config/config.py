import os
from dotenv import load_dotenv

# Load all variables from .env file
load_dotenv()

# GCP Project
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# GCS Buckets
RAW_BUCKET = os.getenv("RAW_BUCKET")
QUARANTINE_BUCKET = os.getenv("QUARANTINE_BUCKET")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")

# BigQuery
BQ_DATASET = "retail_pipeline"
BQ_RAW_TABLE = "raw_sales"

# Source file
SOURCE_FILE_NAME = "sales_data_sample.csv"

# Currency API
CURRENCY_API_URL = "https://api.exchangerate-api.com/v4/latest/USD"