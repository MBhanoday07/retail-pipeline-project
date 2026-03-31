import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def check_row_count(df):
    total_rows = len(df)
    if total_rows == 0:
        return False, f"File is empty — 0 rows found"
    elif total_rows < 100:
        return False, f"Too few rows: {total_rows} — expected at least 100"
    return True, f"Row count OK — {total_rows} rows found"


def check_required_columns(df):
    required_columns = [
        "ORDERNUMBER", "SALES", "ORDERDATE",
        "COUNTRY", "QUANTITYORDERED"
    ]
    missing_columns = []
    for col in required_columns:
        if col not in df.columns.tolist():
            missing_columns.append(col)
    if missing_columns:
        return False, f"Missing columns: {missing_columns}"
    return True, f"All required columns present"


def check_nulls(df):
    critical_columns = ["ORDERNUMBER", "SALES", "ORDERDATE"]
    threshold = 5.0
    total_rows = len(df)
    issues = []
    for col in critical_columns:
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / total_rows) * 100
        if null_percentage > threshold:
            issues.append(f"{col}: {null_percentage:.2f}% nulls")
    if issues:
        return False, f"Null threshold exceeded → {issues}"
    return True, f"Null check passed — all critical columns within threshold"


def check_duplicates(df):
    total_rows = len(df)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = (duplicate_count / total_rows) * 100
    if duplicate_percentage > 5.0:
        return False, f"Too many duplicates: {duplicate_count} rows ({duplicate_percentage:.2f}%)"
    return True, f"Duplicate check passed — {duplicate_count} duplicates found ({duplicate_percentage:.2f}%)"


def check_negative_sales(df):
    negative_count = (df["SALES"] < 0).sum()
    if negative_count > 0:
        return False, f"Found {negative_count} negative sales values!"
    return True, f"No negative sales values found"


def run_all_checks(df):
    print("\n" + "="*50)
    print("STARTING DATA QUALITY CHECKS")
    print("="*50)
    checks = [
        ("Row Count Check",       check_row_count(df)),
        ("Column Check",          check_required_columns(df)),
        ("Null Check",            check_nulls(df)),
        ("Duplicate Check",       check_duplicates(df)),
        ("Negative Sales Check",  check_negative_sales(df)),
    ]
    all_passed = True
    for check_name, (passed, message) in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check_name:25} : {status} — {message}")
        if not passed:
            all_passed = False
    print("="*50)
    if all_passed:
        print("OVERALL RESULT : ✅ ALL CHECKS PASSED — LOADING TO BIGQUERY")
    else:
        print("OVERALL RESULT : ❌ CHECKS FAILED — MOVING TO QUARANTINE")
    print("="*50)
    return all_passed


if __name__ == "__main__":
    from ingestion.fetch_data import ingest_data
    df = ingest_data()
    result = run_all_checks(df)

    print("\n\n🧪 TESTING WITH BAD DATA:")
    df_bad = df.copy()
    df_bad.loc[0:200, "SALES"] = None
    df_bad.loc[0:10, "SALES"] = -999
    run_all_checks(df_bad)