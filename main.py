import functions_framework
import pandas as pd
import io
import json
from google.cloud import storage, bigquery
from datetime import datetime, timezone

PROJECT_ID = "cei-bigquery-sbx-ea-01"       # ← replace this
DATASET_ID = "utility_data"
TABLE_ID   = "equipment_inspections"

@functions_framework.cloud_event
def process_inspection_csv(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]

    # Only process CSV files
    if not file_name.endswith(".csv"):
        print(f"Skipping non-CSV file: {file_name}")
        return

    print(f"Processing: gs://{bucket_name}/{file_name}")

    # Download the file from GCS
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    # Load into pandas and clean
    df = pd.read_csv(io.StringIO(content))
    print(f"Raw rows: {len(df)}")

 # --- ADD THE FIX HERE ---
    # Drop rows where the 'inspection_dt' column is empty or invalid
    df.dropna(subset=['inspection_dt'], inplace=True)
    # --
    # Standardize column names (lowercase, underscores)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Drop blank rows
    df.dropna(how="all", inplace=True)

    # Rename columns to match BigQuery schema (adjust to your real CSV headers)
    df.rename(columns={
        "equip_id":        "equipment_id",
        "inspection_dt":   "inspection_date",
        "inspector":       "inspector_name",
        "loc":             "location",
        "insp_status":     "status",
        "comments":        "notes"
    }, inplace=True)

    # Convert inspection_date to ISO 8601 timestamp
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Add a load timestamp
    df["load_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Keep only schema columns
    schema_cols = ["equipment_id","inspection_date","inspector_name","location","status","notes","load_timestamp"]
    df = df[[c for c in schema_cols if c in df.columns]]

    print(f"Clean rows to insert: {len(df)}")

    # Write to BigQuery
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = bq_client.insert_rows_json(table_ref, df.to_dict(orient="records"))

    if errors:
        print(f"BigQuery insert errors: {errors}")
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    else:
     	   print(f"Successfully inserted {len(df)} rows into {table_ref}")
