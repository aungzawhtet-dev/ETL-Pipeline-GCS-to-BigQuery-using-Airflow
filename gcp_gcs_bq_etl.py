from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = "white-terra-464503-v1"
BQ_DATASET = "etl_demo"
RAW_TABLE = "raw_house_price"
CURATED_TABLE = "house_price_curated"

GCS_BUCKET = "etl-demo-raw-data"         # Your bucket
GCS_OBJECT = "house-price.csv"           # File at bucket root

# This must match the Connection Id you have in Airflow UI
GCP_CONN_ID = "gcp_default"

# BigQuery location (change if your dataset is elsewhere, e.g. "asia-southeast1")
BQ_LOCATION = "us-central1"
# ---------------------------------------------------------------------------


with DAG(
    dag_id="etl_gcs_to_bq_house_price",
    description="ETL: Load house price CSV from GCS to BigQuery and transform",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gcp", "bigquery", "etl", "gcs"],
) as dag:

    # ------------------------------------------------------------------
    # 1) LOAD RAW FROM GCS INTO BIGQUERY
    # ------------------------------------------------------------------
    load_raw_from_gcs = GCSToBigQueryOperator(
        task_id="load_raw_from_gcs",
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_TABLE}",
        source_format="CSV",
        field_delimiter=",",
        # Skip header row ("price,area,bedrooms,...") so it is not loaded as data
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",         # Overwrite table if it exists
        create_disposition="CREATE_IF_NEEDED",      # Create table if it does not exist
        gcp_conn_id=GCP_CONN_ID,
        autodetect=False,                           # We define schema below    
        schema_fields=[
            {"name": "price",            "type": "INT64",  "mode": "NULLABLE"},
            {"name": "area",             "type": "INT64",  "mode": "NULLABLE"},
            {"name": "bedrooms",         "type": "INT64",  "mode": "NULLABLE"},
            {"name": "bathrooms",        "type": "INT64",  "mode": "NULLABLE"},
            {"name": "stories",          "type": "INT64",  "mode": "NULLABLE"},
            {"name": "mainroad",         "type": "STRING", "mode": "NULLABLE"},
            {"name": "guestroom",        "type": "STRING", "mode": "NULLABLE"},
            {"name": "basement",         "type": "STRING", "mode": "NULLABLE"},
            {"name": "hotwaterheating",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "airconditioning",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "parking",          "type": "INT64",  "mode": "NULLABLE"},
            {"name": "prefarea",         "type": "STRING", "mode": "NULLABLE"},
            {"name": "furnishingstatus", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # ------------------------------------------------------------------
    # 2) TRANSFORM RAW TABLE to CURATED TABLE
    # ------------------------------------------------------------------
    transform_to_curated = BigQueryInsertJobOperator(
        task_id="transform_raw_to_curated",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE
                      `{{ params.project_id }}.{{ params.dataset }}.{{ params.curated_table }}` AS
                    SELECT
                        price,
                        area,
                        bedrooms,
                        bathrooms,
                        stories,

                        -- Convert yes/no to BOOLEAN flags
                        (mainroad = 'yes')        AS mainroad_flag,
                        (guestroom = 'yes')       AS guestroom_flag,
                        (basement = 'yes')        AS basement_flag,
                        (hotwaterheating = 'yes') AS hotwaterheating_flag,
                        (airconditioning = 'yes') AS airconditioning_flag,
                        (prefarea = 'yes')        AS prefarea_flag,

                        parking,
                        furnishingstatus,

                        -- Derived metrics
                        SAFE_DIVIDE(price, NULLIF(area, 0)) AS price_per_sqft,
                        (bedrooms + bathrooms) AS total_rooms

                    FROM
                        `{{ params.project_id }}.{{ params.dataset }}.{{ params.raw_table }}`;
                """,
                "useLegacySql": False,
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": BQ_DATASET,
            "raw_table": RAW_TABLE,
            "curated_table": CURATED_TABLE,
        },
    )

    # DAG DEPENDENCY CHAIN
    load_raw_from_gcs >> transform_to_curated