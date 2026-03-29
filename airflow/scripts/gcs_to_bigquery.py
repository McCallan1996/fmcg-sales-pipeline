import os
from google.cloud import bigquery


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
DATASET = "fmcg_raw"
TABLE = "daily_sales"


def load():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{BUCKET_NAME}/raw/fmcg_sales/FMCG_2022_2024.csv"
    print(f"Loading {uri} into {table_ref}")

    job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()  # wait

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_ref}")


if __name__ == "__main__":
    load()
