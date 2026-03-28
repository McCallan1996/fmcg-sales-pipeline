import os
import glob
from google.cloud import storage


BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
LOCAL_DIR = "/opt/data"
GCS_PREFIX = "raw/fmcg_sales"


def upload():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    csvs = glob.glob(os.path.join(LOCAL_DIR, "*.csv"))
    if not csvs:
        raise FileNotFoundError("Nothing to upload — no CSVs in /opt/data")

    for local_path in csvs:
        filename = os.path.basename(local_path)
        blob_path = f"{GCS_PREFIX}/{filename}"
        blob = bucket.blob(blob_path)

        print(f"Uploading {filename} -> gs://{BUCKET_NAME}/{blob_path}")
        blob.upload_from_filename(local_path)

    print("Done.")


if __name__ == "__main__":
    upload()
