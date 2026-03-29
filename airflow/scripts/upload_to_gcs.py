import os
from google.cloud import storage


BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
LOCAL_DIR = "/opt/data"
GCS_PREFIX = "raw/fmcg_sales"
TARGET_FILE = "FMCG_2022_2024.csv"


def upload():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    local_path = os.path.join(LOCAL_DIR, TARGET_FILE)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"{TARGET_FILE} not found in {LOCAL_DIR}")

    blob_path = f"{GCS_PREFIX}/{TARGET_FILE}"
    blob = bucket.blob(blob_path)

    print(f"Uploading {TARGET_FILE} -> gs://{BUCKET_NAME}/{blob_path}")
    blob.upload_from_filename(local_path)

    print("Done.")


if __name__ == "__main__":
    upload()
