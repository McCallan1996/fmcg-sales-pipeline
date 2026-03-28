import os
import glob
from kaggle.api.kaggle_api_extended import KaggleApi


DATASET = "beatafaron/fmcg-daily-sales-data-to-2022-2024"
OUTPUT_DIR = "/opt/data"


def download():
    api = KaggleApi()
    api.authenticate()

    print(f"Downloading {DATASET}...")
    api.dataset_download_files(DATASET, path=OUTPUT_DIR, unzip=True)

    # check we actually got something
    csvs = glob.glob(os.path.join(OUTPUT_DIR, "*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found in {OUTPUT_DIR} after download")

    for f in csvs:
        size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"  {os.path.basename(f)}: {size_mb:.1f} MB")

    return csvs


if __name__ == "__main__":
    download()
