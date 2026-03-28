import subprocess
import sys


SPARK_MASTER = "spark://spark-master:7077"
JOB_PATH = "/opt/spark/jobs/transform_sales.py"

# connector jars needed for spark <-> bigquery
BQ_CONNECTOR = "com.google.cloud.spark:spark-3.5-bigquery:0.39.1"
GCS_CONNECTOR = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21"


def submit():
    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--packages", f"{BQ_CONNECTOR},{GCS_CONNECTOR}",
        "--conf", "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        JOB_PATH,
    ]

    print(f"Submitting spark job: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        sys.exit(result.returncode)

    print("Spark job finished.")


if __name__ == "__main__":
    submit()
