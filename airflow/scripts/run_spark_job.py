import subprocess
import sys


JOB_PATH = "/opt/spark/jobs/transform_sales.py"

# use the shaded "with-dependencies" jar to avoid guava conflicts
BQ_CONNECTOR = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"


def submit():
    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--packages", BQ_CONNECTOR,
        "--conf", "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        JOB_PATH,
    ]

    print(f"Running spark job: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        sys.exit(result.returncode)

    print("Spark job finished.")


if __name__ == "__main__":
    submit()
