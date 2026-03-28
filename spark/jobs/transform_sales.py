import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS",
                              "/opt/spark/credentials/google_credentials.json")

RAW_TABLE = f"{PROJECT_ID}.fmcg_raw.daily_sales"
OUTPUT_PATH = f"gs://{BUCKET_NAME}/processed/daily_sales_enriched"
OUTPUT_TABLE = f"{PROJECT_ID}.fmcg_processed.daily_sales_enriched"


def main():
    spark = (
        SparkSession.builder
        .appName("fmcg-sales-transform")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-3.5-bigquery:0.39.1")
        .config("temporaryGcsBucket", BUCKET_NAME)
        .config("credentialsFile", CREDENTIALS)
        .getOrCreate()
    )

    # read raw table from bigquery
    df = (
        spark.read
        .format("bigquery")
        .option("table", RAW_TABLE)
        .load()
    )

    print(f"Read {df.count()} rows from {RAW_TABLE}")
    df.printSchema()

    # figure out the date column name — dataset might use "Date" or "date"
    date_col = None
    for col_name in df.columns:
        if col_name.lower() == "date":
            date_col = col_name
            break

    if date_col is None:
        raise ValueError(f"Can't find a date column. Columns: {df.columns}")

    # similarly for quantity and price
    qty_col = next((c for c in df.columns if "quantit" in c.lower()), None)
    price_col = next((c for c in df.columns if "price" in c.lower()), None)
    promo_col = next((c for c in df.columns if "promo" in c.lower()), None)

    # rename to consistent names + add computed fields
    enriched = df.select(
        F.col(date_col).cast("date").alias("sale_date"),
        *[F.col(c) for c in df.columns if c != date_col],
    )

    # revenue
    if qty_col and price_col:
        enriched = enriched.withColumn(
            "revenue",
            F.col(qty_col).cast("double") * F.col(price_col).cast("double")
        )

    # date parts
    enriched = (
        enriched
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .withColumn("day_of_week", F.dayofweek("sale_date"))
        .withColumn("week_of_year", F.weekofyear("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
        .withColumn("is_weekend", F.dayofweek("sale_date").isin([1, 7]))
    )

    # cast promo to boolean if it exists and looks like 0/1
    if promo_col:
        enriched = enriched.withColumn(
            promo_col,
            F.col(promo_col).cast("boolean")
        )

    print(f"Writing {enriched.count()} enriched rows...")
    enriched.printSchema()

    # write to GCS as parquet (partitioned by year/month for efficient reads)
    (
        enriched.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUTPUT_PATH)
    )

    # also write to bigquery processed dataset
    (
        enriched.write
        .format("bigquery")
        .option("table", OUTPUT_TABLE)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )

    print("All done.")
    spark.stop()


if __name__ == "__main__":
    main()
