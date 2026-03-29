import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS",
                              "/opt/spark/credentials/google_credentials.json")

RAW_TABLE = f"{PROJECT_ID}.fmcg_raw.daily_sales"
OUTPUT_TABLE = f"{PROJECT_ID}.fmcg_processed.daily_sales_enriched"


def main():
    spark = (
        SparkSession.builder
        .appName("fmcg-sales-transform")
        .config("temporaryGcsBucket", BUCKET_NAME)
        .config("credentialsFile", CREDENTIALS)
        .getOrCreate()
    )

    df = (
        spark.read
        .format("bigquery")
        .option("table", RAW_TABLE)
        .load()
    )

    print(f"Read {df.count()} rows from {RAW_TABLE}")
    df.printSchema()

    # actual columns: date, sku, brand, segment, category, channel, region,
    # pack_type, price_unit, promotion_flag, delivery_days, stock_available,
    # delivered_qty, units_sold

    enriched = df.select(
        F.col("date").cast("date").alias("sale_date"),
        *[F.col(c) for c in df.columns if c != "date"],
    )

    # revenue = units_sold * price_unit
    enriched = enriched.withColumn(
        "revenue",
        F.col("units_sold").cast("double") * F.col("price_unit").cast("double")
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

    # cast promo flag to boolean
    enriched = enriched.withColumn(
        "promotion_flag",
        F.col("promotion_flag").cast("boolean")
    )

    print(f"Writing {enriched.count()} enriched rows...")
    enriched.printSchema()

    # write directly to bigquery
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
