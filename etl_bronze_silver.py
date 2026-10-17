import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from delta import configure_spark_with_delta_pip

# 1. Initialize Delta Spark Session
builder = (
    SparkSession.builder.appName("SupplyChain-Bronze-Silver")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "8")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Paths
BASE_DIR = Path("/mnt/storage/projects/supply-chain-lakehouse")
RAW_CSV = BASE_DIR / "data/raw/DataCoSupplyChainDataset.csv"
BRONZE_PATH = BASE_DIR / "data/delta/bronze_orders"
SILVER_PATH = BASE_DIR / "data/delta/silver_orders"

print(">>> [1/3] Ingesting CSV to Bronze Delta Lake...")

# Ingest raw CSV as-is (strings only, preserving raw landing state)
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .load(str(RAW_CSV))
)

# Standardize raw column headers: replace spaces, slashes, and hyphens with underscores
sanitized_cols = [
    F.col(c).alias(
        c.strip()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .replace("/", "_")
    )
    for c in df_raw.columns
]
df_bronze = df_raw.select(*sanitized_cols).withColumn(
    "_ingestion_timestamp", F.current_timestamp()
)

# Write to Bronze Delta table
(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(BRONZE_PATH))
)

bronze_count = spark.read.format("delta").load(str(BRONZE_PATH)).count()
print(f"Bronze Delta table written: {bronze_count:,} records.")

print(">>> [2/3] Transforming Bronze to Silver Layer...")

# Read back from Bronze Delta table
df_bronze_source = spark.read.format("delta").load(str(BRONZE_PATH))

# Silver Transformations: Clean types and handle nulls
df_silver = (
    df_bronze_source.filter(F.col("Order_Item_Id").isNotNull())
    .dropDuplicates(["Order_Item_Id"])
    # Parsing dates (format: M/d/yyyy H:mm or MM/dd/yyyy HH:mm)
    .withColumn(
        "order_date",
        F.to_timestamp(F.col("order_date_DateOrders"), "M/d/yyyy H:mm"),
    )
    .withColumn(
        "shipping_date",
        F.to_timestamp(F.col("shipping_date_DateOrders"), "M/d/yyyy H:mm"),
    )
    # Casting numeric fields
    .withColumn(
        "days_for_shipping_real",
        F.col("Days_for_shipping_real").cast(IntegerType()),
    )
    .withColumn(
        "days_for_shipment_scheduled",
        F.col("Days_for_shipment_scheduled").cast(IntegerType()),
    )
    .withColumn(
        "order_item_quantity", F.col("Order_Item_Quantity").cast(IntegerType())
    )
    .withColumn(
        "order_item_total", F.col("Order_Item_Total").cast(DoubleType())
    )
    .withColumn("order_item_profit", F.col("Order_Profit_Per_Order").cast(DoubleType()))
    .withColumn("sales_per_customer", F.col("Sales_per_customer").cast(DoubleType()))
    .withColumn(
        "benefit_per_order", F.col("Benefit_per_order").cast(DoubleType())
    )
    # Fulfillment & OTIF KPI Logic
    .withColumn(
        "days_delayed",
        F.col("days_for_shipping_real") - F.col("days_for_shipment_scheduled"),
    )
    .withColumn(
        "is_late",
        F.when(
            (F.col("days_delayed") > 0)
            | (F.col("Delivery_Status") == "Late delivery"),
            1,
        ).otherwise(0),
    )
    .withColumn(
        "is_in_full",
        # In this operational data, canceled or suspected fraud orders are unfulfilled
        F.when(
            F.col("Order_Status").isin("CANCELED", "SUSPECTED_FRAUD"), 0
        ).otherwise(1),
    )
    .withColumn(
        "is_otif",
        F.when((F.col("is_late") == 0) & (F.col("is_in_full") == 1), 1).otherwise(
            0
        ),
    )
    # Time-partitioning columns for analytical query performance
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
)

# Write to Silver Delta table, partitioned by Year and Month
print(">>> [3/3] Writing Silver Delta Layer (partitioned by year/month)...")
(
    df_silver.write.format("delta")
    .mode("overwrite")
    .partitionBy("order_year", "order_month")
    .option("overwriteSchema", "true")
    .save(str(SILVER_PATH))
)

# Verification
df_silver_res = spark.read.format("delta").load(str(SILVER_PATH))
silver_count = df_silver_res.count()

print("\n--- PHASE 2 COMPLETED SUCCESSFULLY ---")
print(f"Silver Records: {silver_count:,}")
print("\nKPI Snapshot:")
df_silver_res.select(
    F.avg("is_late").alias("Late_Delivery_Rate"),
    F.avg("is_in_full").alias("In_Full_Rate"),
    F.avg("is_otif").alias("OTIF_Rate"),
    F.avg("days_delayed").alias("Avg_Days_Delayed"),
).show()

spark.stop()