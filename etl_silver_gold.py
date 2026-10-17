from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from delta import configure_spark_with_delta_pip

# 1. Initialize Delta Spark Session
builder = (
    SparkSession.builder.appName("SupplyChain-Silver-Gold")
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

# Base directories
BASE_DIR = Path("/mnt/storage/projects/supply-chain-lakehouse")
SILVER_PATH = BASE_DIR / "data/delta/silver_orders"
GOLD_DIR = BASE_DIR / "data/delta/gold"

print(">>> [1/5] Loading Silver Delta table...")
df_silver = spark.read.format("delta").load(str(SILVER_PATH))

# ---------------------------------------------------------
# Dimension 1: Dim_Product
# ---------------------------------------------------------
print(">>> [2/5] Building Dimensions: Dim_Product, Dim_Customer, Dim_Shipping...")

dim_product = (
    df_silver.select(
        F.col("Product_Card_Id").cast(IntegerType()).alias("product_id"),
        F.col("Product_Name").alias("product_name"),
        F.col("Category_Id").cast(IntegerType()).alias("category_id"),
        F.col("Category_Name").alias("category_name"),
        F.col("Department_Id").cast(IntegerType()).alias("department_id"),
        F.col("Department_Name").alias("department_name"),
        F.col("Product_Price").cast("double").alias("product_price"),
    )
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

(
    dim_product.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(GOLD_DIR / "dim_product"))
)

# ---------------------------------------------------------
# Dimension 2: Dim_Customer
# ---------------------------------------------------------
dim_customer = (
    df_silver.select(
        F.col("Customer_Id").cast(IntegerType()).alias("customer_id"),
        F.col("Customer_City").alias("city"),
        F.col("Customer_State").alias("state"),
        F.col("Customer_Country").alias("country"),
        F.col("Market").alias("market"),
        F.col("Order_Region").alias("order_region"),
        F.col("Customer_Segment").alias("segment"),
    )
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

(
    dim_customer.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(GOLD_DIR / "dim_customer"))
)

# ---------------------------------------------------------
# Dimension 3: Dim_Shipping
# ---------------------------------------------------------
dim_shipping = (
    df_silver.select(
        F.col("Shipping_Mode").alias("shipping_mode"),
        F.col("Delivery_Status").alias("delivery_status"),
        F.col("Type").alias("payment_type"),
    )
    .dropDuplicates()
    .withColumn("shipping_key", F.monotonically_increasing_id().cast(IntegerType()))
)

(
    dim_shipping.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(GOLD_DIR / "dim_shipping"))
)

# ---------------------------------------------------------
# Dimension 4: Dim_Date (Continuous Calendar)
# ---------------------------------------------------------
print(">>> [3/5] Generating Dim_Date (Continuous Calendar)...")

date_range = df_silver.select(
    F.min(F.to_date("order_date")).alias("min_date"),
    F.max(F.to_date("shipping_date")).alias("max_date"),
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

# Spark sequence to generate daily calendar records
dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) as calendar_date")
    .withColumn("date_key", F.date_format(F.col("calendar_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year(F.col("calendar_date")))
    .withColumn("quarter", F.quarter(F.col("calendar_date")))
    .withColumn("month", F.month(F.col("calendar_date")))
    .withColumn("month_name", F.date_format(F.col("calendar_date"), "MMMM"))
    .withColumn("week_of_year", F.weekofyear(F.col("calendar_date")))
    .withColumn("day_of_week", F.dayofweek(F.col("calendar_date")))
    .withColumn("day_name", F.date_format(F.col("calendar_date"), "EEEE"))
)

(
    dim_date.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(GOLD_DIR / "dim_date"))
)

# ---------------------------------------------------------
# Fact Table: Fact_Fulfillment
# ---------------------------------------------------------
print(">>> [4/5] Building Fact_Fulfillment...")

# Join with Dim_Shipping to link surrogate shipping_key
dim_shipping_loaded = spark.read.format("delta").load(str(GOLD_DIR / "dim_shipping"))

fact_fulfillment = (
    df_silver.join(
        dim_shipping_loaded,
        (df_silver["Shipping_Mode"] == dim_shipping_loaded["shipping_mode"])
        & (df_silver["Delivery_Status"] == dim_shipping_loaded["delivery_status"])
        & (df_silver["Type"] == dim_shipping_loaded["payment_type"]),
        how="left",
    )
    .select(
        F.col("Order_Item_Id").cast(IntegerType()).alias("order_item_id"),
        F.col("Order_Id").cast(IntegerType()).alias("order_id"),
        # Foreign Keys
        F.date_format(F.to_date("order_date"), "yyyyMMdd").cast(IntegerType()).alias("order_date_key"),
        F.date_format(F.to_date("shipping_date"), "yyyyMMdd").cast(IntegerType()).alias("shipping_date_key"),
        F.col("Customer_Id").cast(IntegerType()).alias("customer_id"),
        F.col("Product_Card_Id").cast(IntegerType()).alias("product_id"),
        F.col("shipping_key"),
        # Quantitative Metrics & Lead Times
        F.col("days_for_shipping_real").alias("actual_shipping_days"),
        F.col("days_for_shipment_scheduled").alias("scheduled_shipping_days"),
        F.col("days_delayed"),
        F.col("order_item_quantity").alias("order_quantity"),
        F.col("order_item_total").alias("sales_amount"),
        F.col("order_item_profit").alias("profit_amount"),
        # Operational KPI Flags
        F.col("is_late"),
        F.col("is_in_full"),
        F.col("is_otif"),
    )
)

print(">>> [5/5] Writing Fact_Fulfillment Delta table...")
(
    fact_fulfillment.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(GOLD_DIR / "fact_fulfillment"))
)

# ---------------------------------------------------------
# Verification
# ---------------------------------------------------------
print("\n--- PHASE 3 GOLD STAR SCHEMA SUMMARY ---")
tables = ["dim_product", "dim_customer", "dim_shipping", "dim_date", "fact_fulfillment"]
for t in tables:
    count = spark.read.format("delta").load(str(GOLD_DIR / t)).count()
    print(f"Table [{t}]: {count:,} records")

spark.stop()