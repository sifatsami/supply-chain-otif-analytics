from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("Export-Gold-Parquet")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

BASE_DIR = Path("/mnt/storage/projects/supply-chain-lakehouse")
GOLD_DIR = BASE_DIR / "data/delta/gold"
EXPORT_DIR = BASE_DIR / "data/export_powerbi"

tables = ["dim_product", "dim_customer", "dim_shipping", "dim_date", "fact_fulfillment"]

print(">>> Exporting Gold tables to single Parquet files for Power BI...")

for table in tables:
    df = spark.read.format("delta").load(str(GOLD_DIR / table))
    out_path = EXPORT_DIR / table
    # coalesce(1) writes a single clean parquet file per table for easy Power BI import
    df.coalesce(1).write.mode("overwrite").parquet(str(out_path))
    print(f"  [✓] Exported {table} ({df.count():,} rows)")

spark.stop()
print(f"\nAll files saved to: {EXPORT_DIR}")