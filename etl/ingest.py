from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from delta.tables import DeltaTable
import sys

spark = SparkSession.builder.appName("FinanceIngestion").config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0").getOrCreate()

# Simulate stream from parquet (in prod: Kafka)
df = spark.read.parquet("raw_transactions.parquet")
df = df.withColumn("timestamp", to_timestamp(col("timestamp"))) \
       .withWatermark("timestamp", "1 hour")  # Handle late data

# Dedup and basic clean
df_clean = df.dropDuplicates(["timestamp", "amount"]) \
             .filter(col("amount") > 0)
# Write to Delta (lakehouse)

df_clean.write.format("delta").mode("overwrite").save("s3a://finance-bucket/raw_delta")
delta_table = DeltaTable.forPath(spark, "s3a://finance-bucket/raw_delta")
delta_table.optimize().executeVacuum()  # Clean old versions
spark.stop()

# Run with: 
# docker run -v $(pwd):/opt/spark/work-dir -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin local-spark uv run etl/ingest.py
