from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable
import os
import tempfile  # For local testing

# Print env for debugging
endpoint = os.environ.get("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
print(f"s3 endpoint: {endpoint}")

def ingest_transactions(spark, input_path, output_path=None):
    """Core ingestion logic: read, transform, return cleaned DF. Write if output_path provided."""
    df = spark.read.parquet(input_path)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"))) \
           .withWatermark("timestamp", "1 hour")
    df_clean = df.dropDuplicates(["timestamp", "amount"]) \
                 .filter(col("amount") > 0)
    if output_path:
        # For prod: use S3; for test: local
        if "s3a://" not in output_path:
            output_path = f"{tempfile.mkdtemp()}/{output_path.split('/')[-1]}"
        df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        delta_table = DeltaTable.forPath(spark, output_path)
        delta_table.optimize().executeCompaction()
        delta_table.vacuum()
        print("Delta write and optimization successful.")
    return df_clean

# Main entrypoint remains similar, but calls the function
if __name__ == "__main__":
    endpoint = os.environ.get("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
    print(f"s3 endpoint: {endpoint}")
    spark = SparkSession.builder \
       .appName("FinanceIngestion") \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
       .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
       .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
       .config("spark.hadoop.fs.s3a.path.style.access", "true") \
       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
       .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
       .config("spark.hadoop.fs.s3a.region", "us-east-1") \
       .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
       .config("spark.hadoop.fs.s3a.connection.maximum.connections", "100") \
       .config("spark.hadoop.fs.s3a.proxy.host", "") \
       .config("spark.hadoop.fs.s3a.proxy.port", "-1") \
       .config("spark.hadoop.fs.s3a.user.agent.prefix", "") \
       .config("spark.hadoop.fs.s3a.retry.limit", "5") \
       .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
       .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
       .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "false") \
       .config("spark.sql.adaptive.enabled", "false") \
       .config("spark.hadoop.fs.s3a.metrics.collector", "None") \
       .config("spark.hadoop.fs.s3a.http.socket-timeout", "60000") \
       .config("spark.hadoop.fs.s3a.http.read-timeout", "60000") \
       .getOrCreate()
    try:
        input_path = "raw_transactions.parquet"
        output_path = "s3a://finance-bucket/raw_delta"
        ingest_transactions(spark, input_path, output_path)
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        spark.stop()
