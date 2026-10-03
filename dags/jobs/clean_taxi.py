"""PySpark job: clean NYC Yellow Taxi data and write partitioned Parquet to GCS.

Usage: clean_taxi.py <bucket> <months comma separated>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

bucket = sys.argv[1]
months = sys.argv[2].split(",")

spark = SparkSession.builder.appName("clean_nyc_taxi").getOrCreate()

# Read each month separately and cast to one schema, because column types
# (int vs double) drift between monthly files.
COLS = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_ts", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_ts", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("rate_code_id", "int"),
    "PULocationID": ("pu_location_id", "int"),
    "DOLocationID": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "total_amount": ("total_amount", "double"),
}

frames = []
for m in months:
    df = spark.read.parquet(f"gs://{bucket}/raw/yellow_taxi/{m}/yellow_tripdata_{m}.parquet")
    df = df.select([F.col(src).cast(t).alias(dst) for src, (dst, t) in COLS.items()])
    # keep only rows whose pickup falls inside the file's own month
    df = df.filter(F.date_format("pickup_ts", "yyyy-MM") == m)
    frames.append(df)

raw = frames[0]
for f in frames[1:]:
    raw = raw.unionByName(f)
raw_count = raw.count()

clean = (
    raw.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("dropoff_ts") > F.col("pickup_ts"))
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") < 200)
        & (F.col("fare_amount") >= 3.0)
        & (F.col("total_amount") > 0)
        & (F.col("passenger_count").isNull() | F.col("passenger_count").between(1, 6))
    )
    .dropDuplicates(
        ["vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount"]
    )
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    .withColumn(
        "trip_duration_min",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
    )
    .filter(F.col("trip_duration_min") < 24 * 60)
)

clean.write.mode("overwrite").partitionBy("pickup_month").parquet(
    f"gs://{bucket}/clean/yellow_taxi/"
)
clean_count = spark.read.parquet(f"gs://{bucket}/clean/yellow_taxi/").count()

print(f"RAW_ROWS={raw_count} CLEAN_ROWS={clean_count} DROPPED={raw_count - clean_count}")
spark.stop()