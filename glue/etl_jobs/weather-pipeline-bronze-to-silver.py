import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, StringType
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "silver_bucket",
    "silver_database",
    "silver_table",
])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

BRONZE_DB = args["bronze_database"]
BRONZE_TABLE = args["bronze_table"]
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]
SILVER_PATH = f"s3://{SILVER_BUCKET}/weather/observations/"

logger.info(f"Bronze: {BRONZE_DB}.{BRONZE_TABLE}")
logger.info(f"SILVER: {SILVER_DB}.{SILVER_TABLE} -> {SILVER_PATH}")


# Reading from Bronze
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_source",
)
df = dyf.toDF()
initial = df.count()
logger.info(f"Bronze records read: {initial}")

if initial == 0:
    logger.info("No data in Bronze. Commiting empty job.")
    job.commit()
    sys.exit(0)

cols = set(df.columns)


# Unpacking Nested JSON from OpenWeather data

select_exprs = [
    F.col("name").cast(StringType()).alias("city"),
    F.col("id").cast(LongType()).alias("city_id"),
    F.col("coord.lat").cast(DoubleType()).alias("lat"),
    F.col("coord.lon").cast(DoubleType()).alias("lon"),
    F.col("main.temp").cast(DoubleType()).alias("temp"),
    F.col("main.feels_like").cast(DoubleType()).alias("feels_like"),
    F.col("main.temp_min").cast(DoubleType()).alias("temp_min"),
    F.col("main.temp_max").cast(DoubleType()).alias("temp_max"),
    F.col("main.pressure").cast(LongType()).alias("pressure"),
    F.col("main.humidity").cast(LongType()).alias("humidity"),
    F.col("wind.speed").cast(DoubleType()).alias("wind_speed"),
    F.col("wind.deg").cast(LongType()).alias("wind_deg"),
    F.col("clouds.all").cast(LongType()).alias("clouds_pct"),
    F.col("visibility").cast(LongType()).alias("visibility"),
    F.col("dt").cast(LongType()).alias("observed_unix"),
    F.col("sys.country").cast(StringType()).alias("country"),
    F.col("weather")[0]["main"].cast(StringType()).alias("weather_main"),
    F.col("weather")[0]["description"].cast(StringType()).alias("weather_desc"),
]

df = df.select(*select_exprs)

# Transformation

df = df.filter(F.col("city").isNotNull() & F.col("observed_unix").isNotNull())
df = df.withColumn("city", F.lower(F.trim(F.col("city"))))
df = df.withColumn("observed_at", F.to_timestamp(F.from_unixtime(F.col("observed_unix"))))
df = df.withColumn("observation_date", F.to_date(F.col("observed_at")))

for c in ["wind_speed", "clouds_pct", "humidity", "pressure"]:
    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

df = df.withColumn("temp_range", F.round(F.col("temp_max") - F.col("temp_min"), 2))
df = df.withColumn("_processed_at", F.current_timestamp())
df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))

w = Window.partitionBy("city", "observed_unix").orderBy(F.col("_processed_at").desc())
df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

clean = df.count()
logger.info(f"After cleansing & dedup: {clean}")


# Writing to Silver

dynf = DynamicFrame.fromDF(df, glueContext, "silver_weather")

sink = glueContext.getSink(
    connection_type="s3",
    path=SILVER_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["country"],
)
sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_TABLE)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(dynf)

logger.info(f"Silver write complete: {clean} rows -> {SILVER_PATH}")
job.commit()

