import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "silver_table",
    "gold_bucket",
    "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]


def write_gold(dframe, table_name, partition_key="country"):
    path = f"s3://{GOLD_BUCKET}/weather/{table_name}/"
    dynf = DynamicFrame.fromDF(dframe, glueContext, table_name)
    sink = glueContext.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[partition_key],
    )
    sink.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName=table_name)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynf)
    logger.info(f"Wrote {table_name} -> {path}")

dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB, table_name=SILVER_TABLE, transformation_ctx="silver_src",
)
df = dyf.toDF()
logger.info(F"Silver rows: {df.count()}")

city_daily = df.groupBy("country", "city", "observation_date").agg(
    F.round(F.avg("temp"), 2).alias("avg_temp"),
    F.min("temp").alias("min_temp"),
    F.max("temp").alias("max_temp"),
    F.round(F.avg("humidity"), 1).alias("avg_humidity"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
    F.round(F.avg("pressure"), 1).alias("avg_pressure"),
    F.count("*").alias("observations"),
)
city_daily = city_daily.withColumn("_aggregated_at", F.current_timestamp())
write_gold(city_daily, "city_daily_weather")


country_summary = df.groupBy("country", "city").agg(
    F.round(F.avg("temp"), 2).alias("avg_temp"),
    F.max("temp").alias("peak_temp"),
    F.min("temp").alias("lowest_temp"),
    F.round(F.avg("humidity"), 1).alias("avg_humidity"),
    F.count("*").alias("total_observations"),
)

w = Window.partitionBy("country").orderBy(F.col("avg_temp").desc())
country_summary = country_summary.withColumn("warmth_rank", F.row_number().over(w))
country_summary = country_summary.withColumn("_aggregated_at", F.current_timestamp())
write_gold(country_summary, "country_weather_summary")

conditions = df.groupBy("country", "weather_main").agg(
    F.count("*").alias("occurrences"),
    F.round(F.avg("temp"), 2).alias("avg_temp_in_condition"),
)

wc = Window.partitionBy("country")
conditions = conditions.withColumn(
    "share_pct",
    F.round(F.col("occurrences") / F.sum("occurrences").over(wc) * 100, 2),
)
conditions = conditions.withColumn("_aggregated_at", F.current_timestamp())
write_gold(conditions, "weather_conditions")

logger.info("Gold build complete.")
job.commit()