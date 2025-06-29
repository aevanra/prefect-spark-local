from pyspark.sql import DataFrame, SparkSession, types
from .constants import S3A_DATA_DIR
from .schemas import schemas

def load_ratings_data(spark: SparkSession) -> DataFrame:
    """ Load data from data_dir into DataFrames """
    ratings = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .schema(schemas["ratings"])
        .load(f"{S3A_DATA_DIR}/u.data")
    )

    return ratings.withColumn(
        "timestamp", ratings["timestamp"].cast(types.TimestampType())
    )

def load_users_data(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("sep", "|")
        .option("header", "false")
        .schema(schemas["users"])
        .load(f"{S3A_DATA_DIR}/u.user")
    )

def load_movies_data(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("sep", "|")
        .option("header", "false")
        .schema(schemas["movies"])
        .load(f"{S3A_DATA_DIR}/u.item")
    )
