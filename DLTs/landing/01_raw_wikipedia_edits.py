import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Define the schema for Wikipedia edit events
wikipedia_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", StructType([
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True)
    ]), True),
    StructField("revision", StructType([
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True)
    ]), True),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True)
    ]), True)
])

@dlt.table(
    name="raw_wikipedia_edits",
    comment="Raw Wikipedia edit events from Azure Blob Storage - Landing Layer"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
def raw_wikipedia_edits():
    spark.conf.set(
        "fs.azure.account.key.unitycatalogstore.dfs.core.windows.net",
        "SECRET_REMOVED=="
    )
    return (
        spark.readStream
        .format("json")
        .schema(wikipedia_schema)  # Use explicit schema to avoid empty directory issues
        .option("multiline", "false")  # One JSON object per line
        .option("pathGlobFilter", "wiki_edits_*.json")  # Only read Wikipedia files
        .load("/Volumes/main/landing/wiki_files/landing/raw_files/")
    )