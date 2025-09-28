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
    comment="Raw Wikipedia edit events from Azure Blob Storage"
)
def raw_wikipedia_edits():
    return (
        spark.readStream
        .format("json")
        .schema(wikipedia_schema)  # Use explicit schema to avoid empty directory issues
        .option("multiline", "false")  # One JSON object per line
        .option("pathGlobFilter", "wiki_edits_*.json")  # Only read Wikipedia files
        .load("/Volumes/main/wikipedia_data/landing/raw_files/")
    )

@dlt.table(
    name="wikipedia_edits_cleaned",
    comment="Cleaned and structured Wikipedia edit events"
)
def wikipedia_edits_cleaned():
    return (
        dlt.read("raw_wikipedia_edits")
        .withColumn("event_timestamp", F.from_unixtime(F.col("timestamp")/1000))
        .withColumn("is_bot_edit", F.col("bot"))
        .withColumn("is_minor_edit", F.col("minor"))
        .withColumn("page_title", F.col("title"))
        .withColumn("editor", F.col("user"))
        .withColumn("edit_comment", F.col("comment"))
        .withColumn("old_length", F.col("length.old"))
        .withColumn("new_length", F.col("length.new"))
        .withColumn("length_change", F.col("length.new") - F.col("length.old"))
        .withColumn("old_revision", F.col("revision.old"))
        .withColumn("new_revision", F.col("revision.new"))
        .withColumn("wiki_domain", F.col("server_name"))
        .withColumn("wiki_language", F.col("wiki"))
        .withColumn("parsed_comment", F.col("parsedcomment"))
        .withColumn("meta_uri", F.col("meta.uri"))
        .withColumn("meta_request_id", F.col("meta.request_id"))
        .withColumn("meta_domain", F.col("meta.domain"))
        .withColumn("meta_stream", F.col("meta.stream"))
        .withColumn("meta_topic", F.col("meta.topic"))
        .withColumn("meta_partition", F.col("meta.partition"))
        .withColumn("meta_offset", F.col("meta.offset"))
        .select(
            "id", "type", "namespace", "page_title", "editor", 
            "edit_comment", "event_timestamp", "is_bot_edit", 
            "is_minor_edit", "old_length", "new_length", "length_change",
            "old_revision", "new_revision", "wiki_domain", "wiki_language",
            "parsed_comment", "meta_uri", "meta_request_id", "meta_domain",
            "meta_stream", "meta_topic", "meta_partition", "meta_offset"
        )
    )

@dlt.table(
    name="wikipedia_edits_summary",
    comment="Summary statistics of Wikipedia edits"
)
def wikipedia_edits_summary():
    return (
        dlt.read("wikipedia_edits_cleaned")
        .groupBy(
            F.date_trunc("hour", F.col("event_timestamp")).alias("hour"),
            F.col("wiki_language"),
            F.col("is_bot_edit")
        )
        .agg(
            F.count("*").alias("edit_count"),
            F.avg("length_change").alias("avg_length_change"),
            F.countDistinct("editor").alias("unique_editors"),
            F.countDistinct("page_title").alias("unique_pages")
        )
        .orderBy("hour", "wiki_language", "is_bot_edit")
    )

@dlt.table(
    name="wikipedia_top_editors",
    comment="Top editors by edit count"
)
def wikipedia_top_editors():
    return (
        dlt.read("wikipedia_edits_cleaned")
        .groupBy("editor", "wiki_language")
        .agg(
            F.count("*").alias("total_edits"),
            F.countDistinct("page_title").alias("pages_edited"),
            F.avg("length_change").alias("avg_length_change"),
            F.max("event_timestamp").alias("last_edit_time")
        )
        .filter(F.col("editor").isNotNull())
        .orderBy(F.desc("total_edits"))
        .limit(100)
    )

@dlt.table(
    name="wikipedia_top_pages",
    comment="Most edited pages"
)
def wikipedia_top_pages():
    return (
        dlt.read("wikipedia_edits_cleaned")
        .groupBy("page_title", "wiki_language")
        .agg(
            F.count("*").alias("edit_count"),
            F.countDistinct("editor").alias("unique_editors"),
            F.avg("length_change").alias("avg_length_change"),
            F.max("event_timestamp").alias("last_edit_time")
        )
        .filter(F.col("page_title").isNotNull())
        .orderBy(F.desc("edit_count"))
        .limit(100)
    )