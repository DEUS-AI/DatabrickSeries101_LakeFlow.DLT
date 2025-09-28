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

# =============================================================================
# LANDING LAYER - Raw Data Ingestion
# =============================================================================

@dlt.table(
    name="raw_wikipedia_edits",
    comment="Raw Wikipedia edit events from Azure Blob Storage - Landing Layer"
)
def raw_wikipedia_edits():
    return (
        spark.readStream
        .format("json")
        .schema(wikipedia_schema)
        .option("multiline", "false")
        .option("pathGlobFilter", "wiki_edits_*.json")
        .load("/Volumes/main/wikipedia_data/landing/raw_files/")
    )

# =============================================================================
# BRONZE LAYER - Data Cleaning and Basic Transformations
# =============================================================================

@dlt.table(
    name="wikipedia_edits_cleaned",
    comment="Cleaned and structured Wikipedia edit events - Bronze Layer"
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
    comment="Summary statistics of Wikipedia edits - Bronze Layer"
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

# =============================================================================
# SILVER LAYER - Business Logic and Aggregations
# =============================================================================

@dlt.table(
    name="wikipedia_top_editors",
    comment="Top editors by edit count - Silver Layer"
)
def wikipedia_top_editors():
    return (
        dlt.read("wikipedia_edits_cleaned")
        .groupBy("editor", "wiki_language")
        .agg(
            F.count("*").alias("total_edits"),
            F.countDistinct("page_title").alias("pages_edited"),
            F.avg("length_change").alias("avg_length_change"),
            F.max("event_timestamp").alias("last_edit_time"),
            F.min("event_timestamp").alias("first_edit_time")
        )
        .filter(F.col("editor").isNotNull())
        .withColumn("edit_frequency", F.col("total_edits") / F.col("pages_edited"))
        .orderBy(F.desc("total_edits"))
        .limit(100)
    )

@dlt.table(
    name="wikipedia_top_pages",
    comment="Most edited pages - Silver Layer"
)
def wikipedia_top_pages():
    return (
        dlt.read("wikipedia_edits_cleaned")
        .groupBy("page_title", "wiki_language")
        .agg(
            F.count("*").alias("edit_count"),
            F.countDistinct("editor").alias("unique_editors"),
            F.avg("length_change").alias("avg_length_change"),
            F.max("event_timestamp").alias("last_edit_time"),
            F.min("event_timestamp").alias("first_edit_time"),
            F.sum(F.when(F.col("is_bot_edit"), 1).otherwise(0)).alias("bot_edits"),
            F.sum(F.when(F.col("is_minor_edit"), 1).otherwise(0)).alias("minor_edits")
        )
        .filter(F.col("page_title").isNotNull())
        .withColumn("bot_edit_ratio", F.col("bot_edits") / F.col("edit_count"))
        .withColumn("minor_edit_ratio", F.col("minor_edits") / F.col("edit_count"))
        .withColumn("edits_per_editor", F.col("edit_count") / F.col("unique_editors"))
        .orderBy(F.desc("edit_count"))
        .limit(100)
    )

# =============================================================================
# GOLD LAYER - Analytics and Insights
# =============================================================================

@dlt.table(
    name="wikipedia_editor_insights",
    comment="Editor behavior insights and patterns - Gold Layer"
)
def wikipedia_editor_insights():
    return (
        dlt.read("wikipedia_top_editors")
        .withColumn("activity_span_days", 
                   F.datediff(F.col("last_edit_time"), F.col("first_edit_time")))
        .withColumn("edits_per_day", 
                   F.when(F.col("activity_span_days") > 0, 
                         F.col("total_edits") / F.col("activity_span_days"))
                   .otherwise(F.col("total_edits")))
        .withColumn("editor_type", 
                   F.when(F.col("avg_length_change") > 100, "Heavy Editor")
                   .when(F.col("avg_length_change") > 0, "Regular Editor")
                   .otherwise("Minor Editor"))
    )

@dlt.table(
    name="wikipedia_page_trends",
    comment="Page edit trends and patterns - Gold Layer"
)
def wikipedia_page_trends():
    return (
        dlt.read("wikipedia_top_pages")
        .withColumn("edit_intensity", 
                   F.when(F.col("edit_count") > 1000, "High")
                   .when(F.col("edit_count") > 100, "Medium")
                   .otherwise("Low"))
        .withColumn("editor_diversity", 
                   F.when(F.col("unique_editors") > 50, "High")
                   .when(F.col("unique_editors") > 10, "Medium")
                   .otherwise("Low"))
    )
