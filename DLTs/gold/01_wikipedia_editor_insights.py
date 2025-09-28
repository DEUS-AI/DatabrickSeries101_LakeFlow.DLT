import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_editor_insights",
    comment="Editor behavior insights and patterns - Gold Layer"
)
def wikipedia_editor_insights():
    return (
        dlt.read("silver.wikipedia_edits_cleaned")
        .groupBy("editor", "wiki_language")
        .agg(
            F.count("*").alias("total_edits"),
            F.countDistinct("page_title").alias("pages_edited"),
            F.avg("length_change").alias("avg_length_change"),
            F.stddev("length_change").alias("length_change_stddev"),
            F.max("event_timestamp").alias("last_edit_time"),
            F.min("event_timestamp").alias("first_edit_time"),
            F.sum(F.when(F.col("is_bot_edit"), 1).otherwise(0)).alias("bot_edits"),
            F.sum(F.when(F.col("is_minor_edit"), 1).otherwise(0)).alias("minor_edits")
        )
        .filter(F.col("editor").isNotNull())
        .withColumn("bot_edit_ratio", F.col("bot_edits") / F.col("total_edits"))
        .withColumn("minor_edit_ratio", F.col("minor_edits") / F.col("total_edits"))
        .withColumn("edit_frequency", F.col("total_edits") / F.col("pages_edited"))
        .withColumn("activity_span_days", 
                   F.datediff(F.col("last_edit_time"), F.col("first_edit_time")))
        .withColumn("edits_per_day", 
                   F.when(F.col("activity_span_days") > 0, 
                         F.col("total_edits") / F.col("activity_span_days"))
                   .otherwise(F.col("total_edits")))
        .withColumn("editor_type", 
                   F.when(F.col("bot_edit_ratio") > 0.8, "Bot")
                   .when(F.col("bot_edit_ratio") > 0.2, "Mixed")
                   .otherwise("Human"))
    )
