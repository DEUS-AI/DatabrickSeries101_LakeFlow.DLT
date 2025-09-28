import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_page_trends",
    comment="Page edit trends and patterns - Gold Layer"
)
def wikipedia_page_trends():
    return (
        dlt.read("silver.wikipedia_edits_cleaned")
        .groupBy("page_title", "wiki_language", 
                F.date_trunc("day", F.col("event_timestamp")).alias("edit_date"))
        .agg(
            F.count("*").alias("daily_edits"),
            F.countDistinct("editor").alias("daily_editors"),
            F.avg("length_change").alias("avg_length_change"),
            F.sum(F.when(F.col("is_bot_edit"), 1).otherwise(0)).alias("bot_edits"),
            F.sum(F.when(F.col("is_minor_edit"), 1).otherwise(0)).alias("minor_edits")
        )
        .withColumn("bot_edit_ratio", F.col("bot_edits") / F.col("daily_edits"))
        .withColumn("minor_edit_ratio", F.col("minor_edits") / F.col("daily_edits"))
        .withColumn("edits_per_editor", F.col("daily_edits") / F.col("daily_editors"))
        .orderBy("page_title", "wiki_language", "edit_date")
    )
