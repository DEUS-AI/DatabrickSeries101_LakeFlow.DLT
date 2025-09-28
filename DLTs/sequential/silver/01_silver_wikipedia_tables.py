import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_top_editors_seq",
    comment="Top editors by edit count - Silver Layer"
)
@dlt.expect_or_drop("valid_total_edits", "total_edits > 0")
@dlt.expect_or_drop("valid_editor", "editor IS NOT NULL")
@dlt.expect_or_drop("valid_wiki_language", "wiki_language IS NOT NULL")
@dlt.expect_or_drop("valid_last_edit_time", "last_edit_time IS NOT NULL")
def wikipedia_top_editors():
    return (
        dlt.read("wikipedia_edits_cleaned_seq")
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
    name="wikipedia_top_pages_seq",
    comment="Most edited pages - Silver Layer"
)
@dlt.expect_or_drop("valid_edit_count", "edit_count > 0")
def wikipedia_top_pages():
    return (
        dlt.read("wikipedia_edits_cleaned_seq")
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
