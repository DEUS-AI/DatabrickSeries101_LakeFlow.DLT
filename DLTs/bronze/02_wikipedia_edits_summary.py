import dlt
from pyspark.sql import functions as F

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
