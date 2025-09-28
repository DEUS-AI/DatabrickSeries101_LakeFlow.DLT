import dlt
from pyspark.sql import functions as F

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
