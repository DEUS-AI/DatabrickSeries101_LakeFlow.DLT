import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_top_editors",
    comment="Top editors by edit count - Silver Layer"
)
@dlt.expect_or_drop("valid_total_edits", "total_edits > 0")
@dlt.expect_or_drop("valid_editor", "editor IS NOT NULL")
@dlt.expect_or_drop("valid_wiki_language", "wiki_language IS NOT NULL")
@dlt.expect_or_drop("valid_last_edit_time", "last_edit_time IS NOT NULL")
def wikipedia_top_editors():
    return (
        dlt.read("bronze.wikipedia_edits_cleaned")
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
