import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_editor_insights_seq",
    comment="Editor behavior insights and patterns - Gold Layer"
)
@dlt.expect("non_negative_activity_span", "activity_span_days >= 0")
def wikipedia_editor_insights():
    return (
        dlt.read("wikipedia_top_editors_seq")
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
    name="wikipedia_page_trends_seq",
    comment="Page edit trends and patterns - Gold Layer"
)
@dlt.expect("positive_unique_editors", "unique_editors > 0")
def wikipedia_page_trends():
    return (
        dlt.read("wikipedia_top_pages_seq")
        .withColumn("edit_intensity", 
                   F.when(F.col("edit_count") > 1000, "High")
                   .when(F.col("edit_count") > 100, "Medium")
                   .otherwise("Low"))
        .withColumn("editor_diversity", 
                   F.when(F.col("unique_editors") > 50, "High")
                   .when(F.col("unique_editors") > 10, "Medium")
                   .otherwise("Low"))
    )
