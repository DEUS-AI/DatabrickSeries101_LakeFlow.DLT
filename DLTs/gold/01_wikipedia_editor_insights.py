import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_editor_insights",
    comment="Editor behavior insights and patterns - Gold Layer"
)
def wikipedia_editor_insights():
    return (
        dlt.read("silver.wikipedia_top_editors")
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
