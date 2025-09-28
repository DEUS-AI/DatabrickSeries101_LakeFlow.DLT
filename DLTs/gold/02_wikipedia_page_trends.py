import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_page_trends",
    comment="Page edit trends and patterns - Gold Layer"
)
@dlt.expect("positive_unique_editors", "unique_editors > 0")
def wikipedia_page_trends():
    return (
        dlt.read("silver.wikipedia_top_pages")
        .withColumn("edit_intensity", 
                   F.when(F.col("edit_count") > 1000, "High")
                   .when(F.col("edit_count") > 100, "Medium")
                   .otherwise("Low"))
        .withColumn("editor_diversity", 
                   F.when(F.col("unique_editors") > 50, "High")
                   .when(F.col("unique_editors") > 10, "Medium")
                   .otherwise("Low"))
    )
