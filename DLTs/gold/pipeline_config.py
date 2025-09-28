# Gold Pipeline Configuration
# This pipeline creates analytics and insights tables from silver layer

# Pipeline Settings
dlt.configuration = {
    "pipelines.gold.storage": "delta",
    "pipelines.gold.target": "main.gold",
    "pipelines.gold.schema": "wikipedia",
    "pipelines.gold.catalog": "main"
}

# Dependencies
# - Source: Silver layer tables
# - Target: Delta tables in gold schema
# - Frequency: Continuous streaming
# - Dependencies: silver.wikipedia_top_editors, silver.wikipedia_top_pages
