# Silver Pipeline Configuration
# This pipeline creates business logic tables from bronze layer

# Pipeline Settings
dlt.configuration = {
    "pipelines.silver.storage": "delta",
    "pipelines.silver.target": "main.silver",
    "pipelines.silver.schema": "wikipedia",
    "pipelines.silver.catalog": "main"
}

# Dependencies
# - Source: Bronze layer tables
# - Target: Delta tables in silver schema
# - Frequency: Continuous streaming
# - Dependencies: bronze.wikipedia_edits_cleaned
