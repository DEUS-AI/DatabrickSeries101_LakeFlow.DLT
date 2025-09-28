# Bronze Pipeline Configuration
# This pipeline processes cleaned Wikipedia data from landing layer

# Pipeline Settings
dlt.configuration = {
    "pipelines.bronze.storage": "delta",
    "pipelines.bronze.target": "main.bronze",
    "pipelines.bronze.schema": "wikipedia",
    "pipelines.bronze.catalog": "main"
}

# Dependencies
# - Source: Landing layer tables
# - Target: Delta tables in bronze schema
# - Frequency: Continuous streaming
# - Dependencies: landing.raw_wikipedia_edits
