# Landing Pipeline Configuration
# This pipeline processes raw Wikipedia data from blob storage

# Pipeline Settings
dlt.configuration = {
    "pipelines.landing.storage": "delta",
    "pipelines.landing.target": "main.landing",
    "pipelines.landing.schema": "wikipedia",
    "pipelines.landing.catalog": "main"
}

# Dependencies
# - Source: Azure Blob Storage (wikipedia-data container)
# - Target: Delta tables in landing schema
# - Frequency: Continuous streaming
