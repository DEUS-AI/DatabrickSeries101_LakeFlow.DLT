import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="wikipedia_edits_cleaned",
    comment="Cleaned and structured Wikipedia edit events - Bronze Layer"
)
def wikipedia_edits_cleaned():
    return (
        dlt.read("raw_wikipedia_edits")
        .withColumn("event_timestamp", F.from_unixtime(F.col("timestamp")/1000))
        .withColumn("is_bot_edit", F.col("bot"))
        .withColumn("is_minor_edit", F.col("minor"))
        .withColumn("page_title", F.col("title"))
        .withColumn("editor", F.col("user"))
        .withColumn("edit_comment", F.col("comment"))
        .withColumn("old_length", F.col("length.old"))
        .withColumn("new_length", F.col("length.new"))
        .withColumn("length_change", F.col("length.new") - F.col("length.old"))
        .withColumn("old_revision", F.col("revision.old"))
        .withColumn("new_revision", F.col("revision.new"))
        .withColumn("wiki_domain", F.col("server_name"))
        .withColumn("wiki_language", F.col("wiki"))
        .withColumn("parsed_comment", F.col("parsedcomment"))
        .withColumn("meta_uri", F.col("meta.uri"))
        .withColumn("meta_request_id", F.col("meta.request_id"))
        .withColumn("meta_domain", F.col("meta.domain"))
        .withColumn("meta_stream", F.col("meta.stream"))
        .withColumn("meta_topic", F.col("meta.topic"))
        .withColumn("meta_partition", F.col("meta.partition"))
        .withColumn("meta_offset", F.col("meta.offset"))
        .select(
            "id", "type", "namespace", "page_title", "editor", 
            "edit_comment", "event_timestamp", "is_bot_edit", 
            "is_minor_edit", "old_length", "new_length", "length_change",
            "old_revision", "new_revision", "wiki_domain", "wiki_language",
            "parsed_comment", "meta_uri", "meta_request_id", "meta_domain",
            "meta_stream", "meta_topic", "meta_partition", "meta_offset"
        )
    )
