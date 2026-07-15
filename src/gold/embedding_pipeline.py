"""
Gold embedding pipeline: embeds active chunks using the team's single
configured model, and writes the gold table Vector Search's Delta Sync
Index reads from.
"""
import dlt
from pyspark.sql import functions as F

EMBEDDING_MODEL = spark.conf.get("gold.embedding_model")  # single value, not a resolver -- see CORRECTIONS.md #13


@dlt.table(
    name="gold_embeddings",
    comment="Servable embeddings. Vector Search's Delta Sync Index reads directly from this table.",
)
def gold_embeddings():
    chunks = dlt.read_stream("silver_chunk_content").filter(F.col("is_active"))

    # Aggregated, not joined raw -- a single content_hash can have
    # multiple active document_instances (a rename, a copy). A raw join
    # would silently duplicate every chunk once per instance. Citation
    # display should honestly show every source a piece of content is
    # currently known under, not one arbitrarily picked path.
    instances = dlt.read("silver_document_instances").filter(F.col("is_active")).groupBy("content_hash").agg(
                F.collect_list(
                F.struct(F.col("source_path"), F.col("file_name"))
            ).alias("source_references"),
            F.collect_set("resolved_acl_group").alias("_distinct_acl_groups"),
        ).withColumn(
            "acl_conflict_detected", F.size(F.col("_distinct_acl_groups")) > 1
        ).withColumn(
            "resolved_acl_group",
            F.when(F.col("acl_conflict_detected"), F.lit(None))
            .otherwise(F.element_at(F.col("_distinct_acl_groups"), 1)),
        ).drop("_distinct_acl_groups")

    joined = chunks.join(instances, "content_hash", "inner")

    embedded = joined.withColumn(
        "embedding_vector",
        F.expr(f"ai_query('{EMBEDDING_MODEL}', chunk_text)"),
    )

    return embedded.select(
        F.col("chunk_id"),
        F.col("chunk_text"),
        F.col("embedding_vector"),
        F.lit(EMBEDDING_MODEL).alias("embedding_model"),
        F.current_timestamp().alias("embedded_at"),
        F.col("source_references"),
        F.col("content_hash"),
        F.col("section_heading"),
        F.col("resolved_acl_group"),
        F.col("chunking_method_used"),
        F.col("extraction_method_used"),
    )