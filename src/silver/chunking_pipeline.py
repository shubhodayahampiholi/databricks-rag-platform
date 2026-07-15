"""
Silver chunking pipeline: dispatches successfully extracted content to the
correct chunking method, and writes silver_chunk_content -- the
deduplicated, content-addressed table gold's embedding step reads from.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

from chunking_config import resolve_chunking_method, load_chunking_config
from chunking_methods import (
    fixed_size_with_overlap, recursive_structure_aware,
    heading_hierarchy_split, table_aware_row_based,
)
from extraction_methods import ExtractionSection

CHUNKING_CONFIG = load_chunking_config(spark.conf.get("silver.chunking_config_path"))

# chunking_method_used travels with each chunk tuple, resolved once here
# -- never re-resolved downstream, so there's exactly one source of truth
# for which method actually produced a given chunk.
CHUNK_SCHEMA = ArrayType(StructType([
    StructField("chunk_index", IntegerType()),
    StructField("chunk_text", StringType()),
    StructField("section_heading", StringType()),
    StructField("token_count", IntegerType()),
    StructField("chunking_method_used", StringType()),
]))


def _chunk_row(file_type: str, extraction_method_used: str, sections_raw):
    decision = resolve_chunking_method(file_type, extraction_method_used, CHUNKING_CONFIG)
    if decision.matched_rule == "none" or decision.method is None:
        return []

    sections = [
        ExtractionSection(heading=s["heading"], text=s["text"], position=s["position"])
        for s in sections_raw
    ]

    if decision.method == "table_aware_row_based":
        chunks = table_aware_row_based(sections)
    elif decision.method == "recursive_structure_aware":
        chunks = recursive_structure_aware(sections, decision.chunk_size, decision.overlap)
    elif decision.method == "heading_hierarchy_split":
        chunks = heading_hierarchy_split(sections, decision.chunk_size, decision.overlap)
    elif decision.method == "fixed_size_with_overlap":
        chunks = fixed_size_with_overlap(sections, decision.chunk_size, decision.overlap)
    else:
        # semantic_split or any other unimplemented method -- resolver
        # matched a rule, but no implementation exists for it yet.
        return []

    return [
        (c.chunk_index, c.chunk_text, c.section_heading, c.token_count, decision.method)
        for c in chunks
    ]


chunk_udf = F.udf(_chunk_row, CHUNK_SCHEMA)


@dlt.table(
    name="silver_chunk_content",
    comment="One row per (content_hash, chunk_index). Deterministic chunk_id makes reruns idempotent.",
)
def silver_chunk_content():
    extracted = dlt.read_stream("silver_extracted_documents").filter(
        F.col("extraction_status") == "success"
    )

    chunked = extracted.withColumn(
        "chunks",
        chunk_udf(
            F.col("file_type"), F.col("extraction_method_used"), F.col("sections")
        ),
    )

    exploded = chunked.select(
        F.col("content_hash"),
        F.col("extraction_method_used"),
        F.explode("chunks").alias("chunk"),
    )

    with_chunk_id = exploded.withColumn(
        "chunk_id",
        F.sha2(
            F.concat(F.col("content_hash"), F.lit("_"), F.col("chunk.chunk_index").cast("string")),
            256,
        ),
    )

    return with_chunk_id.select(
        F.col("chunk_id"),
        F.col("content_hash"),
        F.col("chunk.chunk_text").alias("chunk_text"),
        F.col("chunk.section_heading").alias("section_heading"),
        F.col("chunk.chunking_method_used").alias("chunking_method_used"),
        F.col("extraction_method_used"),
        F.col("chunk.token_count").alias("token_count"),
        F.lit(True).alias("is_active"),
    )