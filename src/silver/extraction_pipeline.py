"""
Silver extraction pipeline: dispatches bronze rows to the correct
extraction method, deduplicates by content_hash to avoid re-running
expensive extraction on already-seen content, and writes both the
extraction checkpoint and the document-instance record.

NOT YET TESTED against a real Spark/Databricks environment -- written to
the best of current understanding, same status as bronze/ingest.py before
its own testing pass surfaced real corrections. See CORRECTIONS.md.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)

from extraction_config import resolve_extraction_method, load_extraction_config
from extraction_methods import (
    pymupdf_standard, python_docx_standard, openpyxl_standard, plain_text_decode,
)
from extraction_ocr import azure_document_intelligence_ocr

EXTRACTION_CONFIG = load_extraction_config(spark.conf.get("silver.extraction_config_path"))

# Retrieved ONCE on the driver, not inside the distributed UDF -- secret
# retrieval per-row across executors is the wrong pattern and likely
# doesn't work reliably in a distributed context.
_DI_ENDPOINT = dbutils.secrets.get(scope="platform-document-intelligence", key="endpoint")
_DI_API_KEY = dbutils.secrets.get(scope="platform-document-intelligence", key="api_key")

METHOD_DISPATCH = {
    "pymupdf_standard": lambda content: pymupdf_standard(content),
    "python_docx_standard": lambda content: python_docx_standard(content),
    "openpyxl_standard": lambda content: openpyxl_standard(content),
    "plain_text_decode": lambda content: plain_text_decode(content),
    "azure_document_intelligence_ocr": lambda content: azure_document_intelligence_ocr(content), 
}

EXTRACTION_SCHEMA = StructType([
    StructField("extraction_method_used", StringType()),
    StructField("extraction_status", StringType()),
    StructField("full_text", StringType()),
    StructField("sections", ArrayType(StructType([
        StructField("heading", StringType()),
        StructField("text", StringType()),
        StructField("position", IntegerType()),
    ]))),
    StructField("error_message", StringType()),
])


def _extract_row(source_path: str, file_type: str, content: bytes):
    decision = resolve_extraction_method(source_path, file_type, EXTRACTION_CONFIG)
    if decision.matched_rule == "none":
        return (None, "no_extraction_rule_matched", "", [], None)

    extract_fn = METHOD_DISPATCH.get(decision.method)
    if extract_fn is None:
        return (decision.method, "no_extraction_rule_matched", "", [], f"Unknown method: {decision.method}")

    result = extract_fn(content)
    sections = [(s.heading, s.text, s.position) for s in result.sections]
    return (decision.method, result.status, result.full_text, sections, result.error_message)


extract_udf = F.udf(_extract_row, EXTRACTION_SCHEMA)


@dlt.table(
    name="silver_extracted_documents",
    comment="Extraction checkpoint, keyed by content_hash. Extraction runs once per unique content, regardless of how many source paths reference it.",
)
def silver_extracted_documents():
    bronze = dlt.read_stream("bronze_documents").filter(
        F.col("processing_status") == "valid"
    )

    # Layer 1: dedup within this batch -- two new files with the same
    # content_hash arriving together must not both trigger extraction.
    deduped_batch = bronze.dropDuplicates(["content_hash"])

    # Layer 2: anti-join against already-extracted content from prior
    # batches -- this is what actually saves the expensive extraction
    # call for content seen before.
    already_extracted = dlt.read("silver_extracted_documents").select("content_hash")
    new_content = deduped_batch.join(already_extracted, "content_hash", "left_anti")

    extracted = new_content.withColumn(
        "extraction", extract_udf(F.col("source_path"), F.col("file_type"), F.col("content"))
    )

    return extracted.select(
        F.col("content_hash"),
        F.col("extraction.extraction_method_used").alias("extraction_method_used"),
        F.col("extraction.extraction_status").alias("extraction_status"),
        F.col("extraction.full_text").alias("full_text"),
        F.col("extraction.sections").alias("sections"),
        F.col("extraction.error_message").alias("error_message"),
        F.current_timestamp().alias("extracted_at"),
    )


@dlt.table(
    name="silver_document_instances",
    comment="One row per source_path ever seen. Always written, whether or not extraction ran for that content_hash.",
)
def silver_document_instances():
    bronze = dlt.read_stream("bronze_documents").filter(
        F.col("processing_status") == "valid"
    )
    return bronze.select(
        F.col("source_path"),
        F.col("file_name"),
        F.col("content_hash"),
        F.col("ingestion_timestamp"),
        F.col("file_type"),
        F.lit(None).cast("string").alias("resolved_acl_group"),
        F.lit(True).alias("is_active"),
        F.lit(None).cast("timestamp").alias("deactivated_at"),
    )