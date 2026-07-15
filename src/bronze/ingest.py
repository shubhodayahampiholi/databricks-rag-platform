"""
Bronze ingestion pipeline: raw file capture from a team's External Volume.

Captures every file, every type, in full -- unconditionally. Bronze judges
nothing; it captures everything. See docs/DESIGN.md, Bronze Layer, for the
full rationale behind every decision in this file.
"""
import io
import zipfile

import dlt
from pypdf import PdfReader
from pypdf.errors import PdfReadError
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Populated from config/team.yml at bundle deploy time via variable substitution
SOURCE_VOLUME_PATH = spark.conf.get("bronze.source_volume_path")

DETECTION_SCHEMA = StructType([
    StructField("file_type", StringType()),
    StructField("processing_status", StringType()),
])


def _validate_pdf(content: bytes) -> str:
    """
    Confirms the file is a genuinely parseable PDF, not just one that
    starts with the correct header signature. A truncated or corrupted
    PDF can carry a valid %PDF header while failing to open at all.
    """
    try:
        reader = PdfReader(io.BytesIO(content))
        if len(reader.pages) == 0:
            return "corrupt_pdf"
        return "valid"
    except PdfReadError:
        return "corrupt_pdf"


def detect_file_type_and_status(content: bytes):
    """
    Detect file type from content signature and internal structure, not
    file extension -- extensions can be wrong (renamed files, corrupted
    uploads). Returns (file_type, processing_status).
    """
    if content is None or len(content) < 4:
        return ("unknown", "empty_or_truncated")

    if content[:4] == b"%PDF":
        return ("pdf", _validate_pdf(content))

    if content[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                names = z.namelist()
                if "word/document.xml" in names:
                    return ("docx", "valid")
                if "xl/workbook.xml" in names:
                    return ("xlsx", "valid")
                return ("zip_based_unknown", "unsupported_file_type")
        except zipfile.BadZipFile:
            return ("zip_based_unknown", "corrupt_zip")

    return ("unknown", "unsupported_file_type")


detect_file_type_udf = F.udf(detect_file_type_and_status, DETECTION_SCHEMA)


@dlt.table(
    name="bronze_documents",
    comment="Raw file capture. Every file, every type, full content, unconditionally.",
)
@dlt.expect_or_drop("has_content_column", "content IS NOT NULL")
def bronze_documents():
    file_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.useNotifications", "true")
        .load(SOURCE_VOLUME_PATH)
    )

    enriched_stream = file_stream.withColumn(
        "file_detection", detect_file_type_udf(F.col("content"))
    )

    return enriched_stream.select(
        F.col("content"),
        F.col("path").alias("source_path"),
        F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
        F.col("length").alias("file_size"),
        F.col("modificationTime").alias("file_modification_time"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.sha2(F.col("content"), 256).alias("content_hash"),
        F.col("file_detection.file_type").alias("file_type"),
        F.element_at(F.split(F.col("path"), "/"), -2).alias("source_classification_tag"),
        F.col("file_detection.processing_status").alias("processing_status")
    )