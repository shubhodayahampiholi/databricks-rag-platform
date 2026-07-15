"""
OCR extraction via Azure AI Document Intelligence, used specifically for
scanned/image-based PDFs where pymupdf_standard would return empty text.

This function has NOT been executed against a real Azure resource -- no
credentials or endpoint are available in the environment this was
authored in. Unlike the other extraction methods, this one is written
correctly to the best of current documentation, not verified by running
it. Two things specifically worth checking before first real use:

1. The exact parameter name for passing raw bytes to begin_analyze_document
   -- this has varied across SDK versions in what could be confirmed;
   check against the actual installed azure-ai-documentintelligence
   version's signature.
2. Serverless compute has its own outbound network egress controls --
   confirm the Document Intelligence endpoint is reachable from the
   pipeline's serverless compute before assuming this works in production.
"""
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

from extraction_methods import ExtractionResult, ExtractionSection

# Databricks secret scope holding the Document Intelligence endpoint and
# key. Provisioned once per platform, not per team -- see PREREQUISITES.md.
SECRET_SCOPE = "platform-document-intelligence"


def _get_client() -> DocumentIntelligenceClient:
    endpoint = dbutils.secrets.get(scope=SECRET_SCOPE, key="endpoint")
    api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")
    return DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(api_key)
    )


def azure_document_intelligence_ocr(content: bytes) -> ExtractionResult:
    """
    Uses the "prebuilt-read" model specifically -- it performs OCR and
    returns plain text, without the added cost and complexity of
    "prebuilt-layout" (which also extracts tables and structure we don't
    need here, since chunking already handles structure separately).
    """
    try:
        client = _get_client()
        # NOTE: verify this parameter name against the installed SDK
        # version -- some versions expect `body=content` directly for
        # bytes, others expect a wrapped request object.
        poller = client.begin_analyze_document("prebuilt-read", body=content)
        result = poller.result()
    except Exception as e:
        return ExtractionResult(status="corrupt", full_text="", error_message=str(e))

    sections = []
    full_text_parts = []
    for page_num, page in enumerate(result.pages or []):
        page_text = "\n".join(line.content for line in (page.lines or []))
        sections.append(
            ExtractionSection(heading=None, text=page_text, position=page_num)
        )
        full_text_parts.append(page_text)

    full_text = "\n\n".join(full_text_parts).strip()
    status = "success" if full_text else "empty"
    return ExtractionResult(status=status, full_text=full_text, sections=sections)