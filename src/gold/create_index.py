"""
One-time index creation, made safe to run on every bundle deploy.
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import DeltaSyncVectorIndexSpecRequest

w = WorkspaceClient()

index_name = f"{team_catalog}.{team_schema}.gold_embeddings_index"

try:
    w.vector_search_indexes.get_index(index_name=index_name)
    print(f"Index {index_name} already exists -- skipping creation.")
except Exception:
    # NOTE: catching broadly here because the exact exception type for
    # "index not found" (likely a NotFound/ResourceDoesNotExist variant)
    # is unconfirmed -- narrow this once verified against a real call.
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=team_vector_search_endpoint,
        primary_key="chunk_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{team_catalog}.{team_schema}.gold_embeddings",
            pipeline_type="TRIGGERED",
            # NOTE: field name for referencing the precomputed
            # embedding_vector column is unconfirmed -- see CORRECTIONS.md #16.
        ),
    )
    print(f"Created index {index_name}.")