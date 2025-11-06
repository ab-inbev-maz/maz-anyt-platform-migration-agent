from typing import TypedDict, List, Dict, Any, Optional
#Esta es la propuesta inicial del state central que va a ir pasando por los nodos del grafo de migracion
class MigrationGraphState(TypedDict):
    """
    Central state of the migration flow.
    Defines the data contract between all graph nodes.
    """
    # Entradas Iniciales 
    manifest_path: str
    credentials: Optional[Dict[str, str]]
    api_connectivity_ok: bool
    pipelines_to_migrate: List[Dict[str, Any]]
    current_pipeline_data: Optional[Dict[str, Any]]

    # Framework & Templates
    environment_type: Optional[str]
    pipeline_template: Optional[str]
    transform_template: Optional[str]
    notebook_template: Optional[str]

    # Extracción 3.0 
    raw_artifacts_3_0: Optional[Dict[str, Any]]

    # Normalización 4.0 
    normalized_schema_v4: Optional[Dict[str, Any]]

    # Artefactos Generados (outputs)
    # En comun
    acl_yaml: Optional[str]
    metadata_yaml: Optional[str]
    quality_yaml: Optional[str]
    sync_yaml: Optional[str]
    observability_yaml: Optional[str]
    # Hopsflow  'slv'/'brz'
    pipeline_yaml: Optional[str]       
    transformations_yaml: Optional[str] 
    # Brewtiful 'gld' NotebookTranslator + Ruff
    generated_notebooks: Optional[List[str]] 

    # Validación 
    validator_output: Optional[str]
    validation_passes: bool
    retry_count: int

    # Aprobación Humana
    human_approval_decision: Optional[str]

    # Auditoría Final
    migration_summary_md: Optional[str]
    push_status: Optional[str]