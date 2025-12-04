from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class MigrationItem(BaseModel):
    """
    Represent a entity (table) uniquely identified within the ADF Trigger.
    Facilitates handling Bronze/Silver configuration as objects.
    """

    table_name: str = Field(..., description="Name of the destination table (Silver).")

    # Configuración del Trigger
    bronze_config: Optional[Dict[str, Any]] = Field(
        None, description="Parameters for load_to_bronze"
    )
    silver_config: Optional[Dict[str, Any]] = Field(
        None, description="Paremeters for load_to_silver"
    )

    # Rutas calculadas (Metadatos para el Fetcher)
    notebook_path_brz: Optional[str] = None
    notebook_path_slv: Optional[str] = None

    # Referencia al archivo de Governance (If Slv)
    governance_path: Optional[str] = None

    @property
    def has_silver(self) -> bool:
        return self.silver_config is not None

    @property
    def source_system(self) -> Optional[str]:
        """Helper to get the source system (needed for paths)."""
        if self.silver_config:
            return self.silver_config.get("source_system")
        if self.bronze_config:
            return self.bronze_config.get("source_system")
        return None
