from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class MigrationGraphState(BaseModel):
    # Initialization fields (filled by Read_Manifest_and_Check_API)
    manifest_path: Optional[str] = Field(default=None)
    credentials: Optional[Dict[str, str]] = Field(default=None)
    api_connectivity_ok: Optional[bool] = Field(default=None)
    pipelines_to_migrate: Optional[List[Dict[str, Any]]] = Field(default=None)
    
    # Existing fields
    environment_type: Optional[str] = Field(default=None)
    normalized_schema_v4: Optional[Dict[str, Any]] = Field(default=None)
    current_pipeline_data: Optional[Dict[str, Any]] = Field(default=None)
    template_path: Optional[str] = Field(default=None)

    @field_validator("environment_type")
    def validate_environment(cls, val):
        if val is None:
            return val
        if val not in {"brz", "slv", "gld"}:
            raise ValueError(f"Invalid environment_type '{val}'. Expected brz, slv or gld.")
        return val

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    def __repr__(self):
        return f"MigrationGraphState({self.model_dump()})"
