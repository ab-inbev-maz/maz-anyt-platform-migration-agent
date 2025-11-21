from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class MigrationGraphState(BaseModel):
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
