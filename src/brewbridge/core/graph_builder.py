from typing import Dict, Literal

from langgraph.graph import END, StateGraph
from src.state import MigrationGraphState


def node_read_manifest(state: MigrationGraphState) -> Dict:
    print("\n--- [Paso 1] READ MANIFEST (MOCK) ---")
    return {
        "api_connectivity_ok": True,
        "current_pipeline_data": {"name": "pipe_brz_ventas", "type": "bronze"},
    }


def node_framework_creator(state: MigrationGraphState) -> Dict:
    print("--- [Paso 2] FRAMEWORK CREATOR (MOCK) ---")
    mock_env = "brz"
    print(f"    Detected environment: {mock_env}")
    return {
        "environment_type": mock_env,
        "pipeline_template": "plantilla_hopsflow_mock",
        "transform_template": "plantilla_transform_mock",
    }


def node_extractor(state: MigrationGraphState) -> Dict:
    print("--- [Paso 3] EXTRACTOR (MOCK) ---")
    return {"raw_artifacts_3_0": {"adf": "{}", "notebook": "print('hola mundo')"}}


def node_schema_normalizer(state: MigrationGraphState) -> Dict:
    print("--- [Paso 4] 🧠 SCHEMA NORMALIZER (MOCK AI) ---")
    return {"normalized_schema_v4": {"mock_key": "mock_value"}}


def router_translators(state: MigrationGraphState) -> Literal["route_hopsflow", "route_brewtiful"]:
    env = state.get("environment_type")
    print(f"--- [Paso 5] 🔀 ROUTER: Decidiendo ruta para '{env}' ---")
    if env in ["brz", "slv"]:
        return "route_hopsflow"
    return "route_brewtiful"
