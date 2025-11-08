from typing import Literal, Dict
from langgraph.graph import StateGraph, END
from src.state import MigrationGraphState

# aqui estoy apenas tratando de crear la estructura como tipo test con un camino por defecto para probar el flujo completo
def node_read_manifest(state: MigrationGraphState) -> Dict:
    print("\n--- [Paso 1] READ MANIFEST (MOCK) ---")
    # Simulamos que leemos un manifiesto y encontramos un pipeline 'brz'
    return {
        "api_connectivity_ok": True,
        "current_pipeline_data": {"name": "pipe_brz_ventas", "type": "bronze"}
    }

def node_framework_creator(state: MigrationGraphState) -> Dict:
    print("--- [Paso 2] FRAMEWORK CREATOR (MOCK) ---")
    # Simulamos detección de Hopsflow (brz) --> despues a gld para probar brewtiful
    mock_env = "brz" 
    print(f"    Detected environment: {mock_env}")
    return {
        "environment_type": mock_env,
        "pipeline_template": "plantilla_hopsflow_mock",
        "transform_template": "plantilla_transform_mock"
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