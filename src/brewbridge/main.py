# Import read_manifest_and_check_api tool
import json
import os
from io import BytesIO
from pathlib import Path

import dotenv
import mlflow
from langchain_core.runnables.graph import MermaidDrawMethod
from PIL import Image

from brewbridge.core.graph_builder import MigrationGraphBuilder
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import end_pipeline_run, start_pipeline_run
from brewbridge.utils.manifest_yaml_utils import load_manifest

dotenv.load_dotenv()

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:8080"))


mlflow.langchain.autolog()


def main():
    logger = get_logger("brewbridge")
    manifest_path = Path(__file__).parent.parent.parent / "inputs" / "samples" / "manifest.yaml"
    state = load_manifest(str(manifest_path))
    state["manifest_path"] = str(manifest_path)
    state.update(
        {
            "environment_type": "slv",
            "normalized_schema_v4": {
                "zone": "maz",
                "landing_zone": "maz",
                "domain": "logistics",
                "pipeline": "test_ingestion_x",
                "schedule": "* * 2 * *",
                "table_name": "raw_logistics_orders",
                "owner": "platform",
                "connector": "blob",
                "source_system": "sap-test",
                "source_entity": "sap-test",
                "target_entity": "sap-test",
                "connection_id": "sap-test-secret",
                "transformations": "",
                "acl": "yn",
            },
        }
    )

    # Alternative initial state for gold environment (commented out, preserved for reference)
    # state = {
    #     "environment_type": "gld",
    #     "normalized_schema_v4": {
    #         "zone": "maz",
    #         "landing_zone": "maz",
    #         "country": "co",
    #         "domain": "sales",
    #         "owner": "platform",
    #         "schedule": "* * 2 * *",
    #         "table_scope": "transformation",
    #         "data_product_subdomain": "promo",
    #         "table_name": "test_table",
    #         "acl": "y",
    #         "trigger": "n"
    #     },
    #     "pipeline_info": {"pipeline_name": "test_pipeline_x"},
    # }

    # Build migration graph
    builder = MigrationGraphBuilder(logger=logger).build()
    runnable = builder.compile()

    try:
        png_bytes = runnable.get_graph().draw_mermaid_png(
            draw_method=MermaidDrawMethod.API, output_file_path="migration_flow.png"
        )
        img = Image.open(BytesIO(png_bytes))
        img.show()
    except Exception as e:
        logger.warning(f"No se pudo generar la imagen del grafo: {e}")

    start_pipeline_run(state)
    logger.info("🎥 Observabilidad iniciada (MLflow run created)")

    try:
        logger.info("🚀 Iniciando ejecución del grafo...")

        final_state = runnable.invoke(state)

        final_state_obj = MigrationGraphState(**final_state)
        logger.info("✅ Grafo finalizado exitosamente.")
        # Save final_state_obj to final_state.json in the project root
        root_path = Path(__file__).parent.parent.parent
        with open(root_path / "final_state.json", "w") as f:
            json.dump(final_state_obj.model_dump(), f, indent=2)
        # logger.debug(f"Estado Final: {final_state_obj}")

        end_pipeline_run(status="success")
        logger.info("🎥 Observabilidad finalizada (Status: Success)")

    except Exception as e:
        logger.error(f"💥 Error crítico durante la ejecución del grafo: {e}")

        end_pipeline_run(status="failed")
        raise e


if __name__ == "__main__":
    main()
