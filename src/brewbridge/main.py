import os
from io import BytesIO

import dotenv
import mlflow
from langchain_core.runnables.graph import MermaidDrawMethod
from PIL import Image

from brewbridge.core.graph_builder import MigrationGraphBuilder
from brewbridge.core.state import MigrationGraphState
from brewbridge.infrastructure.logger import get_logger
from brewbridge.infrastructure.observability import end_pipeline_run, start_pipeline_run

dotenv.load_dotenv()

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:8080"))


# mlflow.langchain.autolog()
def main():
    logger = get_logger("brewbridge")

    initial_state = {
        "environment_type": "brz",
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
        "current_pipeline_data": {"pipeline_name": "test_ingestion_x"},
    }

    # initial_state = {
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
    #     "current_pipeline_data": {"pipeline_name": "test_pipeline_x"},
    # }

    builder = MigrationGraphBuilder(logger=logger).build()
    runnable = builder.compile()

    if os.environ.get("DEBUG") == "true":
        png_bytes = runnable.get_graph().draw_mermaid_png(
            draw_method=MermaidDrawMethod.API, output_file_path="migration_flow.png"
        )

        img = Image.open(BytesIO(png_bytes))
        img.show()

    # Start a top-level MLflow run for this pipeline invocation.
    start_pipeline_run(initial_state)

    try:
        final_state = runnable.invoke(initial_state)
        final_state_obj = MigrationGraphState(**final_state)
        logger.info("Final state: %s", final_state_obj)
        end_pipeline_run(status="success")
    except Exception:
        # If something goes wrong at the graph level, make sure the pipeline
        # run is still closed and marked as failed.
        end_pipeline_run(status="error")
        raise


if __name__ == "__main__":
    main()
    main()
