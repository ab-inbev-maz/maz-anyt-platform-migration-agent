"""
Manual tests for EngineeringStoreCLI:
- Transformation template creation
- Ingestion template creation

Run:
    uv run tests/manual/test_engineeringstore_cli_transformation_ingestion.py
"""

from brewbridge.infrastructure.engineeringstore_cli import (
    EngineeringStoreCLI, EngineeringStoreCommand)

# ============================================================
# 1) TRANSFORMATION TEST
# ============================================================

transformation_input = """\
maz
maz
co
sales
test
* * 3 3 4
test
platform
transformation

promo
yn
"""


def run_transformation():
    cli = EngineeringStoreCLI()

    command = EngineeringStoreCommand(
        command=[
            "engineeringstore",
            "transformation",
            "--create-template-files"
        ],
        table_type="gold",
        needs_input=True,
    )

    print("\n\n>>> Ejecutando TRANSFORMATION...")
    try:
        stdout = cli.run(command, input_text=transformation_input)
        print("\n>>> STDOUT:")
        print(stdout)
    except Exception as e:
        print("\n>>> ERROR:")
        print(e)


# ============================================================
# 2) INGESTION TEST
# ============================================================

ingestion_input = """\
maz
maz
co
sales
test
* * 2 * 2
test
test
blob
sap
sap
sap
sap-secret
yn
"""


def run_ingestion():
    cli = EngineeringStoreCLI()

    command = EngineeringStoreCommand(
        command=[
            "engineeringstore",
            "ingestion",
            "--create-template-files"
        ],
        table_type="brz",      # ingestion → hopsflow
        needs_input=True,
    )

    print("\n\n>>> Ejecutando INGESTION...")
    try:
        stdout = cli.run(command, input_text=ingestion_input)
        print("\n>>> STDOUT:")
        print(stdout)
    except Exception as e:
        print("\n>>> ERROR:")
        print(e)


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    run_transformation()
    run_ingestion()
