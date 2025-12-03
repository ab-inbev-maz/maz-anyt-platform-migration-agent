from dotenv import load_dotenv

from brewbridge.infrastructure.databricks_client import DatabricksClient

load_dotenv()


def main():
    client = DatabricksClient()

    print("Running smoke test...")

    # Prueba trivial: 1+1
    result = client.run_query("SELECT 1 AS value, 1+1 AS result")

    print("RAW JSON RESULT:")
    print(result)

    df = client.read_table("system.information_schema.tables", limit=5)

    print("\nDATAFRAME RESULT:")
    print(df)


if __name__ == "__main__":
    main()
