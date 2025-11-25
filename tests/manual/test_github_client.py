import os
import sys
from dotenv import load_dotenv
from brewbridge.infrastructure.github_client import GitHubClient
from brewbridge.utils.exceptions import GitHubAuthError, GitHubRequestError
from brewbridge.infrastructure.logger import get_logger

logger = get_logger("TEST_INFRA")

def main():
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")

    print("\n" + "="*50)
    print("STARTING INFRASTRUCTURE TEST")
    print("="*50)

    if not token:
        logger.error("No GITHUB_TOKEN found in .env")
        return

    # Initialize Client
    logger.info("Trying to initialize GitHubClient...")
    try:
        client = GitHubClient(token=token)
    except GitHubAuthError:
        logger.error("The client rejected the token immediately.")
        return

    logger.info(">>> Executing PING...")
    if client.ping():
        logger.info(" SUCCESSFUL PING: Connection established.")
    else:
        logger.error(" PING FAILED.")
        return

    # File Read Test
    repo = "BrewDat/brewdat-maz-maz-masterdata-sap-repo-adf" 
    path = "trigger/tr_slv_maz_masterdata_customer_sap_dop_do_d_0100.json"

    logger.info(f">>> Trying to read '{path}' from '{repo}'...")
    try:
        content = client.get_file(repo=repo, path=path)
        print("\n DOWNLOADED CONTENT:")
        print("-" * 20)
        print(content[:500] + "..." if len(content) > 500 else content)
        print("-" * 20 + "\n")
        logger.info(" FILE READ SUCCESSFUL.")

    except GitHubAuthError:
        logger.error(" Permission Error (401/403). Your token works, but not for this repo.")
    except GitHubRequestError as e:
        logger.warning(f" Could not read the file: {e}")

    # Directory Listing Test
    if hasattr(client, "list_directory"):
        logger.info(f">>> Listing root of '{repo}'...")
        try:
            items = client.list_directory(repo, "")
            print(f"\n Files found ({len(items)}):")
            for item in items[:5]:
                print(f" - [{item['type']}] {item['name']}")
            logger.info(" DIRECTORY LISTING SUCCESSFUL.")
        except Exception as e:
            logger.error(f" Failed to list: {e}")

if __name__ == "__main__":
    main()