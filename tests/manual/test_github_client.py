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
    base_url = os.getenv("BASE_URL_GITHUB")

    print("\n" + "="*50)
    print("🚀 INICIANDO PRUEBA DE INFRAESTRUCTURA")
    print("="*50)

    if not token:
        logger.error("No se encontró GITHUB_TOKEN en .env")
        return

    # 2. Inicializar Cliente
    logger.info("Intentando inicializar GitHubClient...")
    try:
        client = GitHubClient(token=token)
    except GitHubAuthError:
        logger.error("El cliente rechazó el token inmediatamente.")
        return

    logger.info(">>> Ejecutando PING...")
    if client.ping():
        logger.info("✅ PING EXITOSO: Conexión establecida.")
    else:
        logger.error("❌ PING FALLIDO.")
        return

    # Prueba de Lectura de Archivo
    # Usaremos un repo público de prueba para garantizar que funcione
    # Si quieres probar tu repo privado, cambia estas variables:
    #repo = "octocat/Hello-World" 
    #path = "README"
    
    # OPCIONAL: Descomenta esto para probar tu repo privado de AB InBev
    repo = "BrewDat/brewdat-maz-maz-masterdata-sap-repo-adf" 
    path = "trigger/tr_slv_maz_masterdata_customer_sap_dop_do_d_0100.json"

    logger.info(f">>> Intentando leer '{path}' de '{repo}'...")
    try:
        content = client.get_file(repo=repo, path=path)
        print("\n📄 CONTENIDO DESCARGADO:")
        print("-" * 20)
        print(content[:500] + "..." if len(content) > 500 else content)
        print("-" * 20 + "\n")
        logger.info("✅ LECTURA DE ARCHIVO EXITOSA.")

    except GitHubAuthError:
        logger.error("⛔ Error de Permisos (401/403). Tu token sirve, pero no para este repo.")
    except GitHubRequestError as e:
        logger.warning(f"⚠️ No se pudo leer el archivo: {e}")

    # 5. Prueba de Listar Directorio (Si agregaste el método list_directory)
    if hasattr(client, "list_directory"):
        logger.info(f">>> Listando raíz de '{repo}'...")
        try:
            items = client.list_directory(repo, "")
            print(f"\n📂 Archivos encontrados ({len(items)}):")
            for item in items[:5]:
                print(f" - [{item['type']}] {item['name']}")
            logger.info("✅ LISTADO DE DIRECTORIO EXITOSO.")
        except Exception as e:
            logger.error(f"❌ Fallo al listar: {e}")

if __name__ == "__main__":
    main()