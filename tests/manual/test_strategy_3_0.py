import json
import os
import sys

from dotenv import load_dotenv

try:
    from brewbridge.domain.extractor_strategies.brewdat.brewdat_3_0_strategy import Brewdat3Strategy
    from brewbridge.infrastructure import GitHubClient, get_logger
    from brewbridge.utils.exceptions import BrewBridgeError
except ImportError as e:
    print(f"❌ Error de importación: {e}")
    sys.exit(1)

logger = get_logger("TEST_STRATEGY")


def print_separator(title):
    print(f"\n{'=' * 80}")
    print(f" 🔍 {title}")
    print(f"{'=' * 80}")


def explore_object_structure(raw_artifacts):
    """
    Simula cómo el 'Schema Normalizer' navegaría este objeto para extraer valor.
    """
    print_separator("SIMULACIÓN DE ACCESO A DATOS (Como lo haría el Normalizador)")

    # 1. ACCESO A VARIABLES GLOBALES
    # Objetivo: Obtener la 'project_zone' de producción para el metadata.yaml
    print("\n👉 CASO 1: Obteniendo Variables de Entorno (Globales)")
    try:
        globals_json = raw_artifacts.get("global_parameters", {})
        # Navegación profunda segura
        prod_params = globals_json["properties"]["globalParameters"][
            "default_parameters_per_environment"
        ]["value"]["prod"]

        zone = prod_params.get("project_zone")
        domain = prod_params.get("project_business_domain")
        country = prod_params.get("source_system_country")

        print(f"   ✅ Zona: {zone}")
        print(f"   ✅ Dominio: {domain}")
        print(f"   ✅ País: {country}")
    except KeyError as e:
        print(f"   ❌ Fallo navegando Global Params: {e}")

    # 2. ACCESO A DETALLES DE UNA TABLA ESPECÍFICA
    # Objetivo: Ver la configuración de 'mx_dd02l'
    target_table = "mx_dd02l"
    print(f"\n👉 CASO 2: Inspeccionando configuración de '{target_table}'")

    # Buscamos la tabla en la lista (Simulando el loop principal)
    # Nota: Como serializamos con model_dump(), accedemos como diccionarios, no objetos.
    items = raw_artifacts.get("items", [])
    found_item = next((i for i in items if i["table_name"] == target_table), None)

    if found_item:
        # A. Ver Configuración Bronze
        brz_cfg = found_item.get("bronze_config", {})
        print(
            f"   🔹 Origen Bronze: {brz_cfg.get('source_system')} -> {brz_cfg.get('source_table')}"
        )
        print(f"   🔹 Carga Incremental: {brz_cfg.get('incremental_load')}")

        # B. Ver Configuración Silver (Mapping)
        slv_cfg = found_item.get("silver_config", {})
        mapping = slv_cfg.get("column_mapping", [])
        print(f"   🔹 Columnas Mapeadas (Silver): {len(mapping)}")
        if mapping:
            print(
                f"      Ejemplo: {mapping[0]['source_column_name']} -> {mapping[0]['target_data_type']}"
            )

        # 3. CRUCE DE REFERENCIA: OBTENER CÓDIGO PYTHON
        # Objetivo: Leer el script asociado a esta tabla para buscar lógica custom
        print(f"\n👉 CASO 3: 'Linkeando' el Código Python para '{target_table}'")

        script_path = found_item.get("notebook_path_brz")
        print(f"   🎫 Ticket (Ruta): {script_path}")

        if script_path:
            # Buscamos en la 'Biblioteca' usando el ticket
            library = raw_artifacts.get("notebooks_source", {})
            code = library.get(script_path, "")

            if code:
                print(f"   ✅ Código encontrado en biblioteca ({len(code)} caracteres).")
                # Análisis simple del código
                if "opflag" in code:
                    print(
                        "   ⚠️  Análisis: El código contiene lógica de 'opflag' (Borrado lógico detectado)."
                    )
                if "write_stream_delta_table" in code:
                    print("   ✅  Análisis: El código usa escritura Delta Stream.")
            else:
                print("   ❌ El código no está en la biblioteca (Error de carga).")

        # 4. CRUCE DE REFERENCIA: OBTENER REGLAS DQ
        # Objetivo: Obtener el YAML de calidad
        print(f"\n👉 CASO 4: Obteniendo Reglas de Calidad para '{target_table}'")

        dq_library = raw_artifacts.get("quality_rules", {})
        dq_yaml = dq_library.get(target_table)

        if dq_yaml:
            print("   ✅ YAML de calidad encontrado.")
            print(
                f"   📄 Contenido (Snippet):\n   {dq_yaml[:100].replace(chr(10), chr(10) + '   ')}..."
            )
        else:
            print("   ⚠️ No hay reglas de calidad para esta tabla (o falló la descarga).")

    else:
        print(f"   ❌ No se encontró la tabla {target_table} en los items extraídos.")


def main():
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")

    if not token:
        logger.error("Falta GITHUB_TOKEN en .env")
        return

    # 1. Inicialización
    print_separator("INICIALIZACIÓN")
    try:
        client = GitHubClient(token)
        strategy = Brewdat3Strategy(client)
        logger.info("✅ Estrategia instanciada.")
    except Exception as e:
        logger.error(f"Fallo al iniciar: {e}")
        return

    # 2. Input
    pipeline_info = {
        "repo_name": "BrewDat/brewdat-maz-maz-tech-sap-repo-adf",
        "trigger_name": "tr_slv_maz_tech_metadata_sap_pr0_mx_d_0500",
    }

    # 3. Ejecución
    print_separator("EJECUTANDO ESTRATEGIA")
    try:
        result = strategy.extract(pipeline_info)
        raw_artifacts = result.get("raw_artifacts", {})
        logger.info("✅ Extracción finalizada.")

        # 4. Exploración Profunda (La parte nueva)
        explore_object_structure(raw_artifacts)

    except BrewBridgeError as e:
        logger.error(f"🛑 Error Controlado: {e}")
    except Exception as e:
        logger.critical(f"💥 Error Inesperado: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
