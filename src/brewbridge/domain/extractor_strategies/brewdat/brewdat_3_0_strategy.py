import json
import os
import sys
from dotenv import load_dotenv
from typing import Any, Dict, List
from brewbridge.domain.extractor_strategies.base_strategy import BaseExtractorStrategy
from brewbridge.domain.extractor_strategies.brewdat.structures import MigrationItem
from brewbridge.infrastructure import GitHubClient
from brewbridge.utils.exceptions import InvalidInputError, ExtractionError
from brewbridge.infrastructure import get_logger

logger = get_logger(__name__)


class Brewdat3Strategy(BaseExtractorStrategy):
    """
    Extraction strategy for Platform 3.0 (ADF + ADB + Governance).

    Flow:
    1. Global Context: Trigger, ADF Pipeline, Global Parameters.
    2. Parsing: Converts the Trigger into a list of MigrationItems.
    3. Code: Downloads unique (deduplicated) notebooks from the ADB repo.
    4. Quality: Downloads Governance rules specific to Silver tables.
    """

    load_dotenv()

    GOVERNANCE_REPO = os.getenv("GOVERNANCE_REPO")

    def __init__(self, github_client: GitHubClient):
        self.client = github_client

    def validate_inputs(self, pipeline_info: Dict[str, Any]) -> None:
        """Verify that we have the ADF repository and the Trigger name."""
        required = ["repo_name", "trigger_name"]
        missing = [key for key in required if not pipeline_info.get(key)]
        if missing:
            raise InvalidInputError(f"Missing fields for Brewdat3Strategy: {missing}")

    def fetch_artifacts(self, pipeline_info: Dict[str, Any]) -> Dict[str, Any]:
        repo_adf = pipeline_info["repo_name"]
        trigger_name = pipeline_info["trigger_name"]

        # Inferencia del nombre del repo de Databricks (ADB)
        repo_adb = repo_adf.replace("-repo-adf", "-repo-adb")

        logger.info(f" || Repositorios detectados -> ADF: {repo_adf} | ADB: {repo_adb}")

        artifacts = {
            "notebooks_source": {},  # Dic {path: content}
            "quality_rules": {},  # Dic {table_name: yaml_content}
        }

        logger.info("--- Fase 1: Extracción de Contexto Global ---")

        # || Extraccion Global
        # A. Trigger
        trigger_path = f"trigger/{trigger_name}.json"
        trigger_content = self.client.get_file(repo_adf, trigger_path)
        trigger_json = json.loads(trigger_content)
        artifacts["trigger_json"] = trigger_json

        # B. Pipeline ADF (Orquestador) - desde el Trigger
        try:
            pipeline_ref = trigger_json["properties"]["pipelines"][0]["pipelineReference"][
                "referenceName"
            ]
            pipeline_path = f"pipeline/{pipeline_ref}.json"
            pipe_content = self.client.get_file(repo_adf, pipeline_path)
            artifacts["pipeline_json"] = json.loads(pipe_content)
        except (KeyError, IndexError) as e:
            logger.warning(f"No se pudo extraer el pipelineReference del trigger: {e}")

        # C. Global Parameters
        artifacts["global_parameters"] = self._fetch_global_parameters(repo_adf)

        # || Parsing & Objetos (Estructuración)
        logger.info("--- Fase 2: Parsing de Tablas (MigrationItems) ---")
        migration_items = self._parse_trigger_items(trigger_json)

        # Guardamos la lista de objetos como diccionarios para serialización en el State
        artifacts["items"] = [item.model_dump() for item in migration_items]
        logger.info(f" Identificadas {len(migration_items)} tablas para migrar.")

        # || Código (Notebooks Deduplicados)
        logger.info("Fase 3: Descarga de Código (Notebooks)")
        unique_scripts = set()

        for item in migration_items:
            if item.notebook_path_brz:
                unique_scripts.add(item.notebook_path_brz)
            if item.notebook_path_slv:
                unique_scripts.add(item.notebook_path_slv)

        logger.info(f" Scripts únicos a descargar: {len(unique_scripts)}")

        for script_path in unique_scripts:
            try:
                code = self.client.get_file(repo_adb, script_path)
                artifacts["notebooks_source"][script_path] = code
            except Exception as e:
                logger.error(f" Error descargando script {script_path}: {e}")
                artifacts["notebooks_source"][script_path] = f"# ERROR: {e}"

        # Calidad (Governance IF SLV )
        logger.info("--- Fase 4: Descarga de Reglas de Calidad (Governance) ---")
        global_params_vals = self._extract_params_values(artifacts["global_parameters"])

        for item in migration_items:
            if item.has_silver:
                try:
                    gov_path = self._build_governance_path(item, global_params_vals)
                    logger.debug(f"Buscando reglas DQ para {item.table_name}: {gov_path}")

                    yaml_content = self.client.get_file(self.GOVERNANCE_REPO, gov_path)
                    artifacts["quality_rules"][item.table_name] = yaml_content

                    for dict_item in artifacts["items"]:
                        if dict_item["table_name"] == item.table_name:
                            dict_item["governance_path"] = gov_path

                except Exception as e:
                    logger.warning(f"⚠️ No hay reglas DQ para {item.table_name} o ruta inválida.")

        return artifacts

    def normalize_output(self, raw_artifacts: Dict[str, Any]) -> Dict[str, Any]:
        """Pass the artifacts directly to the State's raw_artifacts field."""
        return {"raw_artifacts": raw_artifacts}

    # Helpers
    def _fetch_global_parameters(self, repo: str) -> Dict:
        """Search and download the factory file inside 'factory/' folder."""
        try:
            files = self.client.list_directory(repo, "factory")
            for f in files:
                if f["name"].endswith(".json") and "sap-adf" in f["name"]:
                    content = self.client.get_file(repo, f["path"])
                    return json.loads(content)

            logger.warning(
                "No se encontró archivo factory específico, buscando cualquier JSON en factory/"
            )
            for f in files:
                if f["name"].endswith(".json"):
                    return json.loads(self.client.get_file(repo, f["path"]))

        except Exception as e:
            logger.warning(f"No se pudieron cargar Global Parameters: {e}")

        return {}

    def _parse_trigger_items(self, trigger_json: Dict) -> List[MigrationItem]:
        """Converts the complex parameters JSON into MigrationItem objects."""
        parsed = []
        try:
            params = (
                trigger_json.get("properties", {}).get("pipelines", [])[0].get("parameters", {})
            )
            raw_items = params.get("items_to_process", {}).get("pipelines", [])

            for entry in raw_items:
                brz = entry.get("load_to_bronze")
                slv = entry.get("load_to_silver")

                t_name = (
                    slv.get("target_table")
                    if slv
                    else (brz.get("target_table") if brz else "unknown")
                )

                # Calcular rutas de notebooks
                nb_brz = self._clean_adb_path(brz.get("adb_notebook_path")) if brz else None
                nb_slv = self._clean_adb_path(slv.get("adb_notebook_path")) if slv else None

                item = MigrationItem(
                    table_name=t_name,
                    bronze_config=brz,
                    silver_config=slv,
                    notebook_path_brz=nb_brz,
                    notebook_path_slv=nb_slv,
                )
                parsed.append(item)

        except Exception as e:
            logger.error(f"Error parseando estructura del Trigger: {e}")
            raise ExtractionError("El formato del Trigger no coincide con el estándar esperado.")

        return parsed

    def _clean_adb_path(self, raw_path: str) -> str:
        """clean //repo/path -> path.py"""
        if not raw_path:
            return None
        path = raw_path
        if raw_path.startswith("//"):
            parts = raw_path.split("/", 3)
            if len(parts) > 3:
                path = parts[3]
        if not path.endswith(".py"):
            path += ".py"
        return path

    def _extract_params_values(self, global_json: Dict) -> Dict[str, str]:
        """
        Flattens the global parameters for easier use.
        Takes values from 'prod' by default or from the root level.
        """

        values = {}
        try:
            gp = global_json.get("properties", {}).get("globalParameters", {})
            for k, v in gp.items():
                if "value" in v and isinstance(v["value"], str):
                    values[k] = v["value"]

            env_params = gp.get("default_parameters_per_environment", {}).get("value", {})
            target_env = env_params.get("prod") or env_params.get("dev") or {}

            values.update(target_env)

        except Exception:
            pass  # En caso de error, devolvemos lo que pudimos extraer
        return values

    def _build_governance_path(self, item: MigrationItem, global_params: Dict) -> str:
        """Builds the complex path for the quality YAML."""
        # src/{zone}/{zone}/{domain}/{system}/dq_definitions/{country}/{db}/{table}.yaml

        # Datos desde Silver Config -> Trigger
        slv = item.silver_config
        system = slv.get("source_system", "unknown")
        country = slv.get("source_system_country", "unknown")
        db = slv.get("target_database", "unknown")
        table = item.table_name

        # Datos desde Global Params
        zone = global_params.get("project_zone", "maz")
        domain = global_params.get("project_business_domain", "tech")

        return f"src/{zone}/{zone}/{domain}/{system}/dq_definitions/{country}/{db}/{table}.yaml"
