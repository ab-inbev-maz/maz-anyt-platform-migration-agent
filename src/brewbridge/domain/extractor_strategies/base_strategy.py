from abc import ABC, abstractmethod
from typing import Any, Dict
from brewbridge.infrastructure.logger import get_logger
from brewbridge.utils.exceptions import ExtractionError

logger = get_logger(__name__)

class BaseExtractorStrategy(ABC):
    """
    Abstract base class that defines the contract for all extraction strategies.
    Implements the Template Method pattern in 'extract'.
    """

    def extract(self, pipeline_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates the standard extraction flow.
        DO NOT OVERRIDE this method in subclasses unless strictly necessary.
        """
        strategy_name = self.__class__.__name__
        logger.info(f"[{strategy_name}] Starting extraction process...")

        try:
            # Validación de entradas
            self.validate_inputs(pipeline_info)
            
            # Extracción física (Fetch)
            raw_artifacts = self.fetch_artifacts(pipeline_info)
            
            # Normalización de salida
            normalized_data = self.normalize_output(raw_artifacts)
            
            logger.info(f"✅ [{strategy_name}] Extraction completed successfully.")
            return normalized_data

        except Exception as e:
            logger.error(f"Critical error in {strategy_name}: {e}")
            # Re-lanzamos como error de dominio para que el Grafo lo maneje
            raise ExtractionError(f"Fallo en estrategia {strategy_name}: {e}") from e

    @abstractmethod
    def validate_inputs(self, pipeline_info: Dict[str, Any]) -> None:
        """Verifies that 'pipeline_info' contains the minimum required data (e.g., the trigger name)."""
        raise NotImplementedError

    @abstractmethod
    def fetch_artifacts(self, pipeline_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Performs the logic to fetch/download files.
        Must implement discovery, download and multi-repo handling.
        """
        raise NotImplementedError

    @abstractmethod
    def normalize_output(self, raw_artifacts: Dict[str, Any]) -> Dict[str, Any]:
        """Structures raw data into the format expected by 'MigrationGraphState'."""
        raise NotImplementedError