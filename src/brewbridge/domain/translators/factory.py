from typing import Dict, Any, Type
from brewbridge.domain.translators.base_parser import BaseParser
from brewbridge.domain.translators.strategies.transformations_parser.transformations_parser import TransformationsParser
from brewbridge.utils.exceptions import ParserError
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)

class TranslatorFactory:
    """
    Factory Method to instantiate the correct Parser based on artifact type and framework.
    """

    # Registro de Parsers 
    # Clave: (framework, tipo_archivo)
    _REGISTRY: Dict[tuple, Type[BaseParser]] = {
        ("hopsflow", "transformations"): TransformationsParser,
        # Por desarrollar:
        # ("hopsflow", "acl"): AclParser,
        # ("hopsflow", "metadata"): MetadataParser,
        # ("brewtiful", "pipeline"): BrewtifulPipelineParser,
    }

    @staticmethod
    def get_parser(framework: str, artifact_type: str, raw_artifacts: Dict[str, Any]) -> BaseParser:
        """
        Returns an instance of the requested parser by injecting the raw artifacts.
        
        Args:
            framework: "hopsflow" | "brewtiful"
            artifact_type: "transformations" | "acl" | "metadata" | "pipeline"
            raw_artifacts: The large state dictionary containing all extracted data.

        """
        key = (framework, artifact_type)
        
        parser_class = TranslatorFactory._REGISTRY.get(key)
        
        if not parser_class:
            valid_keys = [f"{k[0]}/{k[1]}" for k in TranslatorFactory._REGISTRY.keys()]
            error_msg = f"No existe un parser registrado para: {framework}/{artifact_type}. Disponibles: {valid_keys}"
            logger.error(error_msg)
            raise ParserError(error_msg)

        logger.info(f" Factory: Instanciando {parser_class.__name__}")
        return parser_class(raw_artifacts)