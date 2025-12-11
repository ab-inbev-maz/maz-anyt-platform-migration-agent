from typing import Dict, Any, List
from brewbridge.domain.translators.base_parser import BaseParser
from brewbridge.domain.translators.strategies.transformations_parser.code_analyzer import CodeAnalyzer
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)

class TransformationsParser(BaseParser):
    """
    Strategy to generate 'transformations.yaml' for Hopsflow.
    
    Source of Truth:
    1. The Silver notebook code for business logic.
    2. The Trigger parameters for partitioning.
    """

    def _generate_updates(self, item: Dict[str, Any], template: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizes the Python code and maps found functions to the Hopsflow YAML structure.
        """
        updates = {}
        table_name = item.get("table_name")

        script_path = item.get("notebook_path_slv")
        if not script_path:
            logger.warning(f"[{table_name}] is not there script silver associated. Searching Bronze...")
            script_path = item.get("notebook_path_brz")
        
        if not script_path:
            logger.warning(f"[{table_name}] No script found to analyze.")
            return updates

        raw_code = self.notebooks.get(script_path, "")
        if not raw_code:
            logger.warning(f"[{table_name}] The script {script_path} is empty or not found.")
            return updates

        analyzer = CodeAnalyzer(raw_code)
        
        # Checkpoint 
        # 3.0: widget("reset_stream_checkpoint") / manual column creation
        # 4.0: checkpoint: true
        if analyzer.detect_checkpoint_reset(): 
             # Nota: Si resetean checkpoint, asumimos que quieren usar checkpoints
             updates["checkpoint"] = True
        
        # Trazabilidad de Archivos 
        # 3.0: F.input_file_name()
        # 4.0: source_file_name_columns: true
        if analyzer.detect_input_filename_usage():
            updates["source_file_name_columns"] = True

        # Deduplicación y Upserts
        # 3.0: load_type="UPSERT" + key_columns + watermark_column
        # 4.0: Combinación de 'sort_by_columns' + 'drop_duplicate'
        
        is_upsert = analyzer.detect_upsert()
        dedup_keys = analyzer.extract_dedup_keys()
        watermark_cols = analyzer.extract_watermark()

        if is_upsert or dedup_keys:
            logger.info(f"[{table_name}] UPSERT/Deduplication logic detected.")
            
            if watermark_cols:
                sort_config = []
                for col in watermark_cols:
                    sort_config.append({
                        "column": col,
                        "order": "desc"
                    })
                updates["sort_by_columns"] = sort_config
            
            if dedup_keys:
                updates["drop_duplicate"] = {
                    "based_on_columns": dedup_keys
                }


        add_params = analyzer.get_widget_value("additional_parameters")
        if isinstance(add_params, dict):
            p_col = add_params.get("partition_column")
            p_fmt = add_params.get("partition_date_format")
            
            if p_col and p_fmt:
                updates["derived_column_partition"] = {
                    "source_column": p_col,
                    "target_column": f"{p_col}_partition", # Nombre sugerido
                    "format": p_fmt
                }
        
        return updates