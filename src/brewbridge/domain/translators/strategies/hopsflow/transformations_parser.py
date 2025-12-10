from typing import Dict, Any, List
from brewbridge.domain.translators.base_parser import BaseParser
from brewbridge.domain.translators.strategies.hopsflow.code_analyzer import CodeAnalyzer
from brewbridge.infrastructure.logger import get_logger

logger = get_logger(__name__)

class TransformationsParser(BaseParser):
    """
    Estrategia para generar 'transformations.yaml' de Hopsflow.
    
    Fuente de Verdad:
    1. El código del notebook Silver (prioritario) para lógica de negocio.
    2. Los parámetros del Trigger (secundario) para particionamiento.
    """

    def _generate_updates(self, item: Dict[str, Any], template: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analiza el código Python y mapea las funciones encontradas a la estructura YAML de Hopsflow.
        """
        updates = {}
        table_name = item.get("table_name")

        # 1. Obtener el Código Fuente (Prioridad: Silver > Bronze)
        # Las transformaciones complejas (Hash, Dedup) suelen estar en Silver.
        script_path = item.get("notebook_path_slv")
        if not script_path:
            logger.warning(f"[{table_name}] No hay script Silver asociado. Buscando Bronze...")
            script_path = item.get("notebook_path_brz")
        
        if not script_path:
            logger.warning(f"[{table_name}] No se encontró ningún script para analizar.")
            return updates

        raw_code = self.notebooks.get(script_path, "")
        if not raw_code:
            logger.warning(f"[{table_name}] El script {script_path} está vacío o no se descargó.")
            return updates

        # 2. Instanciar al Detective (CodeAnalyzer)
        analyzer = CodeAnalyzer(raw_code)
        
        # 3. Mapeo de Lógica (Source 3.0 -> Target 4.0)
        
        # --- A. Checkpoint (Auditoría) ---
        # 3.0: widget("reset_stream_checkpoint") / manual column creation
        # 4.0: checkpoint: true
        if analyzer.detect_checkpoint_reset(): 
             # Nota: Si resetean checkpoint, asumimos que quieren usar checkpoints
             updates["checkpoint"] = True
        
        # --- B. Trazabilidad de Archivos ---
        # 3.0: F.input_file_name()
        # 4.0: source_file_name_columns: true
        if analyzer.detect_input_filename_usage():
            updates["source_file_name_columns"] = True

        # --- C. Deduplicación y Upserts ---
        # 3.0: load_type="UPSERT" + key_columns + watermark_column
        # 4.0: Combinación de 'sort_by_columns' + 'drop_duplicate'
        
        is_upsert = analyzer.detect_upsert()
        dedup_keys = analyzer.extract_dedup_keys()
        watermark_cols = analyzer.extract_watermark()

        if is_upsert or dedup_keys:
            logger.info(f"[{table_name}] Detectada lógica de UPSERT/Deduplicación.")
            
            # C.1 Ordenamiento (Vital para determinismo en Upserts)
            if watermark_cols:
                # Mapeamos la lista de columnas de agua a reglas de ordenamiento
                sort_config = []
                for col in watermark_cols:
                    sort_config.append({
                        "column": col,
                        "order": "desc" # Upsert siempre prefiere el más reciente
                    })
                updates["sort_by_columns"] = sort_config
            
            # C.2 Eliminación de Duplicados
            if dedup_keys:
                updates["drop_duplicate"] = {
                    "based_on_columns": dedup_keys
                }

        # --- D. Particionamiento Derivado (Opcional) ---
        # A veces viene en 'additional_parameters' del widget
        add_params = analyzer.get_widget_value("additional_parameters")
        if isinstance(add_params, dict):
            p_col = add_params.get("partition_column")
            p_fmt = add_params.get("partition_date_format")
            
            if p_col and p_fmt:
                # Si existe, creamos la regla de columna derivada
                updates["derived_column_partition"] = {
                    "source_column": p_col,
                    "target_column": f"{p_col}_partition", # Nombre sugerido
                    "format": p_fmt
                }

        # --- E. Hashing (Seguridad) ---
        # Detectamos si usan funciones hash en el código
        # (Esto requeriría agregar un método 'extract_hash_columns' al CodeAnalyzer)
        # Por ahora lo dejamos como placeholder para futura expansión.
        
        return updates