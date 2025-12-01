__version__ = "0.1.0"

from brewbridge.domain.extractor_strategies.framework_3_0_strategy import (
    extract_artifacts_for_table,
    TableItemInput,
)

__all__ = [
    "extract_artifacts_for_table",
    "TableItemInput",
]