.
├── .env
├── .env.example
├── .gitignore
├── .python-version
├── cache
│   ├── brewtiful
│   │   └── .gitkeep
│   └── hopsflow
│       └── .gitkeep
├── inputs
│   ├── .gitkeep
│   └── samples
│       ├── normalized_ingestion_logistics_single_task.json
│       └── normalized_ingestion_sales_multi_task.json
├── LICENSE
├── outputs
│   ├── .gitkeep
│   └── 2025-11-08_pipeline_x
│       └── raw_artifacts
│           └── .gitkeep
├── pyproject.toml
├── README.md
├── RFC.md
├── src
│   └── brewbridge
│       ├── __init__.py
│       ├── config.py
│       ├── core
│       │   ├── __init__.py
│       │   ├── base_nodes.py
│       │   ├── graph_builder.py
│       │   └── state.py
│       ├── domain
│       │   ├── __init__.py
│       │   ├── agents
│       │   │   ├── __init__.py
│       │   │   └── translators
│       │   ├── extractor_strategies
│       │   │   ├── __init__.py
│       │   │   ├── cobos_strategy.py
│       │   │   └── framework_3_0_strategy.py
│       │   └── tools
│       │       ├── __init__.py
│       │       └── extractor
│       │           ├── __init__.py
│       │           └── 3.0
│       │               └── __init__.py
│       ├── humans
│       │   └── __init__.py
│       ├── infrastructure
│       │   └── __init__.py
│       ├── main.py
│       ├── prompts
│       │   ├── __init__.py
│       │   └── schema_normalizer.md
│       └── utils
│           └── __init__.py
├── tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── integration
│   │   └── __init__.py
│   └── unit
│       ├── __init__.py
│       ├── test_extraction.py
│       └── test_normalization.py
├── tree.md
└── uv.lock

26 directories, 43 files
