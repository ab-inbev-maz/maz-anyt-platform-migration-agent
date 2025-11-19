# 🍺 BrewBridge AI

> **Bridging legacy data to the new platform — one pipeline at a time.**

BrewBridge AI is an **intelligent agentic framework** designed to **automate data pipeline migrations** from legacy platforms (e.g., Data Platform 3.0, COBOS) to the modern **Platform 4.0** ecosystem — powered by **Hopsflow** and **Brewtiful** frameworks.

This project leverages **LangGraph** to orchestrate a hybrid workflow of deterministic tools, LLM-based translators, and human-in-the-loop validation nodes — delivering scalable, auditable, and production-grade migrations.

---

## 🧩 Key Highlights

- **End-to-end intelligent migration** from legacy systems to Platform 4.0.
- **Single agent flow** handling classification, extraction, translation, validation, and deployment.
- **Framework-aware architecture**: adapts automatically for Hopsflow (bronze/silver) or Brewtiful (gold) pipelines.
- **Human-in-the-loop approval** for governance and quality assurance.
- **Extensible “Source + Strategy” pattern** supporting multiple origins (GitHub, ADF, SQL, JSON, etc.).
- **Deployable as a package**: works locally, in Databricks Jobs, or via CI/CD.

---

## ⚙️ Architecture Overview

The BrewBridge agent operates through a **LangGraph-based state machine** composed of three types of nodes:

| Node Type | Description | Example |
|------------|--------------|----------|
| 🛠️ **Tool Node** | Deterministic, procedural Python function. No LLM. | `Read_Manifest`, `Validator_Tool` |
| 🧠 **Agent Node** | LLM-powered specialist with a single-purpose prompt. | `Schema_Normalizer`, `Translator` |
| 👤 **Human Node** | Human approval or decision checkpoint. | `Human_Approval_Node` |

Each pipeline migration runs through the full agentic flow:
> Manifest → Extraction → Normalization → Translation → Validation → Human Approval → Deployment

---

## 🧠 Intelligent Extraction Layer

Extraction is handled by a **single `ExtractorTool`**, which dynamically loads **Source Handlers** and **Framework Strategies** depending on the origin framework (e.g., 3.0, COBOS).

### 🔌 Source Handlers
Reusable connectors that know **how** to access a data source:
- `GitHubSourceHandler`
- `ADFSourceHandler`
- `SQLSourceHandler`
- `JSONSourceHandler`

### 🧩 Strategies
Define **what** to extract and **how** to interpret it for each framework:
- `GitHub3_0Strategy`, `GitHubCobosStrategy`
- `ADF3_0Strategy`, `SQLCobosStrategy`

This pattern isolates framework-specific logic while keeping the extraction flow unified and extensible.

---

## 🧱 Folder Structure

```plaintext
.
├── README.md
├── LICENSE
├── pyproject.toml
├── .env.example
├── .gitignore
│
├── inputs/
│   ├── manifest.yaml
│   └── samples/
│       ├── adf_pipeline.json
│       ├── notebook_sample.py
│       └── cobos_query.sql
│
├── outputs/
│   ├── 2025-11-08_pipeline_x/
│   │   ├── raw_artifacts/
│   │   │   ├── adf_pipeline.json
│   │   │   └── notebook_source.py
│   │   ├── normalized_schema_v4.json
│   │   ├── acl.yaml
│   │   ├── metadata.yaml
│   │   ├── quality.yaml
│   │   ├── sync.yaml
│   │   ├── observability.yaml
│   │   ├── pipeline.yaml
│   │   ├── transformations.yaml
│   │   ├── migration_summary.md
│   │   └── logs/
│   │       └── validator_output.txt
│   └── ...
│
├── cache/
│   ├── brewtiful/
│   └── hopsflow/
│
└── src/
    └── brewbridge/
        ├── __init__.py
        ├── main.py
        ├── config.py
        │
        ├── core/
        │   ├── graph_builder.py
        │   ├── state.py
        │   ├── base_nodes.py           # ToolNode, AgentNode, HumanNode
        │   ├── runner.py
        │   ├── callbacks.py
        │   └── __init__.py
        │
        ├── domain/
        │   ├── tools/
        │   │   ├── read_manifest.py
        │   │   ├── framework_creator.py
        │   │   ├── extractor/                # Extractor ToolNodes
        │   │   │   ├── extractor_tool_v3.py  # Step 3 - 3.0
        │   │   │   ├── extractor_tool_cobos.py
        │   │   │   └── __init__.py
        │   │   ├── router_tool.py
        │   │   ├── validator_tool.py
        │   │   ├── generator.py
        │   │   ├── ruff_formatter.py
        │   │   └── __init__.py
        │   │
        │   ├── extractor_strategies/         # Deterministic Strategy Pattern
        │   │   ├── base_strategy.py
        │   │   ├── framework_3_0_strategy.py
        │   │   ├── cobos_strategy.py
        │   │   └── __init__.py
        │   │
        │   ├── agents/
        │   │   ├── schema_normalizer.py
        │   │   ├── corrector_agent.py
        │   │   ├── reporter_logger.py
        │   │   ├── translators/
        │   │   │   ├── acl_translator.py
        │   │   │   ├── metadata_translator.py
        │   │   │   ├── quality_translator.py
        │   │   │   ├── sync_translator.py
        │   │   │   ├── observability_translator.py
        │   │   │   ├── pipeline_translator.py           # Hopsflow only
        │   │   │   ├── transformations_translator.py    # Hopsflow only
        │   │   │   ├── notebook_translator.py           # Brewtiful only
        │   │   │   └── __init__.py
        │   │   └── __init__.py
        │   │
        │   ├── humans/
        │   │   ├── approval_node.py
        │   │   └── decision_node.py
        │   │
        │   └── __init__.py
        │
        ├── infra/
        │   ├── github_client.py
        │   ├── datafactory_client.py      # optional, if still used
        │   ├── engineeringstore_cli.py
        │   ├── storage_manager.py
        │   ├── logger.py
        │   └── __init__.py
        │
        ├── prompts/
        │   ├── schema_normalizer_prompt.py
        │   ├── corrector_agent_prompt.py
        │   ├── reporter_logger_prompt.py
        │   └── translators/
        │       ├── acl_prompt.py
        │       ├── metadata_prompt.py
        │       ├── quality_prompt.py
        │       ├── sync_prompt.py
        │       ├── observability_prompt.py
        │       ├── pipeline_prompt.py
        │       ├── transformations_prompt.py
        │       ├── notebook_prompt.py
        │       └── __init__.py
        │
        ├── utils/
        │   ├── file_utils.py
        │   ├── yaml_utils.py
        │   ├── retry_utils.py
        │   ├── exceptions.py
        │   └── __init__.py
        │
        └── __init__.py

````

---

## 🧰 Setup & Execution

### Installation

```bash
uv sync
uv pip install -e .
```

### Run Migration

```bash
uv run brewbridge --manifest inputs/manifest.yaml
```

### Test Suite

```bash
pytest -v
```

---

## 📦 Packaging

BrewBridge AI is an installable Python package following the `src/` layout.
You can build and distribute it using:

```bash
uv build
uv pip install dist/brewbridge-0.1.0-py3-none-any.whl
```

---

## 🧭 Project Vision

> BrewBridge AI is designed not just to migrate, but to **learn and adapt**.
> Future iterations will integrate telemetry, anomaly detection, and self-healing translation logic for continuous improvement.

---

**Developed by:** Brewdat Platform Team

**Ecosystem:** AB InBev – BrewDat 4.0 / Hopsflow / Brewtiful

