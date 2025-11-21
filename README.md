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
├── migration_flow.png
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
│       │   │       └── __init__.py
│       │   ├── extractor_strategies
│       │   │   ├── __init__.py
│       │   │   ├── cobos_strategy.py
│       │   │   └── framework_3_0_strategy.py
│       │   └── tools
│       │       ├── __init__.py
│       │       ├── engineeringstore_input_builder.py
│       │       ├── extractor
│       │       │   ├── __init__.py
│       │       │   └── 3.0
│       │       │       └── __init__.py
│       │       └── template_creator.py
│       ├── humans
│       │   └── __init__.py
│       ├── infrastructure
│       │   ├── __init__.py
│       │   ├── engineeringstore_cli.py
│       │   └── logger.py
│       ├── main.py
│       ├── prompts
│       │   ├── __init__.py
│       │   ├── schema_normalizer.md
│       │   └── translators
│       │       └── __init__.py
│       └── utils
│           ├── __init__.py
│           └── exceptions.py
├── tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── integration
│   │   └── __init__.py
│   ├── manual
│   │   └── test_engineeringstore_cli_transformation_ingestion.py
│   └── unit
│       ├── __init__.py
│       ├── test_extraction.py
│       └── test_normalization.py
├── tree.md
└── uv.lock

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

