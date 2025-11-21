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

Aquí tienes la sección **limpia, final y perfecta**, sin los puntos 6 y 7.
Lista para pegar directo en tu README.

---

# 📊 MLflow Local Observability Setup

To enable the new **Observability Layer**, every developer must run a **local MLflow Tracking Server**.
This ensures a consistent environment for inspecting traces, artifacts, metrics, YAML diffs, and node-level behaviors across the entire BrewBridge migration flow.

This setup is lightweight, reproducible, and fully aligned with the team’s local development workflow.

---

## 🔧 1. Install Dependencies (via `uv`)

All MLflow dependencies are already defined in the project configuration.

Every developer simply needs to run:

```bash
uv sync
```

This installs MLflow and all required observability packages into the virtual environment.

---

## 🚀 2. Start the Local MLflow Tracking Server

From the project root:

```bash
mlflow server \
  --host 127.0.0.1 \
  --port 8080 \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root ./mlruns
```

This launches:

* **SQLite** → local metadata storage
* `./mlruns/` → artifact store
* MLflow UI → [http://127.0.0.1:8080](http://127.0.0.1:8080)

> Every developer runs this locally.
> Zero cloud dependency. No credentials required. Full autonomy.

---

## 🏷️ 3. Configure BrewBridge to Log to Local MLflow

Add this to your local `.env` (ignored by Git):

```
MLFLOW_TRACKING_URI=http://127.0.0.1:8080
MLFLOW_EXPERIMENT_NAME=brewbridge_observability
```

The observability layer will automatically route all traces and metrics to your local MLflow instance.

---

## 🧪 4. Validate the Setup

Run:

```python
import dotenv
import mlflow

load_dotenv()

print("Tracking:", mlflow.get_tracking_uri())

with mlflow.start_run():
    mlflow.log_param("env_test", "ok")
    mlflow.log_metric("latency_ms", 123)
```

Open the UI:
👉 [http://127.0.0.1:8080](http://127.0.0.1:8080)

You should see the test run.

---

## 🐳 5. Optional – Docker Compose

If the team prefers a containerized environment, add:

```yaml
# docker-compose.yml
services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    ports:
      - "8080:8080"
    volumes:
      - ./mlruns:/mlruns
      - ./mlflow.db:/mlflow.db
    command: >
      mlflow server
      --host 0.0.0.0
      --port 8080
      --backend-store-uri sqlite:///mlflow.db
      --default-artifact-root /mlruns
```

Start it:

```bash
docker compose up -d
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

