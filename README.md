# Arquitectura de Migración Inteligente

## El Problema: Migración de Lógica de Plataforma

Este proyecto aborda la migración de pipelines de datos de nuestra **Plataforma 3.0** a la nueva **Plataforma 4.0**.

Los artefactos de la 3.0 consisten en una combinación de:
* **JSONs de Azure Data Factory (ADF):** Definen el flujo de orquestación.
* **Notebooks de Databricks:** Contienen la lógica de transformación en Python/Spark.

La Plataforma 4.0 desmantela esta estructura en favor de un conjunto de 8 o más archivos YAML especializados (como `acl.yaml`, `metadata.yaml`, `pipeline.yaml`, etc.), cuya estructura depende del framework de destino.

---

## El Desafío Central: Lógica Condicional (Hopsflow vs. Brewtiful)

La complejidad de la traducción radica en que la Plataforma 4.0 utiliza frameworks distintos basados en la capa de la Arquitectura Medallion:

* **Framework Hopsflow:** Se utiliza para pipelines de capa (brz, slv).
* **Framework Brewtiful:** Se utiliza para pipelines de capa Oro (gld).

Por lo tanto, el sistema de migración debe primero clasificar el pipeline 3.0 y luego generar un conjunto de artefactos completamente diferente basado en esa clasificación.

---

## La Solución: Una Arquitectura de Agente Robusta (LangGraph)

Para automatizar esta traducción compleja, se diseñó un agente basado en **LangGraph**. Esta arquitectura permite construir un flujo de trabajo por pasos, condicional, paralelizable y robusto.

---
#### Para comprender la estructura de la arquitectura, es fundamental definir los tres tipos de nodos que conforman esta solución.
## Tipos de Nodos en la Arquitectura

La estructura se compone de tres tipos principales de nodos:

### 1. Nodo-Herramienta (El Trabajador o Enrutador)
**¿Tiene LLM?** No.
**¿Tiene Tools?** No. El nodo *es* la herramienta (una sola función de Python).
**Propósito:** Ejecutar tareas deterministas (mecánicas) o lógicas (enrutamiento if/else). No "piensa", solo "hace".
* **Ejemplos en nuestra arquitectura:**
    * `Read_Manifest_and_Check_API`
    * `Framework_Loader`
    * `Enrutador_de_Translators`
    * `Validator_Tool`
    * `Check Validation`
    * `Check Human Decision`
    * `Generator`
    * `Ruff Format`

### 2. Nodo-Agente Simple (El Especialista)
**¿Tiene LLM?** Sí.
**¿Tiene Tools?** No.
**Propósito:** Ejecutar una tarea de "pensamiento" o traducción altamente especializada. Su inteligencia está 100% enfocada en su prompt específico. No decide entre herramientas, solo ejecuta su única tarea de IA.
* **Ejemplos en nuestra arquitectura:**
    * `Schema_Normalizer`
    * Todos los 8+ Translators (ej. `ACLTranslator`, `PipelineTranslator`, etc.)
    * `CorrectorAgent`
    * `ReporterLogger`

### 3. Nodo-Agente con Herramientas (El Gerente)
**¿Tiene LLM?** Sí.
**¿Tiene Tools?** Sí. Se le proporciona un "cinturón de herramientas" (múltiples funciones) que puede usar.
**Propósito:** Ejecutar tareas dinámicas o exploratorias. El LLM (el "Gerente") decide qué herramientas usar y en qué orden para cumplir un objetivo complejo.
* **Ejemplos en nuestra arquitectura:**
    * `Data_Fetcher` (que decide si usar la `github_api_tool` o la `databricks_api_tool`)

---

## El Flujo Completo de la Arquitectura

Este flujo describe el procesamiento de un solo pipeline de la lista contenida en el manifiesto.

```mermaid

flowchart TD
    subgraph MigrationFlow
        
        %% --- 1. Start and Pre-Validation ---
        A["START - manifest.yaml"]:::flow --> B["1. Read_Manifest_and_Check_API, Reads manifest and credentials. Validates API connectivity"]:::tool

        %% --- 2. Extraction (Split into 2 Nodes) ---
        subgraph C_Fetcher ["2 Data_Fetcher_Agent_with_Tools"]
            direction TB

            N1_LLM["🧠 LLM (Manager) Decides which tools to use"]
            subgraph Tools
                direction LR
                T1[" github_api_tool"]:::tool
                T2[" databricks_api_tool"]:::tool
            end
            N1_LLM --> T1
            N1_LLM --> T2
        end
        B --> C_Fetcher

        %% Node 2b: The Normalizer
        C_Normalizer["3. Schema_Normalizer -----(Simple Agent) ----------🧠 LLM (Specialist) Translates and Classifies"]:::agent
        C_Fetcher --> C_Normalizer

        %% --- Routing ---
        D_Loader["4. Framework_Loader ------(Tool: Loads Hopsflow/Brewtiful templates)"]:::tool
        C_Normalizer --> D_Loader
        
        D_Router["5.Translator_Router --- (Tool: Lists parallel nodes based on bronze/slv/gld)"]:::tool
        D_Loader --> D_Router

        %% --- Parallel Branches ---
        P2["TransformationsTranslator"]:::agent
        D_Router -->|"slv / brz"| P2

        G1["NotebookTranslator"]:::agent
        RF["Ruff Format (TOOL)"]:::tool
        D_Router -->|"gld"| G1
        G1 --> RF

        P1["PipelineTranslator"]:::agent
        C1["ACLTranslator"]:::agent
        C2["MetadataTranslator"]:::agent
        C3["QualityTranslator"]:::agent
        C4["SyncTranslator"]:::agent
        C5["ObservabilityTranslator"]:::agent

        D_Router -->|"slv / brz/ gld"| P1
        D_Router -->|"slv / brz/ gld"| C1
        D_Router -->|"slv / brz/ gld"| C2
        D_Router -->|"slv / brz/ gld"| C3
        D_Router -->|"slv / brz/ gld"| C4
        D_Router -->|"slv / brz/ gld"| C5

        %% --- 5. Validation and Correction Loop ---
        V["6 Validator_Tool, Runs 'engineeringstore --validate-dags'"]:::tool
        P1 --> V
        C1 --> V
        C2 --> V
        C3 --> V
        C4 --> V
        C5 --> V
        P2 --> V
        RF --> V

        CV["7 Check Validation, 'engineeringstore' report OK?"]:::tool
        V --> CV
        
        COR["8 CorrectorAgent, Uses 'engineeringstore' output to fix artifacts (MAX 3 iter.)"]:::agent
        CV -->|"FAIL"| COR
        COR --> V

        %% --- 6. Human Approval ---
        HITL["9 Human_Approval_Node PAUSE: awaits final human approval"]:::human
        CV -->|"PASS"| HITL

        %% --- 7. Final Flow ---
        CH["10 Check Human Decision"]:::human
        HITL --> CH
        
        R["11. ReporterLogger Writes migration_summary.md"]:::agent
        CH -->|"APPROVE"| R
        
        GEN["12. Generator Pushes to target repository"]:::tool
        R --> GEN
        
        Z["END - Package Ready"]:::flow
        GEN --> Z

        %% --- 8. Exit on Rejection ---
        ZR["END - Rejected by Human"]:::flow
        CH -->|"REJECT"| ZR
    end

    %% --- STYLES ---
    classDef tool fill:#fff3b0,stroke:#806c00,stroke-width:1px,color:#000;
    classDef agent fill:#9fd5ff,stroke:#004d80,stroke-width:1px,color:#000;
    classDef agent_with_tools fill:#ffd8b1,stroke:#a15800,stroke-width:1px,color:#000;
    classDef human fill:#c8f7c5,stroke:#2b8000,stroke-width:1px,color:#000;
    classDef flow fill:#e0e0e0,stroke:#888,stroke-width:1px,color:#000;

```

### Paso 1: Read_Manifest_and_Check_API (Ingesta y Pre-Validación) (Opcional)
* **Tipo de Nodo:** Nodo-Herramienta (Puro Python) | **:::tool**
* **Inicio:** El flujo comienza cuando un humano invoca al agente con la ruta a un `manifest.yaml`.
* **Acción:** Este nodo (una función de Python) lee el `manifest.yaml` para extraer:
    * La lista de pipelines a migrar (`pipelines_to_migrate`).
    * Las credenciales de API (`credentials`).
* **Lógica:** Realiza un "pre-flight check" usando las credenciales para hacer "ping" a las APIs de GitHub y Databricks y confirmar la conectividad.
* **Actualización de Estado:** El `GraphState` se actualiza con `credentials`, `api_connectivity_ok = True`, y la lista `pipelines_to_migrate`. El orquestador externo ahora iterará sobre esta lista.

### Paso 2: Data_Fetcher
* **Tipo de Nodo:** Nodo-Agente con Herramientas (Gerente) | **:::agent_with_tools**
* **Acción:** Este nodo toma las `credentials` y el `current_pipeline_data` (el primer ítem del manifiesto) del estado.
* **Lógica (LLM):** El LLM recibe un prompt para "recolectar archivos". Para hacerlo, decide qué herramientas de su cinturón usar:
    * *Ejemplo:*
    * Llama a `github_api_tool` para obtener el JSON de ADF.
    * Llama a `databricks_api_tool` para obtener el notebook.
* **Actualización de Estado:** Guarda los artefactos 3.0 crudos en `state['raw_artifacts_3_0']`.

### Paso 3: Schema_Normalizer
* **Tipo de Nodo:** Nodo-Agente Simple (Especialista) | **:::agent**
* **Acción:** Este nodo se activa después del `Data_Fetcher`. Toma los `raw_artifacts_3_0` del estado.
* **Lógica (LLM):** Llama al LLM (Especialista) con un prompt enfocado en dos tareas:
    1.  **Traducir:** Analizar los artefactos crudos y generar el `normalized_schema_v4.json`.
    2.  **Clasificar:** identificar `environment_type` ('slv' o 'gld'). -> (Este puede ser una función de python)
* **Actualización de Estado:** Guarda `normalized_schema_v4` y `environment_type` en el estado.

```mermaid

graph TD
    subgraph "Extraction Flow (2 Steps)"
        direction LR
        
        %% --- Node 1: Agent with Tools ---
        subgraph A ["2. Data_Fetcher (Node-Agent with Tools)"]
            direction TB
            
            %% The "brain" of the agent
            N1_LLM["🧠 LLM (Manager)<br/>Decides which tools to use"]
            
            %% The tools the brain can use
            subgraph "Tools"
                direction LR
                T1[" github_api_tool"]
                T2[" databricks_api_tool"]
                T3[" ... "]              
            end
            
            %% The brain uses the tools
            N1_LLM --> T1
            N1_LLM --> T2
            N1_LLM --> T3            
        end

        %% --- Node 2: Simple Agent ---
        B["3 Schema_Normalizer<br/>(Node-Simple Agent)<br/>🧠 LLM (Specialist)<br/>Translates and Classifies"]

        %% --- The Main Flow ---
        A --> B
    end

```

### Paso 4: Framework_Loader
* **Tipo de Nodo:** Nodo-Herramienta (Puro Python) | **:::tool**
* **Acción:** Este nodo lee el `environment_type` del estado.
* **Lógica:** Es un `if/else` que "hace checkout" de las plantillas correctas. Si es 'slv', carga los template files de Hopsflow. Si es 'gld', carga los de Brewtiful.
* **Actualización de Estado:** Guarda las plantillas de texto crudo (ej. `state['pipeline_template']`) en el estado.
* **Comandos:**
    ```bash
    engineeringstore transformation --create-template-files (glds)
    engineeringstore ingestion --create-template-files (brz, slv)
    ```

### Paso 5: Enrutador_de_Translators
* **Tipo de Nodo:** Nodo-Herramienta (Enrutador Condicional) | **:::tool**
* **Acción:** Lee el `environment_type` del estado.
* **Lógica:** Es un `if/else` que define el plan de ejecución paralelo. Define una lista de traductores comunes (como `PipelineTranslator`, `ACLTranslator`, etc.) y añade los traductores condicionales (`TransformationsTranslator` si es 'slv', `NotebookTranslator` si es 'gld').
* **Salida:** Retorna una lista de strings (ej. `["PipelineTranslator", "ACLTranslator", "NotebookTranslator"...]`) que LangGraph usará para el siguiente paso.

```mermaid

graph TD
    subgraph "Extraction and Routing Flow"
        A["...Schema_Normalizer"] --> B["4 Framework_Loader<br/>(Tool: Loads Hopsflow/Brewtiful templates)"]
        B --> C["5 Translator_Router<br/>(Tool: Lists parallel nodes)"]
        C --> D["Parallel: Translators..."]
    end

    %% Color Styles
    style B fill:#fff3b0,stroke:#806c00,stroke-width:1px,color:#000;
    style C fill:#fff3b0,stroke:#806c00,stroke-width:1px,color:#000;

```

### Paso 6: Translators (El "Fan-Out" Paralelo)
* **Tipo de Nodo:** Nodos-Agente Simples (Especialistas) | **:::agent**
* **Acción:** LangGraph toma la lista del enrutador y ejecuta todos esos nodos `Translator` en **paralelo**.
* **Lógica (LLM):** Cada nodo `Translator` (ej. `ACLTranslator`, `MetadataTranslator`, etc.) es un "Especialista" que toma el `normalized_schema_v4` y su plantilla correspondiente (cargada en el Paso 4) y genera el archivo YAML final.
* **Actualización de Estado:** Cada nodo escribe en su propio campo del estado (ej. `state['acl_yaml'] = "..."`).

### Paso 7: Ruff Format
* **Tipo de Nodo:** Nodo-Herramienta (Puro Python) | **:::tool**
* **Acción:** Este nodo se ejecuta solo en el branch 'gld', después del `NotebookTranslator`.
* **Lógica:** Es una función simple que toma el código del `generated_notebooks` y lo formatea usando la herramienta `ruff` para asegurar la calidad del código.
* **Actualización de Estado:** Sobrescribe `state['generated_notebooks']` con el código formateado.

### Paso 8: Validator_Tool (Validación Específica)
* **Tipo de Nodo:** Nodo-Herramienta (Trabajador) | **:::tool**
* **Acción (Sincronización):** Actúa como una barrera **"Fan-In"**. Espera a que todos los traductores (Paso 6) y el formateador (Paso 7, si se ejecutó) terminen.
* **Lógica (Herramienta):** Ejecuta el comando `engineeringstore --validate-dags` sobre los artefactos generados.
* **Actualización de Estado:** Captura la salida de texto (stdout/stderr) y la guarda en `state['validator_output']`.

### Paso 9: Check Validation (Bucle de Auto-Corrección)
* **Tipo de Nodo:** Nodo-Herramienta (Enrutador Condicional) | **:::tool**
* **Acción:** Lee el `state['validator_output']` y el `state['retry_count']`.
* **Lógica (Fallo):** Si el `validator_output` contiene errores y `retry_count` es menor a 3:
    * Incrementa `retry_count` y establece `validation_passes = False`.
    * Desvía el flujo al `CorrectorAgent`.
* **Lógica (Éxito):** Si no hay errores:
    * Establece `validation_passes = True`.
    * Desvía el flujo al `Human_Approval_Node`.

### Paso 10: CorrectorAgent (El Corrector)
* **Tipo de Nodo:** Nodo-Agente Simple (Especialista) | **:::agent**
* **Acción:** Se activa en el bucle "FAIL".
* **Lógica (LLM):** Recibe un prompt muy específico que contiene el error (`validator_output`) y los artefactos fallidos del estado. Genera un nuevo conjunto de artefactos corregidos.
* **Actualización de Estado:** Sobrescribe los artefactos en el estado y el flujo vuelve al **Paso 8 (Validator_Tool)** para una nueva validación.

### Paso 11: Human_Approval_Node (Parada Obligatoria) (Solo en fase de prueba)
* **Tipo de Nodo:** Nodo de Pausa (Humano) | **:::human**
* **Acción:** Se activa solo después de una validación exitosa ("PASS").
* **Lógica:** **PAUSA** la ejecución del grafo, cumpliendo el requisito de que "un humano tiene que validar el resultado final". (Esto puede ser solo para el período de prueba).
* **Interacción:** El sistema espera a que un humano actualice `state['human_approval_decision']` con "APPROVE" o "REJECT".

### Paso 12: Check Human Decision (Aprobación Final)
* **Tipo de Nodo:** Nodo-Herramienta (Enrutador Condicional) | **:::human**
* **Acción:** Se reanuda cuando `human_approval_decision` se llena.
* **Lógica:** Lee la decisión.
    * Si es "APPROVE", retorna la ruta "APPROVE".
    * Si es "REJECT", retorna la ruta "REJECT".

### Paso 13: ReporterLogger (El Auditor)
* **Tipo de Nodo:** Nodo-Agente Simple (Especialista) | **:::agent**
* **Acción:** Se activa solo en el flujo "APPROVE".
* **Lógica (LLM):** Genera el `migration_summary.md` documentando todo el proceso, la validación exitosa y la aprobación humana.
* **Actualización de Estado:** Guarda el .md en `state['migration_summary_md']`.

### Paso 14: Generator (El Desplegador)
* **Tipo de Nodo:** Nodo-Herramienta (Trabajador) | **:::tool**
* **Acción:** Se activa después del `ReporterLogger`.
* **Lógica:** Recolecta todos los artefactos aprobados (.yaml, notebooks) y el reporte (.md) del estado. Usando las `credentials` del estado, realiza un `git push` para subir estos archivos al repositorio 4.0 objetivo.
* **Salida:** El flujo termina en `END (Package Ready)`.

### Paso 15: Bucle del Manifiesto
* **Acción:** Una vez que el flujo termina (ya sea en `END (Package Ready)` o `END (Rejected by Human)`), el orquestador externo vuelve al **Paso 2** para procesar el siguiente ítem en la lista `pipelines_to_migrate` del manifiesto, repitiendo todo el proceso.

---

## Propuesta de State (GraphState)

El `GraphState` es el único objeto de datos y la fuente central de verdad para nuestro flujo de migración.

Es un diccionario de Python que contiene toda la información de un pipeline mientras se procesa: entradas, credenciales, artefactos intermedios (como el schema), todos los YAMLs generados, los reportes de validación y las decisiones humanas.

En este proyecto, el `GraphState` nos permite:

* **Comunicación:** Es la forma en que los nodos se pasan información (ej. el Extractor le pasa el schema a los Translators).
* **Control de Flujo:** Permite a los enrutadores tomar decisiones lógicas al leer su contenido (ej. "si `environment_type` es 'gld', ir a `NotebookTranslator`").
* **Paralelismo:** Habilita que los 8 Translators se ejecuten al mismo tiempo, ya que cada uno escribe en su propio campo aislado dentro del estado.
* **Robustez (Bucles):** Es lo que hace posible el bucle de auto-corrección, al persistir el `validator_output` y el `retry_count` para que el `CorrectorAgent` sepa qué arreglar.
* **Interacción Humana:** Permite que el grafo se pause (esperando que se llene `human_approval_decision`) y se reanude más tarde, habilitando la validación humana.

```python


from typing import TypedDict, List, Dict, Any, Optional

class MigrationGraphState(TypedDict):
    """
    Este es el 'Estado' central que fluye a través del grafo de migración.
    Refleja la arquitectura de varios pasos para la extracción y el enrutamiento.
    """

    # --- SECCIÓN 1: ENTRADAS INICIALES Y MANIFIESTO ---
    # Llenado al invocar el grafo
    
    manifest_path: str  # La ruta al 'manifest.yaml' que define el lote
    
    # --- SECCIÓN 2: ESTADO DEL LOTE Y PRE-VALIDACIÓN ---
    # Llenado por el nodo 'Read_Manifest_and_Check_API'
    
    credentials: Optional[Dict[str, str]]    # Credenciales de API (GitHub, Databricks)
    api_connectivity_ok: bool                # Resultado del 'pre-flight check'
    pipelines_to_migrate: List[Dict[str, Any]] # La lista de trabajo del manifiesto
    
    # El pipeline individual que se está procesando actualmente en el bucle
    current_pipeline_data: Optional[Dict[str, Any]] 

    
    # --- SECCIÓN 3: ESTADO DE EXTRACCIÓN (EN 2 PASOS) ---
    # Llenado por el 'Data_Fetcher' (Paso 2a)
    raw_artifacts_3_0: Optional[Dict[str, Any]] # {"adf_json": "...", "notebook_code": "..."}
    
    # Llenado por el 'Schema_Normalizer' (Paso 2b)
    normalized_schema_v4: Optional[Dict[str, Any]] # El JSON limpio, fuente de verdad
    environment_type: Optional[str]                # 'slv' o 'gld'

    
    # --- SECCIÓN 4: PLANTILLAS DE FRAMEWORK (HOPSFLOW/BREWTIFUL) ---
    # Llenado por el 'Framework_Loader' (Paso 3a)
    
    pipeline_template: Optional[str]         # El texto de la plantilla 'hopsflow_pipeline_template.yaml'
    transform_template: Optional[str]        # El texto de la plantilla 'hopsflow_transformations_template.yaml'
    notebook_template: Optional[str]         # El texto de la plantilla 'brewtiful_notebook_template.py'

    
    # --- SECCIÓN 5: ARTEFACTOS DE TRADUCCIÓN (SALIDAS) ---
    # Llenados en paralelo por los nodos 'Translator' (Paso 6)

    # Comunes
    acl_yaml: Optional[str]              #
    metadata_yaml: Optional[str]         #
    quality_yaml: Optional[str]          #
    sync_yaml: Optional[str]             #
    observability_yaml: Optional[str]    #
    pipeline_yaml: Optional[str]         #

    # Condicional 'slv'
    transformations_yaml: Optional[str]  #

    # Condicional 'gld' (salida de NotebookTranslator + Ruff)
    generated_notebooks: Optional[List[str]] #

    
    # --- SECCIÓN 6: VALIDACIÓN Y BUCLE DE CORRECCIÓN ---
    # Llenado por 'Validator_Tool', 'Check Validation' y 'CorrectorAgent'
    
    validator_output: Optional[str]      # El stdout/stderr crudo de 'engineeringstore --validate-dags'
    validation_passes: bool              # True/False, basado en el análisis del 'validator_output'
    retry_count: int                     # Contador para el bucle de corrección (inicia en 0)


    # --- SECCIÓN 7: APROBACIÓN HUMANA ---
    # Llenado por el 'Human_Approval_Node'
    
    human_approval_decision: Optional[str] # La decisión del humano ('APPROVE' o 'REJECT')


    # --- SECCIÓN 8: SALIDAS FINALES Y AUDITORÍA ---
    # Llenado por 'ReporterLogger' y 'Generator'
    
    migration_summary_md: Optional[str]  # El resumen de auditoría .md
    
    # El resultado del push final al repositorio 4.0
    push_status: Optional[str]           # (ej. "Éxito: commit 7a8b9c1" o "Fallo: ...")

```

