---
title: CRM Data Pipeline
emoji: ⚙️
colorFrom: blue
colorTo: green
sdk: docker
app_port: 8080
pinned: false
---

# OpenEnv CRM Data Pipeline Benchmark

## 1. Abstract

The CRM Data Pipeline Environment is a highly specialized, programmatic testbed designed to evaluate the reasoning, planning, and code-generation capabilities of autonomous agents. The environment simulates a rigorous enterprise data engineering workload: retrieving, profiling, standardizing, and merging heterogeneous customer databases (Salesforce, Web Leads, Legacy Databases) into a pristine, unified schema.

This environment strictly adheres to the OpenEnv Specification, exposing standardized `step()`, `reset()`, and `state()` endpoints across an isolated HTTP interface.

---

## 2. Environment Description

Real-world customer relationship management (CRM) data is inherently unstructured and noisy. Agents interacting with this environment must solve challenges involving conflicting database schemas, inconsistent formatting (e.g., lowercase normalization, ISO-8601 timestamps, E.164 phone formats), missing values, and record duplication.

The environment utilizes the `pandas` underlying execution engine to securely evaluate data mutations in memory. It tracks the pipeline modifications sequentially and restricts destructive queries, forcing the agent to demonstrate deterministic data manipulation.

---

## 3. Action Space

The action space is a strictly typed JSON payload adhering to the `CRMPipelineAction` Pydantic schema. The agent can dispatch the following operations via the `step()` endpoint:

* **`VIEW_SOURCE`**: Retrieves a textual representation of the specified input source dataset (head rows).
* **`PROFILE_SOURCE`**: Generates a statistical data quality report for the target source, identifying missing values, inferred types, and uniqueness constraints.
* **`STANDARDIZE_COLUMN`**: Applies a deterministic transformation to a specific column. Supported strategies include:
  * `LOWERCASE_STRIP`: Lowercase projection with whitespace trimming.
  * `TO_DATETIME_ISO`: Parses arbitrary date strings into ISO-8601 formatting.
  * `EXTRACT_NUMBERS`: Retains only numeric characters (e.g., standardizing phone numbers).
* **`HANDLE_MISSING`**: Resolves NaN/Null fields using imputation (`FILL_VALUE`) or row deletion (`DROP_ROW`).
* **`DEDUPLICATE`**: Implements logical deduplication based on strict criteria (e.g., `EXACT_EMAIL`).
* **`EXECUTE_SQL`**: Allows the agent to construct custom SQLite queries to perform complex joins or unions. The environment rigidly parses the syntax to prevent SQL injection and state poisoning (blocking `DROP` or `DELETE` operators).
* **`SUBMIT_PIPELINE`**: Concludes the episode, signaling the grader to evaluate the final specified output dataframe.

---

## 4. Observation Space

Upon initialization or after successfully processing an action, the environment returns a `CRMPipelineObservation` object detailing the current Markov Decision Process (MDP) state:

* **`current_task_objective`** *(string)*: Formal declaration of the agent's goal for the episode.
* **`schema_target`** *(dict)*: The expected column topology for the final output table.
* **`available_sources`** *(list)*: Enumeration of currently available input tables.
* **`current_view`** *(string)*: A rendered Markdown table representing the state of the active source dataset.
* **`data_quality_report`** *(string)*: Output statistics stemming from a prior `PROFILE_SOURCE` action.
* **`last_action_feedback`** *(string)*: Detailed status or stack trace derived from the execution of the previous step.

---

## 5. Reward Design

To facilitate active reinforcement learning, the environment implements a robust dense reward signal system:

* **Heuristic Progress (+0.03 to +0.05)**: Awarded sequentially for successful intermediate cleaning actions (e.g., proper execution of `STANDARDIZE_COLUMN` or `DEDUPLICATE`) based on the delta of row conformity.
* **Destructive Operation Penalties (-0.1 to -0.5)**: Deducted for syntax errors in `EXECUTE_SQL`, or for attempting premature submission (failing the `MIN_STEPS` threshold constraint).
* **Terminal Heuristic Bonus (+0.2 max)**: Awarded at terminal state representing the ratio of output columns matching the required `schema_target`.

*Note: The programmatic Grader score (0.0 to 1.0) is mathematically isolated from this environment step-reward and represents the ultimate validity of the output dataset.*

---

## 6. Task Formulation & Evaluation

The environment configures three discrete tasks, increasing geometrically in computational complexity. Evaluation is deterministic: the agent's terminal dataset is strictly compared cell-by-cell against a sequestered `Ground Truth` dataset generated at initialization.

### Task 1: Web Forms Normalization (Easy)
* **Goal**: Process a single `web_forms` dataset.
* **Mechanics**: The agent must identify date formats, normalize email casings, and strip arbitrary whitespace anomalies.

### Task 2: Legacy DB Deduplication (Medium)
* **Goal**: Merge `web_forms` and `legacy_db` sources.
* **Mechanics**: Requires data normalization across two distinct tables followed by a complex deduplication strategy.

### Task 3: 3-Way Source Merge (Hard)
* **Goal**: Unify `salesforce`, `web_leads`, and `legacy_db` silos.
* **Mechanics**: Forces the agent to utilize advanced `EXECUTE_SQL` joins, resolve conflicting primary keys, and dynamically drop bot-injected rows before final compilation.

---

## 7. System Architecture Setup

### Prerequisites
* Python 3.10, 3.11, or 3.12
* Docker (for isolated evaluation)
* OpenEnv Core Framework (`pip install openenv-core`)

### Environment Variables
Configure `.env` prior to running the baseline script:
```env
HF_TOKEN=your_authentication_token
API_BASE_URL=your_llm_inference_endpoint
MODEL_NAME=your_target_model
```

### Local Execution (Python)
Launch the FastAPI backend server on port 8080:
```bash
python -m server.app
```

### Containerized Execution (Docker)
Build and run the verified OpenEnv container:
```bash
docker build -t crm-env .
docker run -p 8080:8080 crm-env
```

---

## 8. Baseline Inference

The repository includes `inference.py`, demonstrating a deterministic rule-based heuristic fallback alongside a standard OpenAI API caller loop.

Execute the baseline directly against the local server to verify execution boundaries:
```bash
python inference.py
```

### Reference Baseline Scores
Executing the inference script yields the following verifiable grade metrics:
* **Task 1 (Easy):** 1.000 / 1.000
* **Task 2 (Medium):** 1.000 / 1.000
* **Task 3 (Hard):** 1.000 / 1.000
