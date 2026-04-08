---
title: SQL DEBUGGER
emoji: 🚀
colorFrom: green
colorTo: gray
sdk: docker
pinned: false
license: gpl-3.0
short_description: OpenEnv-style RL environment for SQL debugging
base_path: /web
---

# SQL Debug Gym

SQL Debug Gym is a flat, OpenEnv-style RL environment for SQL debugging. It uses FastAPI plus in-memory SQLite and includes 3 graded tasks for join repair, ETL debugging, and query optimization.

## Environment Description And Motivation

The environment simulates a workflow analytics database where an agent must inspect schema context, reason about broken SQL, execute candidate fixes, and submit a final answer for grading. The benchmark is designed to measure whether an LLM agent can handle realistic SQL debugging patterns instead of toy text-only tasks.

Motivation:

- evaluate SQL repair over realistic workflow/event/referral tables
- test iterative debugging with execution feedback
- measure both correctness and efficiency

Runtime target:

- `2 vCPU`
- `8 GB RAM`
- inference finishes within the required time budget

## Action Space

Defined in [`model.py`](/d:/RL/SQL_DEBUGGER/model.py):

- `query`
  SQL statement or script to execute or submit.
- `explanation`
  Optional natural-language reasoning from the agent.
- `submit`
  Boolean flag indicating final submission for grading.

## Observation Space

Defined in [`model.py`](/d:/RL/SQL_DEBUGGER/model.py):

- `task_name`
  Active task identifier.
- `task_description`
  Natural-language description of the objective.
- `schema`
  Schema and seed data context for the task.
- `broken_artifact`
  The buggy SQL the agent is trying to repair.
- `last_query`
  Most recent query executed by the agent.
- `execution_result`
  SQL execution feedback or returned rows.
- `hint`
  Hint text unlocked during the episode.
- `step_number`
  Current step count.
- `max_steps`
  Maximum allowed steps for the task.
- `done`
  Whether the episode has ended.
- `error`
  Final error or execution issue if present.

## Tasks

Task definitions and graders are in [`tasks.py`](/d:/RL/SQL_DEBUGGER/Server/tasks.py).

| Task ID | Description | Difficulty |
|---|---|---|
| `fix_broken_join` | Repair a query with a wrong join key, invalid date boundary, and alias typo in `ORDER BY`. | Easy |
| `debug_etl_pipeline` | Repair an ETL-style queue query with incorrect domain lookup, filtering, and ranking logic. | Medium |
| `optimize_slow_query` | Rewrite a slow and logically wrong aggregate query into a correct ranked solution. | Hard |

All task rewards are validated to stay in the `0.0` to `1.0` range.

## Structure

```text
SQL_DEBUG_GYM
|-- Server
|   |-- app.py
|   |-- environment.py
|   `-- tasks.py
|-- client.py
|-- model.py
|-- inference.py
|-- Dockerfile
|-- README.md
|-- requirements.txt
|-- .env
|-- openenv.yaml
`-- validate_submission.py
```

## Setup And Usage Instructions

Install dependencies:

```cmd
python -m pip install -r requirements.txt
```

Start the environment:

```cmd
python -m uvicorn Server.app:app --host 127.0.0.1 --port 7860
```

Run inference:

```cmd
python inference.py
```

The root [`inference.py`](inference.py) auto-loads values from [`.env`](.env), runs all three tasks one by one, and emits the required `[START]`, `[STEP]`, and `[END]` structured logs.

Store output after each run (append mode):

```powershell
python inference.py *>> SQL_results.txt
```

This appends every new run to [`SQL_results.txt`](/d:/RL/SQL_DEBUGGER/SQL_results.txt) instead of overwriting previous runs.

## Baseline Score

Reference local run (April 8, 2026, from [`SQL_result.txt`](/d:/RL/SQL_DEBUGGER/SQL_result.txt)):

- Model: `Qwen/Qwen2.5-72B-Instruct`
- Tasks completed: `3/3`
- Average score: `0.6577`

Per-task baseline:

| Task ID | Score |
|---|---|
| `fix_broken_join` | `1.0000` |
| `debug_etl_pipeline` | `0.2200` |
| `optimize_slow_query` | `0.7530` |

Reproduce:

```cmd
python inference.py
```

Scoring factors (from [`tasks.py`](/d:/RL/SQL_DEBUGGER/server/tasks.py) and [`environment.py`](/d:/RL/SQL_DEBUGGER/server/environment.py)):

- `correctness` = `0.6 * row_match_score`
- `efficiency` = `0.2 * efficiency_score`
- `progress` = `0.2 * row_match_score`
- `step_bonus` = `0.02` when the submitted SQL executes without runtime error, else `0.0`
- `total` = `min(1.0, correctness + efficiency + progress + step_bonus)`

Note: your structured log format (`[STEP] ... reward=...`) records task totals, not individual factor components.  
To capture per-factor values (`correctness`, `efficiency`, `progress`, `step_bonus`), save the pretty stderr output from `inference.py`.

Note: if external model API calls are unavailable, the runner may use deterministic fallback logic (`OPENENV_USE_FALLBACK=1`), which can still produce perfect task scores.

## API Endpoints

Base URL (local): `http://127.0.0.1:7860`

- `GET /health`
  Returns service health and available task ids.
- `POST /reset`
  Starts or restarts an episode and returns a fresh observation.
  Body:
  ```json
  { "task_name": "fix_broken_join" }
  ```
- `POST /step`
  Executes one agent action (`query`, optional `explanation`, and `submit`) and returns reward/done/observation.
  Body:
  ```json
  { "query": "SELECT 1;", "explanation": "", "submit": false }
  ```
- `GET /state`
  Returns current environment state snapshot.
- `GET /web`
  Opens a simple browser UI for interactive debugging.
- `WS /ws`
  WebSocket endpoint for interactive reset/step streaming.


## Required Environment Variables

Keep these in [`.env`](.env):

- `API_BASE_URL`
- `MODEL_NAME`
- `HF_TOKEN`

Optional:

- `SQL_GYM_BASE_URL`
- `OPENENV_TASK_MODE`
- `OPENENV_USE_FALLBACK`

## Validation

Run:

```cmd
python validate_submission.py
```

The validator checks:

- `openenv.yaml`
- typed models
- `reset()`, `step()`, and `state()` endpoints
- 3+ tasks with graders
- root `inference.py`
- Docker build when a local Docker daemon is available

## Docker
Build:

```
docker build -t sql-debug-gym .
```
