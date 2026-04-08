from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from model import SqlDebugAction, SqlDebugObservation, SqlDebugReward
from server.app import app
from server.tasks import TASKS

ROOT = Path(__file__).resolve().parent


def check(condition: bool, label: str) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}")
    if not condition:
        raise SystemExit(1)


def validate_openenv_yaml() -> None:
    path = ROOT / "openenv.yaml"
    check(path.exists(), "root openenv.yaml exists")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    check(isinstance(data, dict), "openenv.yaml parses as mapping")
    check(data.get("entrypoint") == "server.app:app", "entrypoint matches app")
    check(data.get("type") == "http", "OpenEnv type is http")
    check(str(data.get("spec_version")) == "1.0", "spec version is 1.0")
    tasks = data.get("tasks", [])
    check(isinstance(tasks, list) and len(tasks) >= 3, "three or more tasks declared")


def validate_models() -> None:
    check(hasattr(SqlDebugAction, "model_json_schema"), "action model is typed")
    check(hasattr(SqlDebugObservation, "model_json_schema"), "observation model is typed")
    check(hasattr(SqlDebugReward, "model_json_schema"), "reward model is typed")


def validate_endpoints() -> None:
    with TestClient(app) as client:
        health = client.get("/health")
        check(health.status_code == 200, "/health returns 200")
        tasks = health.json().get("tasks", [])
        check(len(tasks) >= 3, "/health enumerates tasks")

        reset = client.post("/reset", json={"task_name": "fix_broken_join"})
        check(reset.status_code == 200, "/reset returns 200")
        observation = reset.json()["observation"]
        check(observation["task_name"] == "fix_broken_join", "/reset selects requested task")

        step = client.post(
            "/step",
            json={"query": "SELECT 1;", "explanation": "validator probe", "submit": False},
        )
        check(step.status_code == 200, "/step returns 200")

        state = client.get("/state")
        check(state.status_code == 200, "/state returns 200")


def validate_graders() -> None:
    check(len(TASKS) >= 3, "three or more graders available")
    for name, task in TASKS.items():
        reward = task.grader(task.expected_query, task.name, 1, task.max_steps)
        values = reward.model_dump()
        bounded = all(0.0 <= float(value) <= 1.0 for value in values.values())
        check(bounded, f"{name} reward fields are within 0.0-1.0")


def validate_inference_script() -> None:
    path = ROOT / "inference.py"
    check(path.exists(), "root inference.py exists")
    module = importlib.import_module("inference")
    check(hasattr(module, "run_all_tasks"), "root inference.py exposes run_all_tasks")


def validate_docker_build() -> None:
    if shutil.which("docker") is None:
        print("[SKIP] docker not installed locally; skipped docker build check")
        return
    probe = subprocess.run(
        ["docker", "info"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        print("[SKIP] docker daemon unavailable locally; skipped docker build check")
        return
    result = subprocess.run(
        ["docker", "build", "-t", "sql-debug-gym-validate", "."],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    check(result.returncode == 0, "docker build succeeds")


def main() -> int:
    validate_openenv_yaml()
    validate_models()
    validate_endpoints()
    validate_graders()
    validate_inference_script()
    validate_docker_build()
    print("[PASS] submission validation complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
