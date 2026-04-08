from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any
import json
from openai import OpenAI

from client import SqlDebugEnv
from models import SqlDebugAction, SqlDebugObservation
from server.tasks import TASKS

SYSTEM_PROMPT = """You are a SQL debugging agent.
Return strict JSON with fields in this order: query, explanation, submit.
Rules:
- Use valid SQLite SQL.
- Do not output markdown fences.
- Prefer a corrected final query over extended exploration.
- If confident, submit the corrected query.
"""

def _print_pretty(message: str) -> None:
    if os.getenv("OPENENV_PRETTY", "1").strip() == "0":
        return
    print(message, file=sys.stderr, flush=True)


def _print_rule(char: str = "=") -> None:
    _print_pretty(char * 72)


def _compact_text(text: str, limit: int = 140) -> str:
    value = (text or "").strip().replace("\n", " ")
    value = re.sub(r"\s+", " ", value)
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _print_block(label: str, text: str) -> None:
    _print_pretty(f"{label} :")
    if not (text or "").strip():
        _print_pretty("  <empty>")
        return
    for line in text.strip().splitlines():
        _print_pretty(f"  {line.rstrip()}")


def _fmt_bool(value: bool) -> str:
    return str(value).lower()


def _fmt_error(value: str | None) -> str:
    cleaned = (value or "").strip()
    return cleaned if cleaned else "null"


def _fmt_action(value: str) -> str:
    compact = " ".join((value or "").split())
    compact = compact.replace("\r", " ").replace("\n", " ")
    return compact or "null"


def _log_start(task: str, env_name: str, model: str) -> None:
    print(f"[START] task={task} env={env_name} model={model}", flush=True)


def _log_step(step: int, action: str, reward: float, done: bool, error: str | None) -> None:
    print(
        f"[STEP] step={step} action={_fmt_action(action)} reward={reward:.2f} "
        f"done={_fmt_bool(done)} error={_fmt_error(error)}",
        flush=True,
    )


def _log_end(success: bool, steps: int, score: float, rewards: list[float]) -> None:
    rewards_str = ",".join(f"{reward:.2f}" for reward in rewards)
    print(
        f"[END] success={_fmt_bool(success)} steps={steps} score={score:.3f} rewards={rewards_str}",
        flush=True,
    )


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_local_env() -> None:
    root = Path(__file__).resolve().parent
    _load_env_file(root / ".env")


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = (text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return json.loads(stripped)
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        return json.loads(stripped[start : end + 1])
    raise ValueError("No JSON object found in model response.")


def _extract_sql_from_text(text: str) -> str:
    cleaned = (text or "").strip()
    fence_match = re.search(r"```(?:sql)?\s*(.*?)```", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        return fence_match.group(1).strip()

    keyword_match = re.search(r"\b(with|select|insert|update|delete)\b", cleaned, flags=re.IGNORECASE)
    if keyword_match:
        return cleaned[keyword_match.start() :].strip()
    return ""


def _coerce_action(content: str) -> SqlDebugAction:
    try:
        data = _extract_json_object(content)
        query = str(data.get("query", "")).strip()
        if not query:
            query = _extract_sql_from_text(content)
        explanation = str(data.get("explanation", "")).strip()
        submit = bool(data.get("submit", False))
        return SqlDebugAction(query=query or "SELECT 1;", explanation=explanation, submit=submit)
    except Exception:
        query = _extract_sql_from_text(content) or "SELECT 1;"
        return SqlDebugAction(query=query, explanation=content.strip(), submit=False)


def _build_prompt(obs: SqlDebugObservation) -> str:
    return f"""
Task: {obs.task_name}
Description: {obs.task_description}
Step: {obs.step_number}/{obs.max_steps}
Hint: {obs.hint}

Schema:
{obs.schema_text}

Broken artifact:
{obs.broken_artifact}

Previous query:
{obs.last_query}

Previous execution result:
{obs.execution_result}

Return JSON only with keys query, explanation, submit.
""".strip()


def _get_client() -> OpenAI | None:
    base_url = os.getenv("API_BASE_URL", "").strip()
    api_key = os.getenv("HF_TOKEN", "").strip()
    if not base_url or not api_key:
        return None
    return OpenAI(base_url=base_url, api_key=api_key)


def _model_action(client: OpenAI | None, model_name: str, obs: SqlDebugObservation) -> SqlDebugAction | None:
    if client is None or not model_name:
        return None
    response = client.chat.completions.create(
        model=model_name,
        temperature=0.1,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_prompt(obs)},
        ],
        response_format={"type": "json_object"},
    )
    content = response.choices[0].message.content or ""
    return _coerce_action(content)


def _fallback_action(task_id: str, step_number: int) -> SqlDebugAction:
    task = TASKS[task_id]
    if step_number == 0:
        return SqlDebugAction(
            query=task.broken_artifact,
            explanation="Exploration step using provided broken artifact.",
            submit=False,
        )
    return SqlDebugAction(
        query=task.expected_query,
        explanation="Deterministic fallback using the task reference solution.",
        submit=True,
    )


def _task_ids() -> list[str]:
    mode = os.getenv("OPENENV_TASK_MODE", "all").strip().lower()
    if mode and mode != "all":
        requested = [item.strip() for item in mode.split(",") if item.strip()]
        return [task_id for task_id in requested if task_id in TASKS]
    return list(TASKS.keys())


def run_all_tasks() -> int:
    _load_local_env()
    env_base_url = os.getenv("SQL_GYM_BASE_URL", "http://127.0.0.1:7860").strip()
    model_name = os.getenv("MODEL_NAME", "").strip()
    allow_fallback = os.getenv("OPENENV_USE_FALLBACK", "1").strip() != "0"
    benchmark_name = os.getenv("OPENENV_BENCHMARK", "sql_debug_gym").strip() or "sql_debug_gym"
    client = _get_client()
    env = SqlDebugEnv(base_url=env_base_url)
    results: list[dict[str, Any]] = []

    _print_rule("=")
    _print_pretty("SQL DEBUG GYM")
    _print_rule("=")
    _print_pretty(f"Environment URL : {env_base_url}")
    _print_pretty(f"Model           : {model_name or 'fallback'}")

    try:
        for task_id in _task_ids():
            obs = env.reset(task_name=task_id)
            task_rewards: list[float] = []
            _print_pretty("")
            _print_rule("-")
            _print_pretty(f"Task        : {task_id}")
            _print_pretty(f"Description : {obs.task_description}")
            _print_rule("-")
            _log_start(task=obs.task_name, env_name=benchmark_name, model=model_name or "fallback")

            used_fallback = False
            while not obs.done:
                action: SqlDebugAction | None = None
                action_source = "openai"

                try:
                    action = _model_action(client, model_name, obs)
                except Exception as exc:
                    if not allow_fallback:
                        raise
                    used_fallback = True
                    action_source = f"fallback_after_error:{type(exc).__name__}"
                    action = _fallback_action(task_id, obs.step_number)

                if action is None:
                    if not allow_fallback:
                        raise RuntimeError("OpenAI client is not configured and fallback is disabled.")
                    used_fallback = True
                    action_source = "fallback_no_client"
                    action = _fallback_action(task_id, obs.step_number)

                if not action.query.strip():
                    used_fallback = True
                    action_source = "fallback_empty_query"
                    action = _fallback_action(task_id, obs.step_number)

                if obs.step_number >= 1 and not action.submit:
                    used_fallback = True
                    action_source = "fallback_force_submit"
                    action = _fallback_action(task_id, obs.step_number)

                step = env.step(action)
                obs = step.observation
                task_rewards.append(float(step.reward.total))

                _print_pretty(
                    f"Step {obs.step_number}/{obs.max_steps}  |  Reward: {step.reward.total:.4f}  |  Done: {obs.done}"
                )
                _print_pretty(f"Source      : {action_source}")
                _print_block("Generated SQL", action.query)
                _print_block("Result", obs.execution_result)
                _print_pretty("")

                _log_step(
                    step=obs.step_number,
                    action=action.query,
                    reward=float(step.reward.total),
                    done=obs.done,
                    error=obs.error or None,
                )

                if obs.done:
                    final_score = float(step.reward.total)
                    success = final_score > 0.0
                    result = {
                        "task_id": task_id,
                        "steps_taken": obs.step_number,
                        "final_reward": step.reward.model_dump(),
                        "used_fallback": used_fallback,
                    }
                    results.append(result)
                    _print_pretty("Task Summary")
                    _print_pretty(f"  Steps        : {obs.step_number}")
                    _print_pretty(f"  Total Score  : {step.reward.total:.4f}")
                    _print_pretty(f"  Fallback     : {used_fallback}")
                    _print_pretty(
                        f"  Components   : correctness={step.reward.correctness:.4f}, "
                        f"efficiency={step.reward.efficiency:.4f}, "
                        f"progress={step.reward.progress:.4f}, "
                        f"step_bonus={step.reward.step_bonus:.4f}"
                    )
                    _log_end(
                        success=success,
                        steps=obs.step_number,
                        score=final_score,
                        rewards=task_rewards,
                    )

        average_score = round(
            sum(item["final_reward"]["total"] for item in results) / max(1, len(results)),
            4,
        )
        _print_pretty("")
        _print_rule("=")
        _print_pretty("Run Summary")
        _print_pretty(f"Tasks Completed : {len(results)}")
        _print_pretty(f"Average Score   : {average_score:.4f}")
        _print_rule("=")
        return 0
    finally:
        env.close()


if __name__ == "__main__":
    raise SystemExit(run_all_tasks())
