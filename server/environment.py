from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

from models import SqlDebugAction, SqlDebugObservation, SqlDebugReward, StepResponse
from server.tasks import TASKS, TaskSpec

MIN_STRICT_SCORE = 0.1
MAX_STRICT_SCORE = 0.9


@dataclass
class EpisodeState:
    task: TaskSpec
    conn: sqlite3.Connection
    step_number: int
    last_query: str
    execution_result: str
    done: bool
    error: str
    final_reward: SqlDebugReward


class SqlDebugEnvironment:
    def __init__(self, default_task: str | None = None) -> None:
        self.default_task = default_task or os.getenv("SQL_DEBUG_TASK", "fix_broken_join")
        if self.default_task not in TASKS:
            self.default_task = "fix_broken_join"
        self._state: EpisodeState | None = None

    @property
    def state(self) -> EpisodeState:
        if self._state is None:
            self.reset(self.default_task)
        return self._state  # type: ignore[return-value]

    def _build_conn(self, task: TaskSpec) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.executescript(task.schema_sql)
        conn.executescript(task.seed_sql)
        return conn

    def reset(self, task_name: str | None = None) -> SqlDebugObservation:
        chosen = task_name or self.default_task
        if chosen not in TASKS:
            chosen = self.default_task
        task = TASKS[chosen]

        if self._state is not None:
            self._state.conn.close()

        self._state = EpisodeState(
            task=task,
            conn=self._build_conn(task),
            step_number=0,
            last_query="",
            execution_result="No query executed yet.",
            done=False,
            error="",
            final_reward=SqlDebugReward(),
        )
        return self._observation()

    def _hint_for_step(self) -> str:
        state = self.state
        if state.step_number == 0:
            return "Hints unlock every 3 steps."
        idx = min((state.step_number // 3) - 1, len(state.task.hints) - 1)
        if idx < 0:
            return "Hints unlock every 3 steps."
        return state.task.hints[idx]

    def _observation(self) -> SqlDebugObservation:
        s = self.state
        return SqlDebugObservation(
            task_name=s.task.name,
            task_description=s.task.description,
            schema=s.task.full_schema,
            broken_artifact=s.task.broken_artifact,
            last_query=s.last_query,
            execution_result=s.execution_result,
            hint=self._hint_for_step(),
            step_number=s.step_number,
            max_steps=s.task.max_steps,
            done=s.done,
            error=s.error,
        )

    def _run_query(self, query: str) -> tuple[bool, str]:
        cur = self.state.conn.cursor()
        try:
            cur.execute(query)
            if cur.description is None:
                self.state.conn.commit()
                return True, "Query executed successfully (no result set)."
            rows = cur.fetchall()
            if not rows:
                return True, "Query executed successfully. Returned 0 rows."
            preview = rows[:20]
            return True, f"Rows ({len(rows)} total): {preview}"
        except sqlite3.ProgrammingError as exc:
            # Allow multi-statement submissions (e.g., ETL pipelines) via executescript.
            if "one statement at a time" in str(exc).lower():
                try:
                    self.state.conn.executescript(query)
                    self.state.conn.commit()
                    return True, "Script executed successfully."
                except Exception as script_exc:
                    return False, f"{type(script_exc).__name__}: {script_exc}"
            return False, f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    def step(self, action: SqlDebugAction) -> StepResponse:
        s = self.state
        if s.done:
            return StepResponse(observation=self._observation(), reward=s.final_reward)

        s.step_number += 1
        s.last_query = action.query
        s.error = ""

        ok, result_text = self._run_query(action.query)
        s.execution_result = result_text
        step_bonus = 0.02 if ok else 0.0

        should_submit = action.submit or s.step_number >= s.task.max_steps
        if should_submit:
            s.done = True
            graded = s.task.grader(action.query, s.task.name, s.step_number, s.task.max_steps)
            graded.step_bonus = round(step_bonus, 4)
            strict_total = max(MIN_STRICT_SCORE, min(MAX_STRICT_SCORE, graded.total + step_bonus))
            graded.total = round(strict_total, 4)
            s.final_reward = graded
            if not ok:
                s.error = "Final submission had SQL execution error."
            return StepResponse(observation=self._observation(), reward=graded)

        reward = SqlDebugReward(
            total=round(step_bonus, 4),
            correctness=0.0,
            efficiency=0.0,
            progress=0.0,
            step_bonus=round(step_bonus, 4),
        )
        return StepResponse(observation=self._observation(), reward=reward)

    def get_state(self) -> dict:
        s = self.state
        return {
            "task_name": s.task.name,
            "step_number": s.step_number,
            "max_steps": s.task.max_steps,
            "done": s.done,
            "final_reward": s.final_reward.model_dump(),
        }
