from __future__ import annotations

import json
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

from models import SqlDebugAction, StepResponse
from server.environment import SqlDebugEnvironment
from server.tasks import TASKS

app = FastAPI(title="SQL Debug Gym", version="0.1.0")
env = SqlDebugEnvironment(default_task=os.getenv("SQL_DEBUG_TASK", "fix_broken_join"))


class ResetRequest(BaseModel):
    task_name: str | None = None


@app.get("/health")
def health() -> dict:
    return {"status": "healthy", "tasks": list(TASKS.keys())}


@app.post("/reset")
def reset(req: ResetRequest | None = None) -> dict:
    obs = env.reset(req.task_name if req else None)
    return {"observation": obs.model_dump()}


@app.post("/step")
def step(action: SqlDebugAction) -> StepResponse:
    return env.step(action)


@app.get("/state")
def state() -> dict:
    return env.get_state()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    try:
        await ws.send_json({"type": "observation", "payload": env.reset().model_dump()})
        while True:
            data = await ws.receive_text()
            payload = json.loads(data)
            if payload.get("type") == "reset":
                obs = env.reset(payload.get("task_name"))
                await ws.send_json({"type": "observation", "payload": obs.model_dump()})
                continue
            action = SqlDebugAction(**payload)
            resp = env.step(action)
            await ws.send_json({"type": "step", "payload": resp.model_dump()})
    except WebSocketDisconnect:
        return


@app.get("/web", response_class=HTMLResponse)
def web_ui() -> str:
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SQL Debug Gym</title>
  <style>
    body { font-family: Consolas, monospace; background: #f5f7fb; margin: 0; padding: 24px; }
    .wrap { max-width: 1000px; margin: auto; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    textarea { width: 100%; height: 220px; }
    pre { background: #111827; color: #e5e7eb; padding: 12px; border-radius: 8px; white-space: pre-wrap; }
    button { padding: 8px 12px; margin-right: 8px; }
    select { padding: 8px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h2>SQL Debug Gym</h2>
    <div>
      <select id="task">
        <option value="fix_broken_join">fix_broken_join</option>
        <option value="debug_etl_pipeline">debug_etl_pipeline</option>
        <option value="optimize_slow_query">optimize_slow_query</option>
      </select>
      <button onclick="resetEnv()">Reset</button>
      <button onclick="submitStep(false)">Run Query</button>
      <button onclick="submitStep(true)">Submit Final</button>
    </div>
    <div class="row">
      <div>
        <h3>SQL</h3>
        <textarea id="query"></textarea>
      </div>
      <div>
        <h3>Observation</h3>
        <pre id="obs"></pre>
      </div>
    </div>
  </div>
  <script>
    const obsEl = document.getElementById("obs");
    const queryEl = document.getElementById("query");

    async function resetEnv() {
      const task_name = document.getElementById("task").value;
      const r = await fetch("/reset", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({task_name})
      });
      const data = await r.json();
      obsEl.textContent = JSON.stringify(data.observation, null, 2);
      queryEl.value = data.observation.broken_artifact;
    }

    async function submitStep(submit) {
      const r = await fetch("/step", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({query: queryEl.value, explanation: "", submit})
      });
      const data = await r.json();
      obsEl.textContent = JSON.stringify(data, null, 2);
    }

    resetEnv();
  </script>
</body>
</html>
"""


def main() -> None:
    port = int(os.getenv("PORT", "7860"))
    uvicorn.run("server.app:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
