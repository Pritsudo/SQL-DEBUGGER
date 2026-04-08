from __future__ import annotations

from typing import Any

import httpx

from models import SqlDebugAction, SqlDebugObservation, StepResponse


class SqlDebugEnv:
    def __init__(self, base_url: str = "http://localhost:7860", timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._sync_client = httpx.Client(base_url=self.base_url, timeout=self.timeout)
        self._async_client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)

    def close(self) -> None:
        self._sync_client.close()

    async def aclose(self) -> None:
        await self._async_client.aclose()

    def reset(self, task_name: str | None = None) -> SqlDebugObservation:
        payload: dict[str, Any] = {}
        if task_name:
            payload["task_name"] = task_name
        resp = self._sync_client.post("/reset", json=payload)
        resp.raise_for_status()
        data = resp.json()["observation"]
        return SqlDebugObservation(**data)

    def step(self, action: SqlDebugAction) -> StepResponse:
        resp = self._sync_client.post("/step", json=action.model_dump())
        resp.raise_for_status()
        return StepResponse(**resp.json())

    def state(self) -> dict:
        resp = self._sync_client.get("/state")
        resp.raise_for_status()
        return resp.json()

    async def areset(self, task_name: str | None = None) -> SqlDebugObservation:
        payload: dict[str, Any] = {}
        if task_name:
            payload["task_name"] = task_name
        resp = await self._async_client.post("/reset", json=payload)
        resp.raise_for_status()
        data = resp.json()["observation"]
        return SqlDebugObservation(**data)

    async def astep(self, action: SqlDebugAction) -> StepResponse:
        resp = await self._async_client.post("/step", json=action.model_dump())
        resp.raise_for_status()
        return StepResponse(**resp.json())
