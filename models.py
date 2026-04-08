from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

class SqlDebugAction(BaseModel):
    query: str = Field(..., description="SQL to execute or submit")
    explanation: str = Field(default="", description="Optional reasoning")
    submit: bool = Field(default=False, description="If true, trigger grading")


class SqlDebugObservation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    task_name: str
    task_description: str
    schema_text: str = Field(
        ...,
        alias="schema",
        serialization_alias="schema",
        description="CREATE TABLE and INSERT statements for the active task",
    )
    broken_artifact: str
    last_query: str
    execution_result: str
    hint: str
    step_number: int
    max_steps: int
    done: bool
    error: str


class SqlDebugReward(BaseModel):
    total: float = 0.0
    correctness: float = 0.0
    efficiency: float = 0.0
    progress: float = 0.0
    step_bonus: float = 0.0


class StepResponse(BaseModel):
    observation: SqlDebugObservation
    reward: SqlDebugReward
