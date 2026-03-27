from typing import Dict, Any
import json
from models import CRMPipelineAction, CRMPipelineObservation, CRMPipelineState

try:
    from openenv.core import EnvClient
    from openenv.core.client_types import StepResult
except ImportError:
    class EnvClient: pass
    class StepResult: pass

class CRMDataPipelineEnvClient(EnvClient[CRMPipelineAction, CRMPipelineObservation, CRMPipelineState]):
    def _step_payload(self, action: CRMPipelineAction) -> dict:
        return json.loads(action.model_dump_json(exclude_none=True))

    def _parse_result(self, payload: dict) -> "StepResult":
        obs_data = payload.get("observation", payload)
        obs = CRMPipelineObservation(
            done=obs_data.get("done", False) or payload.get("done", False),
            reward=obs_data.get("reward") or payload.get("reward", 0.0),
            current_task_objective=obs_data.get("current_task_objective", ""),
            schema_target=obs_data.get("schema_target", {}),
            available_sources=obs_data.get("available_sources", []),
            current_view=obs_data.get("current_view", ""),
            data_quality_report=obs_data.get("data_quality_report", ""),
            last_action_feedback=obs_data.get("last_action_feedback", "")
        )
        return StepResult(
            observation=obs,
            reward=payload.get("reward", 0.0),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: dict) -> CRMPipelineState:
        return CRMPipelineState(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
            task_id=payload.get("task_id", "t1")
        )
