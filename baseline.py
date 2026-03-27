"""
Baseline Inference Script for CRM Data Pipeline OpenEnv Environment.

Requires OPENAI_API_KEY environment variable to be set before running:
    export OPENAI_API_KEY="sk-..."   (Linux/Mac)
    $env:OPENAI_API_KEY="sk-..."    (Windows PowerShell)

DO NOT hardcode API keys in this file.
"""
import os
import json
import time
import requests
from openai import OpenAI, RateLimitError, APIError
from client import CRMDataPipelineEnvClient
from models import CRMPipelineAction, PipelineActionType

# ============================================================
# SECURITY: API key is ONLY read from environment variables.
# If missing, we raise immediately rather than silently failing.
# ============================================================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise EnvironmentError(
        "OPENAI_API_KEY is not set.\n"
        "Run: $env:OPENAI_API_KEY='sk-...' (Windows PowerShell)\n"
        "  or: export OPENAI_API_KEY='sk-...' (Linux/Mac)"
    )

openai_client = OpenAI(api_key=OPENAI_API_KEY)
MAX_STEPS_PER_TASK = 15  # Up from 6 — enough for complex T3 multi-merge pipelines

SYSTEM_PROMPT = """You are an expert CRM Data Engineer operating a data pipeline.
Your goal is to clean, deduplicate, standardize, and merge messy customer datasets.

You will be given the current state of the pipeline (objective, available sources, schema targets, and feedback from your last action).
Based on this, respond with EXACTLY one JSON action object — no other text.

Valid action_types and their required fields:
- VIEW_SOURCE: {"action_type": "VIEW_SOURCE", "source": "<name>"}
- PROFILE_SOURCE: {"action_type": "PROFILE_SOURCE", "source": "<name>"}
- STANDARDIZE_COLUMN: {"action_type": "STANDARDIZE_COLUMN", "source": "<name>", "column": "<col>", "standardization_strategy": "LOWERCASE_STRIP|EXTRACT_NUMBERS|TO_DATETIME_ISO"}
- HANDLE_MISSING: {"action_type": "HANDLE_MISSING", "source": "<name>", "column": "<col>", "missing_strategy": "DROP_ROW|FILL_VALUE", "fallback_value": "<val or null>"}
- DEDUPLICATE: {"action_type": "DEDUPLICATE", "source": "<name>", "deduplication_strategy": "EXACT_EMAIL|FUZZY_NAME_PHONE"}
- EXECUTE_SQL: {"action_type": "EXECUTE_SQL", "query": "<SQL>", "output_table": "<name>"}
- SUBMIT_PIPELINE: {"action_type": "SUBMIT_PIPELINE", "final_source": "<name>"}

Rules:
1. Always VIEW_SOURCE or PROFILE_SOURCE a new source before standardizing it.
2. SUBMIT_PIPELINE is final — only submit when the data is fully cleaned.
3. For multi-source tasks (T2, T3) use EXECUTE_SQL to JOIN or UNION sources.
4. Remove bot/outlier rows using EXECUTE_SQL with WHERE filters on customer_id or email.
"""

def build_user_prompt(obs, steps_remaining: int, task_id: str) -> str:
    return f"""
=== CRM Pipeline Agent ===
Task ID: {task_id} | Steps Remaining: {steps_remaining}
Objective: {obs.current_task_objective}

Available Sources: {obs.available_sources}
Target Schema: {json.dumps(obs.schema_target, indent=2)}

Current View (last 3 rows):
{obs.current_view}

Data Quality Report:
{obs.data_quality_report or "Not profiled yet."}

Last Action Feedback:
{obs.last_action_feedback or "None"}

Decide your next action (one JSON object only):
"""

def call_gpt_with_retry(prompt: str, max_retries: int = 3) -> dict | None:
    """Call GPT with retry logic for rate limits and transient API errors."""
    for attempt in range(max_retries):
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2,  # Low temperature for more deterministic responses
            )
            raw = resp.choices[0].message.content
            return json.loads(raw)
        except RateLimitError:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            print(f"  [GPT] Rate limit hit. Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except APIError as e:
            print(f"  [GPT] API Error on attempt {attempt + 1}: {e}")
            time.sleep(1)
        except json.JSONDecodeError as e:
            print(f"  [GPT] JSON decode failed: {e}")
            return None
    return None

def build_smart_fallback(obs, step: int, task_id: str) -> dict:
    """A smarter fallback plan the baseline runs if GPT completely fails."""
    sources = obs.available_sources or ["web_forms"]
    target = sources[0]

    fallback_pipeline = {
        "t1": [
            {"action_type": "PROFILE_SOURCE", "source": "web_forms"},
            {"action_type": "STANDARDIZE_COLUMN", "source": "web_forms", "column": "email", "standardization_strategy": "LOWERCASE_STRIP"},
            {"action_type": "STANDARDIZE_COLUMN", "source": "web_forms", "column": "name", "standardization_strategy": "LOWERCASE_STRIP"},
            {"action_type": "STANDARDIZE_COLUMN", "source": "web_forms", "column": "phone", "standardization_strategy": "EXTRACT_NUMBERS"},
            {"action_type": "STANDARDIZE_COLUMN", "source": "web_forms", "column": "signup_date", "standardization_strategy": "TO_DATETIME_ISO"},
            {"action_type": "DEDUPLICATE", "source": "web_forms", "deduplication_strategy": "EXACT_EMAIL"},
            {"action_type": "EXECUTE_SQL", "query": "SELECT * FROM web_forms WHERE customer_id != '???' AND email NOT LIKE '%bot%'", "output_table": "web_forms_clean"},
            {"action_type": "SUBMIT_PIPELINE", "final_source": "web_forms_clean"},
        ],
    }

    plan = fallback_pipeline.get(task_id, [])
    if step < len(plan):
        return plan[step]
    # Final fallback: just submit whatever is available
    return {"action_type": "SUBMIT_PIPELINE", "final_source": target}

def validate_action(payload: dict) -> CRMPipelineAction | None:
    """Validate GPT action payload. Return None if invalid to use fallback."""
    try:
        action_type_val = payload.get("action_type", "")
        if not action_type_val or action_type_val not in [e.value for e in PipelineActionType]:
            print(f"  [WARN] Invalid action_type: {action_type_val!r}")
            return None
        return CRMPipelineAction(**payload)
    except Exception as e:
        print(f"  [WARN] Action validation failed: {e}")
        return None

def run_task(task_id: str) -> float:
    base_url = os.environ.get("OPENENV_BASE_URL", "http://localhost:8080")
    print(f"\n--- Starting Baseline Inference for Task {task_id} ---")
    score = 0.0

    # Notify server which task this connection is for
    try:
        requests.post(f"{base_url}/set_task/{task_id}", timeout=5)
    except Exception as e:
        print(f"  [WARN] Could not set task on server: {e}")

    try:
        with CRMDataPipelineEnvClient(base_url=base_url).sync() as env:
            result = env.reset()
            done = False
            steps = 0

            while not done and steps < MAX_STEPS_PER_TASK:
                obs = result.observation
                steps_remaining = MAX_STEPS_PER_TASK - steps
                prompt = build_user_prompt(obs, steps_remaining, task_id)

                # Try GPT first, fall back to smart pipeline if it fails
                payload = call_gpt_with_retry(prompt)
                action = validate_action(payload) if payload else None

                if action is None:
                    print(f"  [FALLBACK] Step {steps}: using smart fallback pipeline")
                    fallback_payload = build_smart_fallback(obs, steps, task_id)
                    action = validate_action(fallback_payload)
                    if action is None:
                        break  # Safety net — cannot recover

                print(f"  Step {steps}: {action.action_type.value}")
                result = env.step(action)
                done = result.done
                steps += 1

        # Fetch final graded score — no silent fallbacks, we log the real error
        try:
            grader_res = requests.post(f"{base_url}/grader/{task_id}", timeout=10)
            grader_res.raise_for_status()
            score = grader_res.json().get("score", 0.0)
            print(f"  Final Score [{task_id}]: {score:.4f}")
        except Exception as e:
            print(f"  [ERROR] Grader endpoint failed: {e}")
            score = 0.0

    except Exception as e:
        print(f"  [ERROR] Runtime Exception in task {task_id}: {e}")
        score = 0.0

    return score


if __name__ == "__main__":
    results = {}
    for task_id in ["t1", "t2", "t3"]:
        results[task_id] = run_task(task_id)

    results["average"] = sum(results.values()) / 3
    print("\n=== Final Scores ===")
    print(json.dumps(results, indent=2))
