"""TaskManager — outer orchestration loop.

Responsibilities (TaskManager only):
    - Decomposition: split a user prompt into sub-tasks by capability
    - Retry: re-submit a failed sub-task to CKCEngine (which re-discovers)
    - Aggregation: merge sub-task results into one final result
    - Budget / depth guard
    - MongoDB task_memory writes

CKCEngine handles the inner single-step pipeline:
    discovery -> scoring -> execute_task (LLM call)
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from .capabilities import CAPABILITY_DEFINITIONS, GENERAL_TASK, normalize_capability_name
from .routing import infer_primary_capability_details

log = logging.getLogger("ckc.orchestrator")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskDecomposer:
    """Plan conservative sub-tasks after primary capability inference.

    Primary capability inference happens once upstream. Decomposition is
    downstream and guided by that result. It should only split when there is
    strong evidence of multiple ordered intents, and otherwise preserve the
    primary capability as the single execution plan.
    """

    _CONNECTOR_PATTERN = re.compile(r"\b(and then|then|after|based on|and)\b", re.IGNORECASE)
    _LEADING_FILLER_PATTERN = re.compile(r"^(and then|then|after that|after|based on|and)\s+", re.IGNORECASE)

    @staticmethod
    def _detect_segment_capability(segment: str) -> str | None:
        segment_lower = segment.lower()
        matched_capabilities: list[str] = []
        for definition in CAPABILITY_DEFINITIONS:
            capability_name = definition.capability_name
            if capability_name == GENERAL_TASK:
                continue
            for kw in definition.routing_keywords:
                if re.search(r"\b" + re.escape(kw) + r"\b", segment_lower):
                    matched_capabilities.append(capability_name)
                    break
        if len(matched_capabilities) == 1:
            return matched_capabilities[0]
        return None

    @classmethod
    def decompose(
        cls,
        task_prompt: str,
        primary_capability: str,
        low_confidence: bool = False,
    ) -> list[dict[str, Any]]:
        """Return an ordered, conservative sub-task plan.

        Primary capability inference is authoritative. Decomposition only splits
        when the prompt shows clear multi-step intent; otherwise it returns a
        single task using the provided primary capability.
        """
        normalized_primary = normalize_capability_name(primary_capability, fallback=GENERAL_TASK)
        default_subtask = [{
            "capability": normalized_primary,
            "prompt_segment": task_prompt.strip(),
            "order": 0,
            "parent_context": task_prompt.strip(),
        }]

        connectors = [match.group(0).lower() for match in cls._CONNECTOR_PATTERN.finditer(task_prompt)]
        raw_segments = [segment.strip(" ,.;:\n\t") for segment in cls._CONNECTOR_PATTERN.split(task_prompt) if segment.strip(" ,.;:\n\t")]

        if low_confidence or len(raw_segments) < 2 or not connectors:
            log.info(
                "[TaskDecomposer] decision=no_split primary=%s low_confidence=%s detected_intents=%s sub_tasks=%s",
                normalized_primary,
                low_confidence,
                [],
                default_subtask,
            )
            return default_subtask

        detected_intents: list[dict[str, str | int]] = []
        sub_tasks: list[dict[str, Any]] = []
        seen_capabilities: set[str] = set()
        for idx, raw_segment in enumerate(raw_segments):
            cleaned_segment = cls._LEADING_FILLER_PATTERN.sub("", raw_segment).strip(" ,.;:\n\t")
            if not cleaned_segment:
                continue
            capability = cls._detect_segment_capability(cleaned_segment)
            if capability is None:
                continue
            capability = normalize_capability_name(capability, fallback=GENERAL_TASK)
            if capability == GENERAL_TASK or capability in seen_capabilities:
                continue
            seen_capabilities.add(capability)
            detected_intents.append({"capability": capability, "segment": cleaned_segment, "order": idx})
            sub_tasks.append(
                {
                    "capability": capability,
                    "prompt_segment": cleaned_segment,
                    "order": len(sub_tasks),
                    "parent_context": task_prompt.strip(),
                }
            )

        if len(sub_tasks) < 2:
            log.info(
                "[TaskDecomposer] decision=no_split primary=%s low_confidence=%s detected_intents=%s sub_tasks=%s",
                normalized_primary,
                low_confidence,
                detected_intents,
                default_subtask,
            )
            return default_subtask

        log.info(
            "[TaskDecomposer] decision=split primary=%s low_confidence=%s detected_intents=%s sub_tasks=%s",
            normalized_primary,
            low_confidence,
            detected_intents,
            sub_tasks,
        )
        return sub_tasks


# ------------------------------------------------------------------
# Result Aggregator
# ------------------------------------------------------------------

class ResultAggregator:
    """Merge sub-task results."""

    @staticmethod
    def aggregate_sequential(results: list[dict[str, Any]], execution_context: dict[str, Any]) -> dict[str, Any]:
        combined_sections: list[str] = []
        total_cost = 0.0
        total_latency = 0.0
        total_tokens = 0
        agents_used: list[str] = []
        step_outputs: list[dict[str, Any]] = []
        non_empty_outputs: list[str] = []

        for idx, r in enumerate(results, start=1):
            inner = r.get("result", {})
            output = inner.get("output") or inner.get("summary", "")
            output_text = str(output).strip()
            if output_text:
                # Keep a plain combined transcript for debugging/transparency, but do not
                # prefix the user-facing final output with "Step N". In single-step flows
                # especially, users should see the actual result directly.
                combined_sections.append(output_text)
                non_empty_outputs.append(output_text)
            total_cost += float(inner.get("cost_usd", 0.0))
            total_latency += float(inner.get("latency_ms", 0.0))
            total_tokens += int(inner.get("tokens_used", 0))
            if r.get("selected_agent"):
                agents_used.append(r["selected_agent"])
            step_outputs.append(
                {
                    "step": idx,
                    "capability": r.get("selected_capability"),
                    "agent": r.get("selected_agent"),
                    "state": r.get("state"),
                    "summary": inner.get("summary", ""),
                    "output": output_text,
                }
            )

        combined_output = "\n\n".join(combined_sections)
        # The last non-empty step is the workflow's effective final result because
        # downstream steps consume prior outputs and produce the user-facing deliverable.
        final_synthesized_output = non_empty_outputs[-1] if non_empty_outputs else ""

        return {
            "combined_output": combined_output,
            "final_synthesized_output": final_synthesized_output,
            "step_outputs": step_outputs,
            "execution_context": execution_context,
            "agents_used": agents_used,
            "total_cost_usd": round(total_cost, 6),
            "total_latency_ms": round(total_latency, 1),
            "total_tokens_used": total_tokens,
            "sub_task_count": len(results),
        }


# ------------------------------------------------------------------
# Task Manager
# ------------------------------------------------------------------

class TaskManager:
    """Outer orchestration loop.

    Owns: decomposition, sub-task loop, retry, aggregation, budget, memory.
    Delegates per sub-task to: engine.submit_task()
    """

    MAX_RETRIES_PER_SUBTASK = 2

    def __init__(self, engine: Any, mongo_db: Any = None) -> None:
        self._engine = engine
        self._mongo_db = mongo_db

    def run(
        self,
        task_prompt: str,
        user_id: str,
        *,
        max_depth: int = 3,
        budget_usd: float = 0.30,
        llm_config: dict[str, Any] | None = None,
        required_capability: str | None = None,
        routing_plan: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        orchestration_id = str(uuid.uuid4())
        log.info("[TaskManager] orchestration=%s prompt=%r", orchestration_id, task_prompt[:80])
        if routing_plan:
            primary_capability = normalize_capability_name(
                routing_plan.get("primary_capability") or required_capability or "",
                fallback=GENERAL_TASK,
            )
            low_confidence = bool(routing_plan.get("low_confidence", False))
            sub_tasks = list(routing_plan.get("sub_tasks") or [])
        elif required_capability:
            primary_capability = normalize_capability_name(required_capability, fallback=GENERAL_TASK)
            low_confidence = False
            sub_tasks = TaskDecomposer.decompose(task_prompt, primary_capability, low_confidence=low_confidence)
        else:
            inference = infer_primary_capability_details(task_prompt)
            primary_capability = inference.capability
            low_confidence = inference.low_confidence
            sub_tasks = TaskDecomposer.decompose(task_prompt, primary_capability, low_confidence=low_confidence)
        log.info("[TaskManager] decomposed into %d sub-task(s): %s",
                 len(sub_tasks), [s["capability"] for s in sub_tasks])

        results: list[dict[str, Any]] = []
        execution_log: list[dict[str, Any]] = []
        spent_usd = 0.0
        execution_context: dict[str, Any] = {
            "original_prompt": task_prompt,
            "primary_capability": primary_capability,
            "steps": [],
        }
        planned_steps_by_order = {
            int(step.get("order", idx)): step
            for idx, step in enumerate((routing_plan or {}).get("selected_agent_per_step", []))
            if isinstance(step, dict)
        }
        planned_rankings_by_order = {
            int(step.get("order", idx)): step
            for idx, step in enumerate((routing_plan or {}).get("candidate_rankings_per_step", []))
            if isinstance(step, dict)
        }

        execution_log.append({
            "step_type": "decomposition",
            "description": f"Decomposed into {len(sub_tasks)} sub-task(s): {', '.join(s['capability'] for s in sub_tasks)}",
            "status": "success",
            "timestamp": _now_iso(),
        })

        for sub in sub_tasks:
            if spent_usd >= budget_usd:
                log.warning("[TaskManager] budget exceeded (%.4f >= %.4f), stopping", spent_usd, budget_usd)
                execution_log.append({
                    "step_type": "budget_exceeded",
                    "capability": sub["capability"],
                    "description": f"Budget limit ${budget_usd:.2f} reached (spent ${spent_usd:.4f}). Skipping remaining sub-tasks.",
                    "status": "skipped",
                    "timestamp": _now_iso(),
                })
                break

            step_number = len(results) + 1
            step_order = int(sub.get("order", step_number - 1))
            planned_step = planned_steps_by_order.get(step_order, {})
            ranking_step = planned_rankings_by_order.get(step_order, {})
            planned_agent_id = str(planned_step.get("agent_id") or "")
            candidate_rankings = list(ranking_step.get("rankings") or [])
            log.info(
                "[TaskManager] executing step=%d/%d capability=%s order=%s planned_agent=%s",
                step_number,
                len(sub_tasks),
                sub["capability"],
                step_order,
                planned_agent_id or None,
            )
            sub_result, attempts = self._execute_subtask(
                sub,
                task_prompt,
                user_id,
                execution_context,
                llm_config,
                max_depth,
                candidate_rankings=candidate_rankings,
                planned_agent_id=planned_agent_id,
                remaining_budget=max(budget_usd - spent_usd, 0.0),
            )
            results.append(sub_result)

            inner = sub_result.get("result", {})
            cost = float(inner.get("cost_usd", 0.0))
            spent_usd += cost
            agent_used = sub_result.get("selected_agent")
            step_output = str(inner.get("output") or inner.get("summary", ""))
            execution_context[f"step_{step_number}_output"] = step_output
            execution_context["last_step_output"] = step_output
            execution_context["steps"].append(
                {
                    "step": step_number,
                    "capability": sub["capability"],
                    "agent": agent_used,
                    "planned_agent": planned_agent_id or None,
                    "prompt_segment": sub.get("prompt_segment", ""),
                    "output": step_output,
                    "routing_mismatch_reason": sub_result.get("transparency", {}).get("routing_mismatch_reason"),
                }
            )

            log.info(
                "[TaskManager] step=%d preview_agent=%s actual_agent=%s mismatch_reason=%s intermediate_output=%r",
                step_number,
                planned_agent_id or None,
                agent_used,
                sub_result.get("transparency", {}).get("routing_mismatch_reason"),
                step_output[:300],
            )

            if agent_used:
                execution_log.append({
                    "step_type": "agent_selection",
                    "step": step_number,
                    "capability": sub["capability"],
                    "agent": agent_used,
                    "description": f"Selected agent '{agent_used}' for capability '{sub['capability']}'.",
                    "status": "success",
                    "timestamp": _now_iso(),
                })

            for retry_idx in range(1, attempts):
                execution_log.append({
                    "step_type": "retry",
                    "step": step_number,
                    "capability": sub["capability"],
                    "description": f"Retry #{retry_idx} for capability '{sub['capability']}'.",
                    "status": "retried",
                    "timestamp": _now_iso(),
                })

            execution_log.append({
                "step_type": "execution",
                "step": step_number,
                "capability": sub["capability"],
                "agent": agent_used,
                "description": inner.get("summary", "") or f"Executed '{sub['capability']}' via {agent_used or 'unknown'}.",
                "status": sub_result.get("state", "completed"),
                "cost_usd": cost,
                "latency_ms": float(inner.get("latency_ms", 0.0)),
                "intermediate_output": step_output,
                "timestamp": _now_iso(),
            })

        aggregated = ResultAggregator.aggregate_sequential(results, execution_context)
        aggregated["orchestration_id"] = orchestration_id
        aggregated["execution_log"] = execution_log
        aggregated["sub_tasks_decomposed"] = [s["capability"] for s in sub_tasks]
        aggregated["final_output"] = aggregated["final_synthesized_output"]
        if routing_plan:
            aggregated["routing_plan"] = routing_plan

        log.info(
            "[TaskManager] aggregation orchestration=%s step_outputs=%s final_output=%r",
            orchestration_id,
            aggregated.get("step_outputs", []),
            aggregated.get("final_synthesized_output", "")[:500],
        )

        self._write_memory(orchestration_id, task_prompt, user_id, sub_tasks, results, aggregated)

        log.info("[TaskManager] orchestration=%s done — cost=$%.4f agents=%s",
                 orchestration_id, aggregated["total_cost_usd"], aggregated["agents_used"])
        return aggregated

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _execute_subtask(
        self,
        sub: dict[str, Any],
        original_prompt: str,
        user_id: str,
        execution_context: dict[str, Any],
        llm_config: dict[str, Any] | None,
        max_depth: int,
        *,
        candidate_rankings: list[dict[str, Any]] | None = None,
        planned_agent_id: str = "",
        remaining_budget: float = 0.0,
    ) -> tuple[dict[str, Any], int]:
        """Returns (result, attempts_used)."""
        prior_step_outputs = [
            str(step.get("output", "")).strip()
            for step in execution_context.get("steps", [])
            if str(step.get("output", "")).strip()
        ]
        context_sections = [f"Original task: {original_prompt.strip()}"]
        parent_context = str(sub.get("parent_context", "")).strip()
        if parent_context and parent_context != original_prompt.strip():
            context_sections.append(f"Parent context: {parent_context}")
        if prior_step_outputs:
            rendered_prior_outputs = "\n\n".join(
                f"Step {idx} output:\n{output}"
                for idx, output in enumerate(prior_step_outputs, start=1)
            )
            context_sections.append(f"Previous step outputs:\n{rendered_prior_outputs}")

        augmented_prompt = str(sub.get("prompt_segment", "")).strip()
        if len(context_sections) > 1:
            augmented_prompt = (
                f"{augmented_prompt}\n\n"
                "Workflow context:\n"
                f"{chr(10).join(context_sections)}"
            )

        base_task_dict = {
            "task_id": str(uuid.uuid4()),
            "required_capability": sub["capability"],
            "task_prompt": augmented_prompt,
            "context": "\n\n".join(context_sections),
            "payload": {
                "task_prompt": augmented_prompt,
                "prompt_segment": sub.get("prompt_segment", ""),
                "context": "\n\n".join(context_sections),
                "user_id": user_id,
                "original_prompt": original_prompt,
                "execution_context": execution_context,
            },
        }
        ranked_agents = list(candidate_rankings or [])
        attempt_plans: list[dict[str, Any]] = []
        used_agent_ids: set[str] = set()
        if planned_agent_id:
            planned_entry = next((entry for entry in ranked_agents if str(entry.get("agent_id") or "") == planned_agent_id), None)
            attempt_plans.append({"agent_id": planned_agent_id, "entry": planned_entry, "reason": "routing_plan"})
            used_agent_ids.add(planned_agent_id)
        for entry in ranked_agents:
            agent_id = str(entry.get("agent_id") or "")
            if not agent_id or agent_id in used_agent_ids:
                continue
            attempt_plans.append({"agent_id": agent_id, "entry": entry, "reason": "ranking_fallback"})
            used_agent_ids.add(agent_id)
        if not attempt_plans:
            attempt_plans.append({"agent_id": "", "entry": None, "reason": "runtime_selection"})

        total_attempts = max(len(attempt_plans), 1)
        result: dict[str, Any] = {
            "state": "failed",
            "reason": "No executable agent available for this sub-task.",
            "transparency": {},
        }
        for attempt_idx, attempt_plan in enumerate(attempt_plans, start=1):
            selected_agent_id = str(attempt_plan.get("agent_id") or "")
            ranking_entry = attempt_plan.get("entry") or {}
            avg_cost = float(((ranking_entry.get("metrics") or {}).get("avg_cost_usd")) or 0.0)
            reselection_reason = ""
            if selected_agent_id and remaining_budget > 0 and avg_cost > remaining_budget:
                reselection_reason = "cost_constraint_violated"
                log.warning(
                    "[TaskManager] agent reselection triggered due to %s original_agent=%s remaining_budget=%.4f avg_cost=%.4f",
                    reselection_reason,
                    selected_agent_id,
                    remaining_budget,
                    avg_cost,
                )
                result = {
                    "state": "failed",
                    "reason": f"agent reselection triggered due to {reselection_reason}",
                    "transparency": {
                        "preview_agent": selected_agent_id or None,
                        "routing_mismatch_reason": reselection_reason,
                    },
                }
                continue

            task_dict = dict(base_task_dict)
            task_dict["task_id"] = str(uuid.uuid4())
            task_dict["candidate_rankings"] = ranked_agents
            if selected_agent_id:
                task_dict["selected_agent_id"] = selected_agent_id

            log.info(
                "[TaskManager] sub-task cap=%s attempt=%d/%d selected_agent=%s selection_mode=%s",
                sub["capability"],
                attempt_idx,
                total_attempts,
                selected_agent_id or None,
                attempt_plan.get("reason"),
            )

            result = self._engine.submit_task(task_dict, llm_config=llm_config)
            actual_agent = str(result.get("selected_agent") or "")
            mismatch_reason = None
            if selected_agent_id and actual_agent and actual_agent != selected_agent_id:
                mismatch_reason = f"agent reselection triggered due to runtime_mismatch ({selected_agent_id} -> {actual_agent})"
                log.warning("[TaskManager] %s", mismatch_reason)
            elif selected_agent_id and result.get("state") == "failed" and attempt_idx < total_attempts:
                mismatch_reason = "agent reselection triggered due to execution_failure"
                log.warning(
                    "[TaskManager] %s original_agent=%s next_agent=%s",
                    mismatch_reason,
                    selected_agent_id,
                    attempt_plans[attempt_idx].get("agent_id") if attempt_idx < total_attempts else None,
                )

            transparency = result.setdefault("transparency", {})
            if isinstance(transparency, dict):
                transparency["preview_agent"] = selected_agent_id or None
                transparency["routing_mismatch_reason"] = mismatch_reason

            if result.get("state") != "failed":
                return result, attempt_idx

        return result, total_attempts  # type: ignore[possibly-undefined]

    def _write_memory(
        self,
        orchestration_id: str,
        prompt: str,
        user_id: str,
        sub_tasks: list[dict[str, Any]],
        results: list[dict[str, Any]],
        aggregated: dict[str, Any],
    ) -> None:
        if self._mongo_db is None:
            return
        try:
            col = self._mongo_db["task_memory"]
            doc = {
                "task_id": orchestration_id,
                "prompt": prompt,
                "user_id": user_id,
                "sub_tasks": [s["capability"] for s in sub_tasks],
                "agents_used": aggregated.get("agents_used", []),
                "total_cost_usd": aggregated.get("total_cost_usd", 0.0),
                "total_latency_ms": aggregated.get("total_latency_ms", 0.0),
                "total_tokens_used": aggregated.get("total_tokens_used", 0),
                "results_summary": [
                    {
                        "task_id": r.get("task_id"),
                        "agent": r.get("selected_agent"),
                        "state": r.get("state"),
                        "summary": (r.get("result") or {}).get("summary", ""),
                    }
                    for r in results
                ],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            col.replace_one({"task_id": orchestration_id}, doc, upsert=True)
        except Exception:
            log.exception("[TaskManager] failed to write task_memory")
