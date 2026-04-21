"""MongoDB persistence layer for CKC runtime — drop-in replacement for SQLiteStore.

Implements the same public interface (Contract A) so that CKCEngine and api.py
can use it without any call-site changes.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pymongo.database import Database


class MongoStore:
    """MongoDB-backed storage that mirrors every SQLiteStore public method."""

    LATEST_SCHEMA_VERSION = 1

    def __init__(self, db: Database) -> None:
        self._db = db
        self._agents = db["engine_agents"]
        self._perf = db["performance_profiles"]
        self._tasks = db["engine_tasks"]
        self._events = db["task_events"]
        self._users = db["engine_users"]
        self._approvals = db["workflow_approvals"]
        self._receipts = db["operation_receipts"]
        self._task_memory = db["task_memory"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._agents.create_index("agent_id", unique=True)
        self._perf.create_index("agent_id", unique=True)
        self._tasks.create_index("task_id", unique=True)
        self._tasks.create_index("updated_at")
        self._events.create_index([("task_id", 1), ("event_index", 1)], unique=True)
        self._users.create_index("user_id", unique=True)
        self._users.create_index("email")
        self._users.create_index("username")
        self._approvals.create_index("approval_id", unique=True)
        self._receipts.create_index("receipt_id", unique=True)
        self._task_memory.create_index("task_id", unique=True)

    @staticmethod
    def _strip_id(doc: dict | None) -> dict | None:
        if doc is None:
            return None
        doc.pop("_id", None)
        return doc

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Agents
    # ------------------------------------------------------------------

    def upsert_agent(self, agent: dict[str, Any]) -> None:
        agent_id = agent["agent_id"]
        doc = {**agent, "agent_id": agent_id, "updated_at": self._now_iso()}
        self._agents.replace_one({"agent_id": agent_id}, doc, upsert=True)

    def load_agents(self) -> list[dict[str, Any]]:
        return [self._strip_id(doc) for doc in self._agents.find({})]

    # ------------------------------------------------------------------
    # Performance profiles
    # ------------------------------------------------------------------

    def upsert_performance_profile(self, agent_id: str, profile: dict[str, float]) -> None:
        doc = {
            "agent_id": agent_id,
            "success_rate": float(profile.get("success_rate", 0.7)),
            "avg_latency_ms": float(profile.get("avg_latency_ms", 8000.0)),
            "avg_cost_usd": float(profile.get("avg_cost_usd", 0.25)),
            "availability": float(profile.get("availability", 1.0)),
            "runs": float(profile.get("runs", 0.0)),
            "updated_at": self._now_iso(),
        }
        self._perf.replace_one({"agent_id": agent_id}, doc, upsert=True)

    def load_performance_profiles(self) -> dict[str, dict[str, float]]:
        profiles: dict[str, dict[str, float]] = {}
        for doc in self._perf.find({}):
            profiles[doc["agent_id"]] = {
                "success_rate": float(doc.get("success_rate", 0.7)),
                "avg_latency_ms": float(doc.get("avg_latency_ms", 8000.0)),
                "avg_cost_usd": float(doc.get("avg_cost_usd", 0.25)),
                "availability": float(doc.get("availability", 1.0)),
                "runs": float(doc.get("runs", 0.0)),
            }
        return profiles

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def upsert_task(
        self,
        task_id: str,
        task: dict[str, Any],
        current_state: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        doc = {
            "task_id": task_id,
            "task_json": json.dumps(task),
            "current_state": current_state,
            "result_json": json.dumps(result) if result is not None else None,
            "updated_at": self._now_iso(),
            "owner_user_id": task.get("user_id"),
        }
        self._tasks.replace_one({"task_id": task_id}, doc, upsert=True)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        doc = self._tasks.find_one({"task_id": task_id})
        if doc is None:
            return None
        events = list(
            self._events.find({"task_id": task_id}).sort("event_index", 1)
        )
        return {
            "task": json.loads(doc["task_json"]),
            "current_state": doc["current_state"],
            "result": json.loads(doc["result_json"]) if doc.get("result_json") else None,
            "events": [
                {
                    "state": e["state"],
                    "data": json.loads(e["data_json"]) if e.get("data_json") else None,
                }
                for e in events
            ],
        }

    def list_tasks(self, limit: int = 25) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 200))
        docs = (
            self._tasks.find({}, {"task_id": 1, "current_state": 1, "updated_at": 1, "_id": 0})
            .sort("updated_at", -1)
            .limit(safe_limit)
        )
        return [
            {
                "task_id": doc["task_id"],
                "state": doc["current_state"],
                "updated_at": doc.get("updated_at", ""),
            }
            for doc in docs
        ]

    def list_tasks_for_user(self, user_id: str, limit: int = 25) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 200))
        uid = str(user_id or "").strip()
        if not uid:
            return []
        docs = (
            self._tasks.find({"owner_user_id": uid}, {"task_id": 1, "current_state": 1, "updated_at": 1, "task_json": 1, "_id": 0})
            .sort("updated_at", -1)
            .limit(safe_limit)
        )
        out = []
        for doc in docs:
            tp = ""
            try:
                tj = json.loads(doc["task_json"])
                tp = str(tj.get("task_prompt") or "")
            except (TypeError, KeyError, json.JSONDecodeError):
                pass
            out.append(
                {
                    "task_id": doc["task_id"],
                    "state": doc["current_state"],
                    "updated_at": doc.get("updated_at", ""),
                    "task_prompt": tp,
                }
            )
        if len(out) >= safe_limit:
            return out
        # Legacy rows without owner_user_id: scan recent tasks and match task_json.user_id
        need = safe_limit - len(out)
        seen = {row["task_id"] for row in out}
        legacy = self._tasks.find(
            {"$or": [{"owner_user_id": {"$exists": False}}, {"owner_user_id": None}]},
            {"task_id": 1, "current_state": 1, "updated_at": 1, "task_json": 1, "_id": 0},
        ).sort("updated_at", -1).limit(min(500, need * 25))
        for doc in legacy:
            tid = doc.get("task_id")
            if not tid or tid in seen:
                continue
            try:
                tj = json.loads(doc["task_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if str(tj.get("user_id") or "") != uid:
                continue
            out.append(
                {
                    "task_id": tid,
                    "state": doc["current_state"],
                    "updated_at": doc.get("updated_at", ""),
                    "task_prompt": str(tj.get("task_prompt") or ""),
                }
            )
            seen.add(tid)
            if len(out) >= safe_limit:
                break
        return out[:safe_limit]

    def append_task_event(self, task_id: str, state: str, data: dict[str, Any] | None = None) -> None:
        last = self._events.find_one(
            {"task_id": task_id},
            sort=[("event_index", -1)],
        )
        next_index = (last["event_index"] + 1) if last else 0
        self._events.insert_one(
            {
                "task_id": task_id,
                "event_index": next_index,
                "state": state,
                "data_json": json.dumps(data) if data else None,
                "created_at": self._now_iso(),
            }
        )

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    def upsert_user(
        self,
        user_id: str,
        name: str,
        email: str,
        user_type: str = "standard",
        llm_config: dict[str, Any] | None = None,
        username: str | None = None,
        password_hash: str | None = None,
        password_salt: str | None = None,
        failed_login_attempts: int | None = None,
        lockout_until: str | None = None,
    ) -> None:
        existing = self._users.find_one({"user_id": user_id})
        doc: dict[str, Any] = {
            "user_id": user_id,
            "name": name,
            "email": email,
            "user_type": user_type,
            "llm_config_json": json.dumps(llm_config) if llm_config is not None else (
                existing.get("llm_config_json") if existing else None
            ),
            "username": username if username is not None else (
                existing.get("username") if existing else None
            ),
            "password_hash": password_hash if password_hash is not None else (
                existing.get("password_hash") if existing else None
            ),
            "password_salt": password_salt if password_salt is not None else (
                existing.get("password_salt") if existing else None
            ),
            "failed_login_attempts": failed_login_attempts if failed_login_attempts is not None else (
                existing.get("failed_login_attempts", 0) if existing else 0
            ),
            "lockout_until": lockout_until,
            "created_at": existing.get("created_at", self._now_iso()) if existing else self._now_iso(),
        }
        self._users.replace_one({"user_id": user_id}, doc, upsert=True)

    def update_user_llm_config(self, user_id: str, llm_config: dict[str, Any]) -> None:
        self._users.update_one(
            {"user_id": user_id},
            {"$set": {"llm_config_json": json.dumps(llm_config)}},
        )

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        doc = self._users.find_one({"user_id": user_id})
        if doc is None:
            return None
        llm_raw = doc.get("llm_config_json")
        llm_config = json.loads(llm_raw) if llm_raw else {"preferred_model": None, "keys": []}
        return {
            "user_id": doc["user_id"],
            "name": doc.get("name", ""),
            "username": doc.get("username"),
            "email": doc.get("email", ""),
            "password_hash": doc.get("password_hash"),
            "password_salt": doc.get("password_salt"),
            "failed_login_attempts": int(doc.get("failed_login_attempts", 0)),
            "lockout_until": doc.get("lockout_until"),
            "user_type": doc.get("user_type", "standard"),
            "llm_config": llm_config,
        }

    def find_user_by_login_identifier(self, identifier: str) -> dict[str, Any] | None:
        """identifier must already be normalized (strip + lower)."""
        nid = (identifier or "").strip().lower()
        if not nid:
            return None
        doc = self._users.find_one({"$or": [{"email": nid}, {"username": nid}]})
        if doc is None:
            return None
        self._strip_id(doc)
        llm_raw = doc.get("llm_config_json")
        llm_config = json.loads(llm_raw) if llm_raw else {"preferred_model": None, "keys": []}
        return {
            "user_id": doc["user_id"],
            "name": doc.get("name", ""),
            "username": doc.get("username"),
            "email": doc.get("email", ""),
            "password_hash": doc.get("password_hash"),
            "password_salt": doc.get("password_salt"),
            "failed_login_attempts": int(doc.get("failed_login_attempts", 0)),
            "lockout_until": doc.get("lockout_until"),
            "user_type": doc.get("user_type", "standard"),
            "llm_config": llm_config,
        }

    def list_users(self) -> list[dict[str, Any]]:
        docs = self._users.find({}).sort("created_at", -1)
        result = []
        for doc in docs:
            llm_raw = doc.get("llm_config_json")
            llm_config = json.loads(llm_raw) if llm_raw else {"preferred_model": None, "keys": []}
            result.append({
                "user_id": doc["user_id"],
                "name": doc.get("name", ""),
                "username": doc.get("username"),
                "email": doc.get("email", ""),
                "password_hash": doc.get("password_hash"),
                "password_salt": doc.get("password_salt"),
                "failed_login_attempts": int(doc.get("failed_login_attempts", 0)),
                "lockout_until": doc.get("lockout_until"),
                "user_type": doc.get("user_type", "standard"),
                "llm_config": llm_config,
            })
        return result

    # ------------------------------------------------------------------
    # Workflow approvals
    # ------------------------------------------------------------------

    def upsert_workflow_approval(self, approval_id: str, payload: dict[str, Any]) -> None:
        doc = {
            "approval_id": approval_id,
            "payload_json": json.dumps(payload),
            "created_at": self._now_iso(),
        }
        self._approvals.replace_one({"approval_id": approval_id}, doc, upsert=True)

    def get_workflow_approval(self, approval_id: str) -> dict[str, Any] | None:
        doc = self._approvals.find_one({"approval_id": approval_id})
        if doc is None:
            return None
        return json.loads(doc["payload_json"])

    def delete_workflow_approval(self, approval_id: str) -> None:
        self._approvals.delete_one({"approval_id": approval_id})

    def list_workflow_approvals(self) -> dict[str, dict[str, Any]]:
        docs = self._approvals.find({})
        return {doc["approval_id"]: json.loads(doc["payload_json"]) for doc in docs}

    # ------------------------------------------------------------------
    # Operation receipts
    # ------------------------------------------------------------------

    def upsert_operation_receipt(self, receipt_id: str, payload: dict[str, Any]) -> None:
        doc = {
            "receipt_id": receipt_id,
            "payload_json": json.dumps(payload),
            "created_at": self._now_iso(),
        }
        self._receipts.replace_one({"receipt_id": receipt_id}, doc, upsert=True)

    def get_operation_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        doc = self._receipts.find_one({"receipt_id": receipt_id})
        if doc is None:
            return None
        return json.loads(doc["payload_json"])

    def list_operation_receipts(self) -> dict[str, dict[str, Any]]:
        docs = self._receipts.find({})
        return {doc["receipt_id"]: json.loads(doc["payload_json"]) for doc in docs}

    # ------------------------------------------------------------------
    # Schema (stubs — MongoDB needs no migrations)
    # ------------------------------------------------------------------

    def schema_version(self) -> int:
        return self.LATEST_SCHEMA_VERSION

    def list_schema_migrations(self) -> list[dict[str, Any]]:
        return [
            {
                "version": 1,
                "note": "MongoDB backend — no migrations needed",
                "applied_at": self._now_iso(),
            }
        ]

    # ------------------------------------------------------------------
    # Task memory (new — used by orchestrator)
    # ------------------------------------------------------------------

    def upsert_task_memory(self, task_id: str, memory: dict[str, Any]) -> None:
        doc = {**memory, "task_id": task_id, "updated_at": self._now_iso()}
        self._task_memory.replace_one({"task_id": task_id}, doc, upsert=True)

    def get_task_memory(self, task_id: str) -> dict[str, Any] | None:
        doc = self._task_memory.find_one({"task_id": task_id})
        return self._strip_id(doc)
