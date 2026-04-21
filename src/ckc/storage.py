from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class SQLiteStore:
    """Simple SQLite persistence layer for CKC MVP runtime data."""

    LATEST_SCHEMA_VERSION = 3

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    @contextmanager
    def _session(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._session() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    note TEXT,
                    applied_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS performance_profiles (
                    agent_id TEXT PRIMARY KEY,
                    success_rate REAL NOT NULL,
                    avg_latency_ms REAL NOT NULL,
                    avg_cost_usd REAL NOT NULL,
                    availability REAL NOT NULL,
                    runs REAL NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_json TEXT NOT NULL,
                    current_state TEXT NOT NULL,
                    result_json TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    event_index INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    data_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    username TEXT,
                    email TEXT NOT NULL,
                    password_hash TEXT,
                    password_salt TEXT,
                    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
                    lockout_until TEXT,
                    user_type TEXT NOT NULL DEFAULT 'standard',
                    llm_config_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workflow_approvals (
                    approval_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operation_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            try:
                conn.execute("ALTER TABLE users ADD COLUMN user_type TEXT NOT NULL DEFAULT 'standard'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN llm_config_json TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN password_salt TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN failed_login_attempts INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN lockout_until TEXT")
            except sqlite3.OperationalError:
                pass

            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_task_events_task_id_event_index ON task_events(task_id, event_index)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")

            self._migrate_schema(conn)

    def _current_schema_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
        return int(row[0] or 0)

    def _record_migration(self, conn: sqlite3.Connection, version: int, note: str) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO schema_migrations(version, note, applied_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (version, note),
        )

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        current = self._current_schema_version(conn)
        if current >= self.LATEST_SCHEMA_VERSION:
            return

        if current < 1:
            self._record_migration(conn, 1, "Initial runtime schema baseline")
        if current < 2:
            self._record_migration(conn, 2, "User auth and profile schema extensions")
        if current < 3:
            self._record_migration(conn, 3, "Indexes for task timelines and user lookup")

    def schema_version(self) -> int:
        with self._session() as conn:
            return self._current_schema_version(conn)

    def list_schema_migrations(self) -> list[dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute(
                "SELECT version, note, applied_at FROM schema_migrations ORDER BY version ASC"
            ).fetchall()
        return [
            {"version": int(row[0]), "note": str(row[1] or ""), "applied_at": str(row[2] or "")}
            for row in rows
        ]

    def upsert_agent(self, agent: dict[str, Any]) -> None:
        agent_id = agent["agent_id"]
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO agents(agent_id, payload_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(agent_id) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (agent_id, json.dumps(agent)),
            )

    def upsert_performance_profile(self, agent_id: str, profile: dict[str, float]) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO performance_profiles(
                    agent_id, success_rate, avg_latency_ms, avg_cost_usd, availability, runs, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(agent_id) DO UPDATE SET
                    success_rate=excluded.success_rate,
                    avg_latency_ms=excluded.avg_latency_ms,
                    avg_cost_usd=excluded.avg_cost_usd,
                    availability=excluded.availability,
                    runs=excluded.runs,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    agent_id,
                    float(profile.get("success_rate", 0.7)),
                    float(profile.get("avg_latency_ms", 8000.0)),
                    float(profile.get("avg_cost_usd", 0.25)),
                    float(profile.get("availability", 1.0)),
                    float(profile.get("runs", 0.0)),
                ),
            )

    def upsert_task(
        self,
        task_id: str,
        task: dict[str, Any],
        current_state: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO tasks(task_id, task_json, current_state, result_json, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(task_id) DO UPDATE SET
                    task_json=excluded.task_json,
                    current_state=excluded.current_state,
                    result_json=excluded.result_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    task_id,
                    json.dumps(task),
                    current_state,
                    json.dumps(result) if result is not None else None,
                ),
            )

    def append_task_event(self, task_id: str, state: str, data: dict[str, Any] | None = None) -> None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(event_index), -1) FROM task_events WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            next_index = int(row[0]) + 1
            conn.execute(
                """
                INSERT INTO task_events(task_id, event_index, state, data_json)
                VALUES (?, ?, ?, ?)
                """,
                (task_id, next_index, state, json.dumps(data) if data else None),
            )

    def load_agents(self) -> list[dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute("SELECT payload_json FROM agents").fetchall()
        return [json.loads(row[0]) for row in rows]

    def load_performance_profiles(self) -> dict[str, dict[str, float]]:
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT agent_id, success_rate, avg_latency_ms, avg_cost_usd, availability, runs
                FROM performance_profiles
                """
            ).fetchall()
        profiles: dict[str, dict[str, float]] = {}
        for row in rows:
            profiles[str(row[0])] = {
                "success_rate": float(row[1]),
                "avg_latency_ms": float(row[2]),
                "avg_cost_usd": float(row[3]),
                "availability": float(row[4]),
                "runs": float(row[5]),
            }
        return profiles

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._session() as conn:
            task_row = conn.execute(
                "SELECT task_json, current_state, result_json FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if not task_row:
                return None
            event_rows = conn.execute(
                """
                SELECT state, data_json
                FROM task_events
                WHERE task_id = ?
                ORDER BY event_index ASC
                """,
                (task_id,),
            ).fetchall()

        return {
            "task": json.loads(task_row[0]),
            "current_state": str(task_row[1]),
            "result": json.loads(task_row[2]) if task_row[2] else None,
            "events": [
                {"state": str(row[0]), "data": json.loads(row[1]) if row[1] else None}
                for row in event_rows
            ],
        }

    def list_tasks(self, limit: int = 25) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 200))
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT task_id, current_state, updated_at
                FROM tasks
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [
            {
                "task_id": str(row[0]),
                "state": str(row[1]),
                "updated_at": str(row[2]),
            }
            for row in rows
        ]

    def list_tasks_for_user(self, user_id: str, limit: int = 25) -> list[dict[str, Any]]:
        """List persisted tasks owned by user_id (stored in task_json.user_id)."""
        safe_limit = max(1, min(limit, 200))
        uid = str(user_id or "").strip()
        if not uid:
            return []
        with self._session() as conn:
            try:
                rows = conn.execute(
                    """
                    SELECT task_id, current_state, updated_at,
                           json_extract(task_json, '$.task_prompt') AS task_prompt
                    FROM tasks
                    WHERE json_extract(task_json, '$.user_id') = ?
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (uid, safe_limit),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            if rows:
                return [
                    {
                        "task_id": str(row[0]),
                        "state": str(row[1]),
                        "updated_at": str(row[2]),
                        "task_prompt": str(row[3] or ""),
                    }
                    for row in rows
                ]
            # Fallback when JSON1 is unavailable or legacy rows: scan recent tasks
            rows = conn.execute(
                """
                SELECT task_id, current_state, updated_at, task_json
                FROM tasks
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (min(800, safe_limit * 40),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                tj = json.loads(row[3])
                if str(tj.get("user_id") or "") != uid:
                    continue
            except (TypeError, json.JSONDecodeError):
                continue
            out.append(
                {
                    "task_id": str(row[0]),
                    "state": str(row[1]),
                    "updated_at": str(row[2]),
                    "task_prompt": str(tj.get("task_prompt") or ""),
                }
            )
            if len(out) >= safe_limit:
                break
        return out

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
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO users(
                    user_id, name, username, email, password_hash, password_salt, failed_login_attempts,
                    lockout_until, user_type, llm_config_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    name=excluded.name,
                    username=COALESCE(excluded.username, users.username),
                    email=excluded.email,
                    password_hash=COALESCE(excluded.password_hash, users.password_hash),
                    password_salt=COALESCE(excluded.password_salt, users.password_salt),
                    failed_login_attempts=COALESCE(excluded.failed_login_attempts, users.failed_login_attempts),
                    lockout_until=excluded.lockout_until,
                    user_type=excluded.user_type,
                    llm_config_json=excluded.llm_config_json
                """,
                (
                    user_id,
                    name,
                    username,
                    email,
                    password_hash,
                    password_salt,
                    failed_login_attempts,
                    lockout_until,
                    user_type,
                    json.dumps(llm_config) if llm_config is not None else None,
                ),
            )

    def update_user_llm_config(self, user_id: str, llm_config: dict[str, Any]) -> None:
        with self._session() as conn:
            conn.execute(
                """
                UPDATE users
                SET llm_config_json = ?, created_at = created_at
                WHERE user_id = ?
                """,
                (json.dumps(llm_config), user_id),
            )

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        with self._session() as conn:
            row = conn.execute(
                """
                SELECT user_id, name, username, email, password_hash, password_salt,
                       failed_login_attempts, lockout_until, user_type, llm_config_json
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if not row:
            return None
        llm_config = json.loads(row[9]) if row[9] else {"preferred_model": None, "keys": []}
        return {
            "user_id": str(row[0]),
            "name": str(row[1]),
            "username": str(row[2]) if row[2] else None,
            "email": str(row[3]),
            "password_hash": str(row[4]) if row[4] else None,
            "password_salt": str(row[5]) if row[5] else None,
            "failed_login_attempts": int(row[6] or 0),
            "lockout_until": str(row[7]) if row[7] else None,
            "user_type": str(row[8]),
            "llm_config": llm_config,
        }

    def find_user_by_login_identifier(self, identifier: str) -> dict[str, Any] | None:
        """identifier must already be normalized (strip + lower)."""
        nid = (identifier or "").strip().lower()
        if not nid:
            return None
        with self._session() as conn:
            row = conn.execute(
                """
                SELECT user_id, name, username, email, password_hash, password_salt,
                       failed_login_attempts, lockout_until, user_type, llm_config_json
                FROM users
                WHERE lower(trim(email)) = ?
                   OR (username IS NOT NULL AND lower(trim(username)) = ?)
                LIMIT 1
                """,
                (nid, nid),
            ).fetchone()
        if not row:
            return None
        llm_config = json.loads(row[9]) if row[9] else {"preferred_model": None, "keys": []}
        return {
            "user_id": str(row[0]),
            "name": str(row[1]),
            "username": str(row[2]) if row[2] else None,
            "email": str(row[3]),
            "password_hash": str(row[4]) if row[4] else None,
            "password_salt": str(row[5]) if row[5] else None,
            "failed_login_attempts": int(row[6] or 0),
            "lockout_until": str(row[7]) if row[7] else None,
            "user_type": str(row[8]),
            "llm_config": llm_config,
        }

    def list_users(self) -> list[dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT user_id, name, username, email, password_hash, password_salt,
                       failed_login_attempts, lockout_until, user_type, llm_config_json
                FROM users
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [
            {
                "user_id": str(row[0]),
                "name": str(row[1]),
                "username": str(row[2]) if row[2] else None,
                "email": str(row[3]),
                "password_hash": str(row[4]) if row[4] else None,
                "password_salt": str(row[5]) if row[5] else None,
                "failed_login_attempts": int(row[6] or 0),
                "lockout_until": str(row[7]) if row[7] else None,
                "user_type": str(row[8]),
                "llm_config": json.loads(row[9]) if row[9] else {"preferred_model": None, "keys": []},
            }
            for row in rows
        ]

    def upsert_workflow_approval(self, approval_id: str, payload: dict[str, Any]) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO workflow_approvals(approval_id, payload_json, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(approval_id) DO UPDATE SET
                    payload_json=excluded.payload_json
                """,
                (approval_id, json.dumps(payload)),
            )

    def get_workflow_approval(self, approval_id: str) -> dict[str, Any] | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT payload_json FROM workflow_approvals WHERE approval_id = ?",
                (approval_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def delete_workflow_approval(self, approval_id: str) -> None:
        with self._session() as conn:
            conn.execute("DELETE FROM workflow_approvals WHERE approval_id = ?", (approval_id,))

    def list_workflow_approvals(self) -> dict[str, dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute("SELECT approval_id, payload_json FROM workflow_approvals").fetchall()
        return {str(row[0]): json.loads(row[1]) for row in rows}

    def upsert_operation_receipt(self, receipt_id: str, payload: dict[str, Any]) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO operation_receipts(receipt_id, payload_json, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(receipt_id) DO UPDATE SET
                    payload_json=excluded.payload_json
                """,
                (receipt_id, json.dumps(payload)),
            )

    def get_operation_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT payload_json FROM operation_receipts WHERE receipt_id = ?",
                (receipt_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def list_operation_receipts(self) -> dict[str, dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute("SELECT receipt_id, payload_json FROM operation_receipts").fetchall()
        return {str(row[0]): json.loads(row[1]) for row in rows}
