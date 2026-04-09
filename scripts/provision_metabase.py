#!/usr/bin/env python3
"""
Идемпотентная настройка Metabase: первичный setup, админ, студенты, подключения PostgreSQL.
Использует только стандартную библиотеку (urllib).
"""
from __future__ import annotations

import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

METABASE_URL = os.environ.get("METABASE_URL", "http://metabase:3000").rstrip("/")
STUDENT_COUNT = int(os.environ.get("WORKSHOP_STUDENT_COUNT", "30"))
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "workshop_admin@workshop.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "workshop_admin")
ADMIN_FIRST = os.environ.get("METABASE_ADMIN_FIRST_NAME", "Workshop")
ADMIN_LAST = os.environ.get("METABASE_ADMIN_LAST_NAME", "Admin")
SITE_NAME = os.environ.get("METABASE_SITE_NAME", "Workshop")
PG_HOST = os.environ.get("WORKSHOP_PG_HOST", "postgres")
PG_PORT = int(os.environ.get("WORKSHOP_PG_PORT", "5432"))
PG_DB = os.environ.get("WORKSHOP_PG_DATABASE", "workshop")


def _json_req(
    method: str,
    url: str,
    payload: dict | None = None,
    session_id: str | None = None,
) -> tuple[int, dict | str]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if session_id:
        headers["X-Metabase-Session"] = session_id
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            if not raw.strip():
                return resp.status, {}
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode() if e.fp else ""
        try:
            body: dict | str = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            body = raw or str(e)
        return e.code, body
    except (urllib.error.URLError, OSError) as e:
        return 0, str(e)


def wait_ready(max_wait: int = 420) -> bool:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{METABASE_URL}/api/health", timeout=10) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(3)
    return False


def get_properties() -> dict:
    code, body = _json_req("GET", f"{METABASE_URL}/api/session/properties")
    if code == 0:
        print(f"[metabase] skip (unreachable): {body}", file=sys.stderr)
        sys.exit(0)
    if code != 200 or not isinstance(body, dict):
        print(f"[metabase] session/properties failed: {code} {body}", file=sys.stderr)
        sys.exit(1)
    return body


def ensure_setup(props: dict) -> None:
    if props.get("has-user-setup"):
        print("[metabase] site already initialized (has-user-setup).")
        return
    token = props.get("setup-token")
    if not token:
        print("[metabase] no setup-token and not initialized — check Metabase volume.", file=sys.stderr)
        sys.exit(1)
    payload = {
        "token": token,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name": ADMIN_LAST,
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        },
        "prefs": {
            "site_name": SITE_NAME,
            "site_locale": "ru",
            "allow_tracking": False,
        },
    }
    code, body = _json_req("POST", f"{METABASE_URL}/api/setup", payload)
    if code not in (200, 201):
        print(f"[metabase] POST /api/setup failed: {code} {body}", file=sys.stderr)
        sys.exit(1)
    print("[metabase] initial setup completed.")


def login() -> str:
    code, body = _json_req(
        "POST",
        f"{METABASE_URL}/api/session",
        {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    if code != 200 or not isinstance(body, dict) or not body.get("id"):
        print(f"[metabase] admin login failed: {code} {body}", file=sys.stderr)
        sys.exit(1)
    sid = str(body["id"])
    print("[metabase] admin session OK.")
    return sid


def list_users(session_id: str) -> list[dict]:
    code, body = _json_req("GET", f"{METABASE_URL}/api/user", session_id=session_id)
    if code != 200:
        print(f"[metabase] GET /api/user failed: {code} {body}", file=sys.stderr)
        sys.exit(1)
    if isinstance(body, dict) and "data" in body:
        return body["data"]
    if isinstance(body, list):
        return body
    return []


def ensure_student_users(session_id: str) -> None:
    users = list_users(session_id)
    emails = {u.get("email") for u in users if isinstance(u, dict)}
    for i in range(1, STUDENT_COUNT + 1):
        sid = f"{i:02d}"
        email = f"workshop_student_{sid}@workshop.local"
        if email in emails:
            continue
        pwd = f"workshop_stu{sid}"
        payload = {
            "first_name": "Student",
            "last_name": sid,
            "email": email,
            "password": pwd,
        }
        code, body = _json_req(
            "POST", f"{METABASE_URL}/api/user", payload, session_id=session_id
        )
        if code in (200, 201):
            print(f"[metabase] created user {email}")
        elif code == 400 and isinstance(body, dict):
            errs = body.get("errors") or body
            print(f"[metabase] skip user {email}: {errs}")
        else:
            print(f"[metabase] POST user {email} failed: {code} {body}", file=sys.stderr)


def list_databases(session_id: str) -> list[dict]:
    code, body = _json_req("GET", f"{METABASE_URL}/api/database", session_id=session_id)
    if code != 200:
        print(f"[metabase] GET /api/database failed: {code} {body}", file=sys.stderr)
        sys.exit(1)
    if isinstance(body, dict) and "data" in body:
        return body["data"]
    if isinstance(body, list):
        return body
    return []


def ensure_databases(session_id: str) -> None:
    dbs = list_databases(session_id)
    names = {d.get("name") for d in dbs if isinstance(d, dict)}
    for i in range(1, STUDENT_COUNT + 1):
        sid = f"{i:02d}"
        name = f"Workshop PG (student {sid})"
        if name in names:
            continue
        user = f"workshop_student_{sid}"
        pwd = f"workshop_stu{sid}"
        payload = {
            "engine": "postgres",
            "name": name,
            "details": {
                "host": PG_HOST,
                "port": PG_PORT,
                "dbname": PG_DB,
                "user": user,
                "password": pwd,
                "ssl": False,
                # JDBC ApplicationName → Postgres log_line_prefix app=… (кто дернул UI, роль всё равно workshop_student_XX).
                "additional-options": f"ApplicationName=metabase_student_{sid}",
                "let-user-control-scheduling": False,
            },
        }
        code, body = _json_req(
            "POST", f"{METABASE_URL}/api/database", payload, session_id=session_id
        )
        if code in (200, 201):
            print(f"[metabase] added database connection: {name}")
        else:
            print(f"[metabase] POST database {name}: {code} {body}", file=sys.stderr)


def metabase_host_resolves() -> bool:
    h = urlparse(METABASE_URL).hostname or "metabase"
    try:
        socket.getaddrinfo(h, None)
        return True
    except OSError:
        return False


def main() -> None:
    if not metabase_host_resolves():
        print("[metabase] host not in Docker network (add --profile bi); skip.", file=sys.stderr)
        sys.exit(0)
    if not wait_ready():
        print("[metabase] Metabase not healthy in time; skip.", file=sys.stderr)
        sys.exit(0)
    props = get_properties()
    ensure_setup(props)
    session_id = login()
    ensure_student_users(session_id)
    ensure_databases(session_id)
    print("[metabase] provisioning done.")


if __name__ == "__main__":
    main()
