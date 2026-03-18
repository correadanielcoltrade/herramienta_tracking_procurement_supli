import os

from werkzeug.security import generate_password_hash

from queries.json_store import read_json, write_json
from queries.db import db_enabled, execute, init_db

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
USERS_FILE = os.path.join(DATA_DIR, "users.json")

VALID_ROLES = {"ADMIN", "USER"}


def load_users():
    if db_enabled():
        init_db()
        rows = execute(
            "SELECT username, name, role, password_hash FROM trackingsupli_users ORDER BY username",
            fetchall=True,
        )
        return rows or []
    if not os.path.exists(USERS_FILE):
        return []
    return read_json(USERS_FILE, [])


def save_users(users):
    if db_enabled():
        return
    write_json(USERS_FILE, users)


def list_users():
    if db_enabled():
        init_db()
        rows = execute(
            "SELECT username, name, role FROM trackingsupli_users ORDER BY username",
            fetchall=True,
        )
        return rows or []
    users = load_users()
    return [
        {"username": u.get("username"), "name": u.get("name"), "role": u.get("role")}
        for u in users
    ]


def _count_admins(users):
    return sum(1 for u in users if u.get("role") == "ADMIN")


def create_user(payload):
    username = (payload.get("username") or "").strip()
    if not username:
        return None, "Username requerido"
    if db_enabled():
        init_db()
        existing = execute(
            "SELECT 1 FROM trackingsupli_users WHERE username = %s",
            (username,),
            fetchone=True,
        )
        if existing:
            return None, "Username ya existe"
    else:
        users = load_users()
        if any(u.get("username") == username for u in users):
            return None, "Username ya existe"

    role = payload.get("role", "USER")
    if role not in VALID_ROLES:
        role = "USER"
    password = (payload.get("password") or "").strip()
    if not password:
        return None, "Password requerido"

    user = {
        "username": username,
        "name": (payload.get("name") or "").strip(),
        "role": role,
        "password_hash": generate_password_hash(password),
    }
    if db_enabled():
        execute(
            "INSERT INTO trackingsupli_users (username, name, role, password_hash) VALUES (%s, %s, %s, %s)",
            (user["username"], user["name"], user["role"], user["password_hash"]),
        )
    else:
        users.append(user)
        save_users(users)
    return user, None


def update_user(username, payload):
    if db_enabled():
        init_db()
        user = execute(
            "SELECT username, name, role, password_hash FROM trackingsupli_users WHERE username = %s",
            (username,),
            fetchone=True,
        )
        if not user:
            return None, "Usuario no encontrado"
        name = payload.get("name")
        if name is not None:
            user["name"] = str(name).strip()
        role = payload.get("role")
        if role in VALID_ROLES:
            if role != user.get("role") and user.get("role") == "ADMIN":
                count_row = execute(
                    "SELECT COUNT(*) AS count FROM trackingsupli_users WHERE role = 'ADMIN'",
                    fetchone=True,
                )
                if count_row and count_row.get("count", 0) <= 1:
                    return None, "Debe existir al menos un ADMIN"
            user["role"] = role
        password = payload.get("password")
        if password:
            user["password_hash"] = generate_password_hash(str(password))
        execute(
            """
            UPDATE trackingsupli_users
            SET name = %s, role = %s, password_hash = %s, updated_at = NOW()
            WHERE username = %s
            """,
            (user.get("name"), user.get("role"), user.get("password_hash"), username),
        )
        return user, None

    users = load_users()
    for user in users:
        if user.get("username") == username:
            name = payload.get("name")
            if name is not None:
                user["name"] = str(name).strip()
            role = payload.get("role")
            if role in VALID_ROLES:
                if role != user.get("role") and user.get("role") == "ADMIN":
                    if _count_admins(users) <= 1:
                        return None, "Debe existir al menos un ADMIN"
                user["role"] = role
            password = payload.get("password")
            if password:
                user["password_hash"] = generate_password_hash(str(password))
                user.pop("password", None)
            save_users(users)
            return user, None
    return None, "Usuario no encontrado"


def delete_user(username):
    if db_enabled():
        init_db()
        user = execute(
            "SELECT username, role FROM trackingsupli_users WHERE username = %s",
            (username,),
            fetchone=True,
        )
        if not user:
            return False, "Usuario no encontrado"
        if user.get("role") == "ADMIN":
            count_row = execute(
                "SELECT COUNT(*) AS count FROM trackingsupli_users WHERE role = 'ADMIN'",
                fetchone=True,
            )
            if count_row and count_row.get("count", 0) <= 1:
                return False, "Debe existir al menos un ADMIN"
        execute("DELETE FROM trackingsupli_users WHERE username = %s", (username,))
        return True, None

    users = load_users()
    user = next((u for u in users if u.get("username") == username), None)
    if not user:
        return False, "Usuario no encontrado"
    if user.get("role") == "ADMIN" and _count_admins(users) <= 1:
        return False, "Debe existir al menos un ADMIN"
    users = [u for u in users if u.get("username") != username]
    save_users(users)
    return True, None
