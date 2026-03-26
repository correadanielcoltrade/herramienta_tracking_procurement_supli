"""
auth_service.py - Autenticacion y autorizacion via almacenamiento JSON primario.
"""

import os
from datetime import datetime, timedelta
from functools import wraps

import jwt
from flask import current_app, g, jsonify, redirect, request
from werkzeug.security import check_password_hash, generate_password_hash

from queries.storage import (
    get_record_by_field,
    load_records,
    make_record,
    next_id,
    save_records,
)

JWT_ALG = "HS256"
JWT_EXP_HOURS = 8

# Clave de archivo de usuarios
_USERS_FILE_KEY = "ts_users"


# ---------------------------------------------------------------------------
# Bootstrap de usuarios por defecto
# ---------------------------------------------------------------------------

def _ensure_default_users() -> None:
    """
    Crea los usuarios admin/user por defecto si ts_users.json esta vacio
    o si no existe ninguno de los dos.
    """
    records = load_records(_USERS_FILE_KEY)
    usernames = {r.get("data_json", {}).get("username", "") for r in records}

    new_records = list(records)
    next_auto_id = next_id(_USERS_FILE_KEY)
    changed = False

    if "admin" not in usernames:
        new_records.append(
            make_record(
                next_auto_id,
                {
                    "username": "admin",
                    "name": "Administrador",
                    "role": "ADMIN",
                    "password_hash": generate_password_hash("admin123"),
                },
            )
        )
        next_auto_id += 1
        changed = True

    if "user" not in usernames:
        new_records.append(
            make_record(
                next_auto_id,
                {
                    "username": "user",
                    "name": "Usuario",
                    "role": "USER",
                    "password_hash": generate_password_hash("user123"),
                },
            )
        )
        changed = True

    if changed:
        save_records(_USERS_FILE_KEY, new_records)


# ---------------------------------------------------------------------------
# Carga de usuarios
# ---------------------------------------------------------------------------

def load_users() -> list:
    """Devuelve la lista plana de dicts de usuario."""
    _ensure_default_users()
    return [r.get("data_json", {}) for r in load_records(_USERS_FILE_KEY)]


def get_user(username: str) -> dict | None:
    """Devuelve el dict de usuario para ese username, o None."""
    _ensure_default_users()
    record = get_record_by_field(_USERS_FILE_KEY, "username", username)
    if record:
        return record.get("data_json", {})
    return None


# ---------------------------------------------------------------------------
# Autenticacion
# ---------------------------------------------------------------------------

def authenticate(username: str, password: str) -> dict | None:
    """
    Verifica credenciales. Admite password_hash (werkzeug) o password en texto
    plano (compatibilidad con registros legacy).
    Devuelve el dict de usuario si las credenciales son correctas, None si no.
    """
    user = get_user(username)
    if not user:
        return None

    password_hash = user.get("password_hash", "")
    if password_hash:
        if not check_password_hash(password_hash, password):
            return None
        return user

    # Fallback para registros legacy con password en texto plano
    if user.get("password") != password:
        return None
    return user


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------

def create_token(user: dict, secret_key: str) -> str:
    now = datetime.utcnow()
    payload = {
        "sub": user.get("username"),
        "name": user.get("name"),
        "role": user.get("role"),
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXP_HOURS),
    }
    token = jwt.encode(payload, secret_key, algorithm=JWT_ALG)
    if isinstance(token, bytes):
        return token.decode("utf-8")
    return token


def decode_token(token: str, secret_key: str) -> dict:
    return jwt.decode(token, secret_key, algorithms=[JWT_ALG])


def get_token_from_request() -> str | None:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    token = request.cookies.get("access_token")
    if token:
        return token
    return None


def try_get_user_from_request() -> dict | None:
    token = get_token_from_request()
    if not token:
        return None
    try:
        payload = decode_token(token, current_app.config["SECRET_KEY"])
    except Exception:
        return None
    return {
        "username": payload.get("sub"),
        "name": payload.get("name"),
        "role": payload.get("role"),
    }


# ---------------------------------------------------------------------------
# Decorador de autorizacion
# ---------------------------------------------------------------------------

def require_auth(roles=None, redirect_to_login=False):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            token = get_token_from_request()
            if not token:
                if redirect_to_login:
                    return redirect("/auth/login")
                return jsonify({"error": "Unauthorized"}), 401
            try:
                payload = decode_token(token, current_app.config["SECRET_KEY"])
            except Exception:
                if redirect_to_login:
                    return redirect("/auth/login")
                return jsonify({"error": "Invalid token"}), 401

            role = payload.get("role")
            if roles and role not in roles:
                if redirect_to_login:
                    return redirect("/dashboard")
                return jsonify({"error": "Forbidden"}), 403

            g.current_user = {
                "username": payload.get("sub"),
                "name": payload.get("name"),
                "role": role,
            }
            return fn(*args, **kwargs)

        return wrapper

    return decorator
