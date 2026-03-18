import os
from datetime import datetime, timedelta
from functools import wraps

import jwt
from flask import current_app, g, jsonify, redirect, request
from werkzeug.security import check_password_hash, generate_password_hash

from queries.json_store import read_json, write_json
from queries.db import db_enabled, execute, init_db

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
USERS_FILE = os.path.join(DATA_DIR, "users.json")

JWT_ALG = "HS256"
JWT_EXP_HOURS = 8


def ensure_users_file():
    if os.path.exists(USERS_FILE):
        return
    users = [
        {
            "username": "admin",
            "name": "Administrador",
            "role": "ADMIN",
            "password_hash": generate_password_hash("admin123"),
        },
        {
            "username": "user",
            "name": "Usuario",
            "role": "USER",
            "password_hash": generate_password_hash("user123"),
        },
    ]
    write_json(USERS_FILE, users)


def load_users():
    if db_enabled():
        init_db()
        ensure_default_users_db()
        rows = execute(
            "SELECT username, name, role, password_hash FROM trackingsupli_users ORDER BY username",
            fetchall=True,
        )
        return rows or []
    ensure_users_file()
    return read_json(USERS_FILE, [])


def get_user(username: str):
    if db_enabled():
        init_db()
        ensure_default_users_db()
        return execute(
            "SELECT username, name, role, password_hash FROM trackingsupli_users WHERE username = %s",
            (username,),
            fetchone=True,
        )
    users = load_users()
    for user in users:
        if user.get("username") == username:
            return user
    return None


def authenticate(username: str, password: str):
    user = get_user(username)
    if not user:
        return None
    password_hash = user.get("password_hash", "")
    if password_hash:
        if not check_password_hash(password_hash, password):
            return None
        return user
    if user.get("password") != password:
        return None
    return user


def ensure_default_users_db():
    count_row = execute(
        "SELECT COUNT(*) AS count FROM trackingsupli_users", fetchone=True
    )
    if count_row and count_row.get("count", 0) > 0:
        return
    admin_hash = generate_password_hash("admin123")
    user_hash = generate_password_hash("user123")
    execute(
        """
        INSERT INTO trackingsupli_users (username, name, role, password_hash)
        VALUES
            (%s, %s, %s, %s),
            (%s, %s, %s, %s)
        """,
        (
            "admin",
            "Administrador",
            "ADMIN",
            admin_hash,
            "user",
            "Usuario",
            "USER",
            user_hash,
        ),
    )


def create_token(user, secret_key: str):
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


def decode_token(token: str, secret_key: str):
    return jwt.decode(token, secret_key, algorithms=[JWT_ALG])


def get_token_from_request():
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    token = request.cookies.get("access_token")
    if token:
        return token
    return None


def try_get_user_from_request():
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
