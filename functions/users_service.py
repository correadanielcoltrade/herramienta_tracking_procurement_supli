"""
users_service.py - Gestion de usuarios via almacenamiento JSON primario.

Cada registro en ts_users.json tiene la estructura:
    {"id": int, "created_at": str, "updated_at": str, "data_json": {campos usuario}}
"""

from werkzeug.security import generate_password_hash

from queries.storage import (
    delete_record,
    get_record_by_field,
    load_records,
    make_record,
    next_id,
    save_records,
)

FILE_KEY = "ts_users"
VALID_ROLES = {"ADMIN", "USER"}


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _record_to_user(record: dict) -> dict:
    """Extrae el dict de usuario a partir de un registro de storage."""
    return record.get("data_json", {})


def _all_users() -> list:
    """Devuelve la lista plana de dicts de usuario."""
    return [_record_to_user(r) for r in load_records(FILE_KEY)]


def _count_admins(users: list) -> int:
    return sum(1 for u in users if u.get("role") == "ADMIN")


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------

def load_users() -> list:
    """Devuelve todos los usuarios (dicts con username, name, role, password_hash)."""
    return _all_users()


def save_users(users: list) -> None:
    """
    Persiste la lista completa de dicts de usuario.
    Reconstruye los registros de storage manteniendo id/timestamps existentes.
    """
    records = load_records(FILE_KEY)
    # Construir indice username -> record existente
    existing_by_username = {
        r.get("data_json", {}).get("username", ""): r
        for r in records
    }

    new_records = []
    next_auto_id = next_id(FILE_KEY)
    for user in users:
        username = user.get("username", "")
        existing = existing_by_username.get(username)
        if existing:
            # Actualizar data_json pero conservar id y created_at
            from queries.storage import _now_iso
            new_rec = dict(existing)
            new_rec["data_json"] = dict(user)
            new_rec["updated_at"] = _now_iso()
            new_records.append(new_rec)
        else:
            new_rec = make_record(next_auto_id, dict(user))
            new_records.append(new_rec)
            next_auto_id += 1

    save_records(FILE_KEY, new_records)


def list_users() -> list:
    """Devuelve lista de usuarios con solo username, name y role."""
    return [
        {"username": u.get("username"), "name": u.get("name"), "role": u.get("role")}
        for u in _all_users()
    ]


def create_user(payload: dict):
    """
    Crea un nuevo usuario.
    Retorna (user_dict, None) o (None, mensaje_error).
    """
    username = (payload.get("username") or "").strip()
    if not username:
        return None, "Username requerido"

    existing_record = get_record_by_field(FILE_KEY, "username", username)
    if existing_record:
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

    records = load_records(FILE_KEY)
    new_id = next_id(FILE_KEY)
    records.append(make_record(new_id, user))
    save_records(FILE_KEY, records)
    return user, None


def update_user(username: str, payload: dict):
    """
    Actualiza un usuario existente.
    Retorna (user_dict, None) o (None, mensaje_error).
    """
    records = load_records(FILE_KEY)
    for idx, record in enumerate(records):
        user = record.get("data_json", {})
        if user.get("username") != username:
            continue

        # Actualizar nombre
        name = payload.get("name")
        if name is not None:
            user["name"] = str(name).strip()

        # Actualizar rol
        role = payload.get("role")
        if role in VALID_ROLES:
            if role != user.get("role") and user.get("role") == "ADMIN":
                all_users = [r.get("data_json", {}) for r in records]
                if _count_admins(all_users) <= 1:
                    return None, "Debe existir al menos un ADMIN"
            user["role"] = role

        # Actualizar password
        password = payload.get("password")
        if password:
            user["password_hash"] = generate_password_hash(str(password))
            user.pop("password", None)

        from queries.storage import _now_iso
        records[idx]["data_json"] = user
        records[idx]["updated_at"] = _now_iso()
        save_records(FILE_KEY, records)
        return user, None

    return None, "Usuario no encontrado"


def delete_user(username: str):
    """
    Elimina un usuario por username.
    Retorna (True, None) o (False, mensaje_error).
    """
    records = load_records(FILE_KEY)
    record_to_delete = None
    record_id = None

    for record in records:
        user = record.get("data_json", {})
        if user.get("username") == username:
            record_to_delete = user
            record_id = record.get("id")
            break

    if not record_to_delete:
        return False, "Usuario no encontrado"

    if record_to_delete.get("role") == "ADMIN":
        all_users = [r.get("data_json", {}) for r in records]
        if _count_admins(all_users) <= 1:
            return False, "Debe existir al menos un ADMIN"

    delete_record(FILE_KEY, record_id)
    return True, None
