"""
storage.py - Capa de persistencia unificada para Tracking de Embarques.

Comportamiento:
- Almacenamiento PRIMARIO: archivos JSON en data/  (lectura siempre rapida)
- Almacenamiento SECUNDARIO: PostgreSQL, si DATABASE_URL o DB_HOST estan en .env
- Escritura DUAL: cada save_records escribe en JSON Y en DB (si esta disponible)
- Si la DB falla, el sistema continua usando JSON sin interrumpir el servicio
- Cache en memoria con TTL configurable via STORAGE_CACHE_TTL (segundos, 0=sin TTL)

Formato de cada registro:
{
    "id": <int>,
    "created_at": "<iso8601>",
    "updated_at": "<iso8601>",
    "data_json": { ...campos del dominio... }
}

Tablas en DB (prefijo configurable via DB_TABLE_PREFIX, default "trackingsupli"):
    trackingsupli_shipments
    trackingsupli_users
"""

import json
import os
import threading
import time
from datetime import datetime, timezone

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, execute_values, Json
except Exception:
    psycopg2 = None
    RealDictCursor = None
    execute_values = None
    Json = None

# ---------------------------------------------------------------------------
# Configuracion de archivos y tablas
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

DATA_FILES = {
    "ts_shipments": "ts_shipments.json",
    "ts_users":     "ts_users.json",
}

DB_TABLES = {
    "ts_shipments": "shipments",
    "ts_users":     "users",
}

CACHE_TTL = int(os.environ.get("STORAGE_CACHE_TTL", "0") or 0)

# Cache de resolucion de nombres de tabla (evita resolver en cada request)
_TABLE_RESOLUTION: dict = {}

# ---------------------------------------------------------------------------
# Cache en memoria
# ---------------------------------------------------------------------------

_cache_lock = threading.Lock()
_cache: dict = {}
_cache_ts: dict = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _cache_get(file_key: str):
    with _cache_lock:
        if file_key not in _cache:
            return None
        if CACHE_TTL > 0:
            age = time.time() - _cache_ts.get(file_key, 0)
            if age > CACHE_TTL:
                del _cache[file_key]
                _cache_ts.pop(file_key, None)
                return None
        return _cache[file_key]


def _cache_set(file_key: str, records: list) -> None:
    with _cache_lock:
        _cache[file_key] = records
        _cache_ts[file_key] = time.time()


def _cache_invalidate(file_key: str) -> None:
    with _cache_lock:
        _cache.pop(file_key, None)
        _cache_ts.pop(file_key, None)


# ---------------------------------------------------------------------------
# Helpers JSON
# ---------------------------------------------------------------------------

def _file_path(file_key: str) -> str:
    filename = DATA_FILES.get(file_key)
    if not filename:
        raise KeyError(f"file_key desconocido: {file_key!r}. Opciones: {list(DATA_FILES)}")
    return os.path.join(DATA_DIR, filename)


def _read_json_file(path: str) -> list:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _write_json_file(path: str, records: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except OSError:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


# ---------------------------------------------------------------------------
# Configuracion de DB
# ---------------------------------------------------------------------------

def _db_config():
    url = os.environ.get("DATABASE_URL")
    if url:
        return {"url": url, "sslmode": os.environ.get("DB_SSLMODE") or None}

    host     = os.environ.get("DB_HOST")
    name     = os.environ.get("DB_NAME")
    user     = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    port     = os.environ.get("DB_PORT") or "5432"
    sslmode  = os.environ.get("DB_SSLMODE") or "require"

    if not (host and name and user and password):
        return None

    return {"host": host, "name": name, "user": user,
            "password": password, "port": port, "sslmode": sslmode}


def _db_enabled() -> bool:
    return _db_config() is not None and psycopg2 is not None


def _db_table_prefix() -> str:
    prefix = os.environ.get("DB_TABLE_PREFIX") or "trackingsupli"
    prefix = prefix.strip()
    prefix = "".join(ch for ch in prefix if ch.isalnum() or ch == "_")
    if prefix and not prefix.endswith("_"):
        prefix = f"{prefix}_"
    return prefix


def _db_connect():
    config = _db_config()
    if config is None or psycopg2 is None:
        return None
    if "url" in config:
        sslmode = config.get("sslmode")
        conn = psycopg2.connect(config["url"], sslmode=sslmode) if sslmode else psycopg2.connect(config["url"])
    else:
        conn = psycopg2.connect(
            dbname=config["name"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            sslmode=config["sslmode"],
        )
    conn.autocommit = False
    return conn


def _db_table_name(file_key: str) -> str:
    return f"{_db_table_prefix()}{DB_TABLES[file_key]}"


def _table_candidates(file_key: str) -> list:
    base   = DB_TABLES[file_key]
    prefix = _db_table_prefix()
    if not prefix:
        return [base]
    return [f"{prefix}{base}", base]


def _resolve_table(file_key: str, conn) -> str:
    cached = _TABLE_RESOLUTION.get(file_key)
    if cached:
        return cached

    candidates = _table_candidates(file_key)
    if len(candidates) == 1:
        _TABLE_RESOLUTION[file_key] = candidates[0]
        return candidates[0]

    prefixed, fallback = candidates[0], candidates[1]
    prefixed_exists = fallback_exists = False
    prefixed_count = fallback_count = 0

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (prefixed,))
        prefixed_exists = cur.fetchone()[0] is not None
        cur.execute("SELECT to_regclass(%s)", (fallback,))
        fallback_exists = cur.fetchone()[0] is not None

        if prefixed_exists:
            cur.execute(f"SELECT COUNT(*) FROM {prefixed}")
            prefixed_count = cur.fetchone()[0]
        if fallback_exists:
            cur.execute(f"SELECT COUNT(*) FROM {fallback}")
            fallback_count = cur.fetchone()[0]

    if prefixed_exists and prefixed_count > 0:
        chosen = prefixed
    elif fallback_exists and fallback_count > 0:
        chosen = fallback
    elif prefixed_exists:
        chosen = prefixed
    elif fallback_exists:
        chosen = fallback
    else:
        chosen = prefixed

    _TABLE_RESOLUTION[file_key] = chosen
    return chosen


def _ensure_table(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id         INTEGER PRIMARY KEY,
                created_at TEXT,
                updated_at TEXT,
                data_json  JSONB
            )
            """
        )
    conn.commit()


def _deserialize_json(value):
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Escritura en DB (silenciosa si falla)
# ---------------------------------------------------------------------------

def _sync_to_db(file_key: str, records: list) -> None:
    """
    Escribe todos los registros en la tabla de DB correspondiente.
    Fallo completamente silencioso: si la DB no esta disponible o hay error,
    el sistema sigue funcionando con JSON sin interrupciones.
    """
    if not _db_enabled():
        return
    try:
        conn = _db_connect()
        if conn is None:
            return
        try:
            table_name = _resolve_table(file_key, conn)
            _ensure_table(conn, table_name)
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name}")
                if records:
                    rows = [
                        (
                            r.get("id"),
                            r.get("created_at"),
                            r.get("updated_at"),
                            Json(r.get("data_json") or {}),
                        )
                        for r in records
                    ]
                    execute_values(
                        cur,
                        f"INSERT INTO {table_name} (id, created_at, updated_at, data_json) VALUES %s",
                        rows,
                    )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # Fallo silencioso: JSON es la fuente de verdad
        pass


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------

def load_records(file_key: str) -> list:
    """
    Carga todos los registros.
    Siempre lee desde JSON local (rapido, con cache).
    Devuelve lista de dicts: {"id": int, "created_at": str, "updated_at": str, "data_json": dict}
    """
    cached = _cache_get(file_key)
    if cached is not None:
        return cached

    path = _file_path(file_key)
    records = _read_json_file(path)
    _cache_set(file_key, records)
    return records


def save_records(file_key: str, records: list) -> None:
    """
    Persiste la lista completa de registros.
    1. Escribe en JSON local (atomico via .tmp)
    2. Escribe en DB si esta disponible (silencioso si falla)
    3. Actualiza cache en memoria
    """
    path = _file_path(file_key)
    _write_json_file(path, records)
    _cache_invalidate(file_key)
    _cache_set(file_key, records)
    # Escritura dual: sincroniza con DB
    _sync_to_db(file_key, records)


def next_id(file_key: str) -> int:
    """Genera el siguiente ID entero correlativo."""
    records = load_records(file_key)
    if not records:
        return 1
    ids = [r.get("id", 0) for r in records if isinstance(r.get("id"), int)]
    return (max(ids) + 1) if ids else 1


def make_record(record_id: int, data: dict, created_at: str = None, updated_at: str = None) -> dict:
    """Construye un registro con la estructura estandar."""
    now = _now_iso()
    return {
        "id":         record_id,
        "created_at": created_at or now,
        "updated_at": updated_at or now,
        "data_json":  data,
    }


def get_record_by_id(file_key: str, record_id) -> dict | None:
    """Devuelve el registro cuyo 'id' coincida, o None."""
    for r in load_records(file_key):
        if str(r.get("id")) == str(record_id):
            return r
    return None


def get_record_by_field(file_key: str, field: str, value) -> dict | None:
    """Devuelve el primer registro cuyo data_json[field] coincida con value, o None."""
    for r in load_records(file_key):
        if str(r.get("data_json", {}).get(field, "")) == str(value):
            return r
    return None


def upsert_record(file_key: str, record_id, data: dict) -> dict:
    """Actualiza el registro con ese id si existe, o lo crea. Devuelve el registro resultante."""
    records = load_records(file_key)
    now = _now_iso()
    for idx, r in enumerate(records):
        if str(r.get("id")) == str(record_id):
            records[idx]["data_json"] = data
            records[idx]["updated_at"] = now
            save_records(file_key, records)
            return records[idx]
    new_rec = make_record(record_id, data)
    records.append(new_rec)
    save_records(file_key, records)
    return new_rec


def delete_record(file_key: str, record_id) -> bool:
    """Elimina el registro con ese id. Devuelve True si se elimino."""
    records = load_records(file_key)
    new_records = [r for r in records if str(r.get("id")) != str(record_id)]
    if len(new_records) == len(records):
        return False
    save_records(file_key, new_records)
    return True


# ---------------------------------------------------------------------------
# Utilidad: migrar JSON -> DB manualmente
# ---------------------------------------------------------------------------

def migrate_json_to_db() -> dict:
    """
    Fuerza la sincronizacion de todos los archivos JSON hacia la DB.
    Util para poblar la DB desde cero.
    Lanza RuntimeError si la DB no esta configurada.
    """
    if not _db_enabled():
        raise RuntimeError(
            "DB no configurada. Agrega DATABASE_URL o DB_HOST/DB_NAME/DB_USER/DB_PASSWORD en .env"
        )
    summary = {}
    for file_key in DATA_FILES:
        records = _read_json_file(_file_path(file_key))
        _sync_to_db(file_key, records)
        summary[file_key] = {
            "table":   _db_table_name(file_key),
            "records": len(records),
        }
    return summary
