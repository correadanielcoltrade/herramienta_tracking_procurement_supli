import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


def db_enabled() -> bool:
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("DB_HOST"))


def _get_sslmode() -> str:
    return os.environ.get("DB_SSLMODE", "require")


def _connection_kwargs():
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return {"dsn": dsn, "sslmode": _get_sslmode()}
    return {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT", "5432"),
        "sslmode": _get_sslmode(),
    }


def get_conn():
    params = _connection_kwargs()
    if "dsn" in params:
        return psycopg2.connect(
            params["dsn"], sslmode=params["sslmode"], cursor_factory=RealDictCursor
        )
    return psycopg2.connect(cursor_factory=RealDictCursor, **params)


def execute(query: str, params=None, fetchone=False, fetchall=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if fetchone:
                return cur.fetchone()
            if fetchall:
                return cur.fetchall()
    return None


def init_db():
    if not db_enabled():
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trackingsupli_users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    name TEXT,
                    role TEXT NOT NULL CHECK (role IN ('ADMIN', 'USER')),
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trackingsupli_shipments (
                    id UUID PRIMARY KEY,
                    imp TEXT,
                    proveedor TEXT,
                    estado_imp TEXT,
                    tipo_compra TEXT,
                    fecha_llegada DATE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trackingsupli_shipment_products (
                    id SERIAL PRIMARY KEY,
                    shipment_id UUID NOT NULL REFERENCES trackingsupli_shipments(id) ON DELETE CASCADE,
                    producto TEXT,
                    marca TEXT,
                    upc TEXT,
                    sku TEXT,
                    q_total INTEGER,
                    costo_fob_usd NUMERIC,
                    costo_proyectado_ddp NUMERIC,
                    retail INTEGER,
                    resellers INTEGER,
                    corporativo INTEGER,
                    ecommerce INTEGER,
                    telcom INTEGER,
                    libre INTEGER,
                    confirmacion_cantidades_recibidas TEXT,
                    observaciones TEXT
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trackingsupli_shipment_products_shipment_id ON trackingsupli_shipment_products (shipment_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trackingsupli_shipments_imp ON trackingsupli_shipments (imp)"
            )


def utc_iso(dt_value):
    if not dt_value:
        return None
    if isinstance(dt_value, str):
        return dt_value
    if isinstance(dt_value, datetime):
        return dt_value.replace(tzinfo=None).isoformat()
    return str(dt_value)
