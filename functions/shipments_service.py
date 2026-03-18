import os
import time
import uuid
from datetime import datetime
from io import BytesIO

import pandas as pd

from queries.json_store import read_json, write_json
from queries.db import db_enabled, execute, get_conn, init_db, utc_iso

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
SHIPMENTS_FILE = os.path.join(DATA_DIR, "shipments.json")
CACHE_FILE = os.environ.get(
    "SHIPMENTS_CACHE_FILE", os.path.join(DATA_DIR, "shipments_cache.json")
)
CACHE_MODE = os.environ.get("SHIPMENTS_CACHE_MODE", "file").strip().lower()

CACHE_TTL_SECONDS = int(os.environ.get("SHIPMENTS_CACHE_TTL", "0") or 0)
_SHIPMENTS_CACHE = None
_SHIPMENTS_CACHE_AT = 0.0

EXCEL_COLUMNS = [
    "IMP",
    "PROVEEDOR",
    "ESTADO_IMP",
    "TIPO_COMPRA",
    "FECHA_LLEGADA",
    "PRODUCTO",
    "MARCA",
    "UPC",
    "SKU",
    "Q_TOTAL",
    "COSTO_FOB_USD",
    "COSTO_PROYECTADO_DDP",
    "RETAIL",
    "RESELLERS",
    "CORPORATIVO",
    "ECOMMERCE",
    "TELCOM",
    "LIBRE",
    "CONFIRMACION_CANTIDADES_RECIBIDAS",
    "OBSERVACIONES",
]

HEADER_MAP = {
    "IMP": "imp",
    "PROVEEDOR": "proveedor",
    "ESTADO_IMP": "estado_imp",
    "TIPO_COMPRA": "tipo_compra",
    "FECHA_LLEGADA": "fecha_llegada",
    "PRODUCTO": "producto",
    "MARCA": "marca",
    "UPC": "upc",
    "SKU": "sku",
    "Q_TOTAL": "q_total",
    "COSTO_FOB_USD": "costo_fob_usd",
    "COSTO_PROYECTADO_DDP": "costo_proyectado_ddp",
    "RETAIL": "retail",
    "RESELLERS": "resellers",
    "CORPORATIVO": "corporativo",
    "ECOMMERCE": "ecommerce",
    "TELCOM": "telcom",
    "LIBRE": "libre",
    "CONFIRMACION_CANTIDADES_RECIBIDAS": "confirmacion_cantidades_recibidas",
    "OBSERVACIONES": "observaciones",
}


def _parse_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _date_to_str(value):
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _shipment_row_to_dict(row):
    return {
        "id": str(row.get("id")),
        "imp": row.get("imp") or "",
        "proveedor": row.get("proveedor") or "",
        "estado_imp": row.get("estado_imp") or "",
        "tipo_compra": row.get("tipo_compra") or "",
        "fecha_llegada": _date_to_str(row.get("fecha_llegada")),
        "productos": [],
        "created_at": utc_iso(row.get("created_at")),
        "updated_at": utc_iso(row.get("updated_at")),
    }


def _product_row_to_dict(row):
    return {
        "producto": row.get("producto") or "",
        "marca": row.get("marca") or "",
        "upc": row.get("upc") or "",
        "sku": row.get("sku") or "",
        "q_total": row.get("q_total") or 0,
        "costo_fob_usd": float(row.get("costo_fob_usd") or 0),
        "costo_proyectado_ddp": float(row.get("costo_proyectado_ddp") or 0),
        "retail": row.get("retail") or 0,
        "resellers": row.get("resellers") or 0,
        "corporativo": row.get("corporativo") or 0,
        "ecommerce": row.get("ecommerce") or 0,
        "telcom": row.get("telcom") or 0,
        "libre": row.get("libre") or 0,
        "confirmacion_cantidades_recibidas": row.get("confirmacion_cantidades_recibidas")
        or "",
        "observaciones": row.get("observaciones") or "",
    }


def _coerce_product(product):
    return {
        "producto": _safe_str(product.get("producto")),
        "marca": _safe_str(product.get("marca")),
        "upc": _safe_str(product.get("upc")),
        "sku": _safe_str(product.get("sku")),
        "q_total": _to_int(product.get("q_total")),
        "costo_fob_usd": _to_number(product.get("costo_fob_usd")),
        "costo_proyectado_ddp": _to_number(product.get("costo_proyectado_ddp")),
        "retail": _to_int(product.get("retail")),
        "resellers": _to_int(product.get("resellers")),
        "corporativo": _to_int(product.get("corporativo")),
        "ecommerce": _to_int(product.get("ecommerce")),
        "telcom": _to_int(product.get("telcom")),
        "libre": _to_int(product.get("libre")),
        "confirmacion_cantidades_recibidas": _safe_str(
            product.get("confirmacion_cantidades_recibidas")
        ),
        "observaciones": _safe_str(product.get("observaciones")),
    }


def _matches_text(value, term):
    if not term:
        return True
    return term in (value or "").lower()


def _within_range(date_str, from_date, to_date):
    if not from_date and not to_date:
        return True
    if not date_str:
        return False
    try:
        date_value = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return False
    if from_date and date_value < from_date:
        return False
    if to_date and date_value > to_date:
        return False
    return True


def _normalize_filter_value(value):
    return (value or "").strip().lower()


def _filter_shipments_locally(shipments, filters):
    if not filters:
        return shipments

    imp = _normalize_filter_value(filters.get("imp"))
    proveedor = _normalize_filter_value(filters.get("proveedor"))
    estado = _normalize_filter_value(filters.get("estado"))
    tipo_compra = _normalize_filter_value(filters.get("tipo_compra"))
    producto = _normalize_filter_value(filters.get("producto"))
    marca = _normalize_filter_value(filters.get("marca"))
    sku = _normalize_filter_value(filters.get("sku"))
    canal = _normalize_filter_value(filters.get("canal"))
    fecha = (filters.get("fecha") or "").strip()
    fecha_desde = (filters.get("fecha_desde") or "").strip()
    fecha_hasta = (filters.get("fecha_hasta") or "").strip()

    from_date = None
    to_date = None
    if fecha_desde:
        try:
            from_date = datetime.strptime(fecha_desde, "%Y-%m-%d")
        except ValueError:
            from_date = None
    if fecha_hasta:
        try:
            to_date = datetime.strptime(fecha_hasta, "%Y-%m-%d")
        except ValueError:
            to_date = None

    filtered = []
    product_filter_active = bool(producto or marca or sku or canal)

    for shipment in shipments:
        if imp and not _matches_text((shipment.get("imp") or "").lower(), imp):
            continue
        if proveedor and not _matches_text((shipment.get("proveedor") or "").lower(), proveedor):
            continue
        if estado and not _matches_text((shipment.get("estado_imp") or "").lower(), estado):
            continue
        if tipo_compra and (shipment.get("tipo_compra") or "").lower() != tipo_compra:
            continue
        if fecha and shipment.get("fecha_llegada") != fecha:
            continue
        if not _within_range(shipment.get("fecha_llegada", ""), from_date, to_date):
            continue

        if product_filter_active:
            products = shipment.get("productos", [])
            match = False
            for product in products:
                if producto and not _matches_text((product.get("producto") or "").lower(), producto):
                    continue
                if marca and not _matches_text((product.get("marca") or "").lower(), marca):
                    continue
                if sku and not _matches_text((product.get("sku") or "").lower(), sku):
                    continue
                if canal and float(product.get(canal) or 0) <= 0:
                    continue
                match = True
                break
            if not match:
                continue
        filtered.append(shipment)

    return filtered


def list_shipments_summary(filters=None):
    if db_enabled():
        init_db()
        if _cache_enabled():
            shipments = load_shipments()
            filtered = _filter_shipments_locally(shipments, filters or {})
            result = []
            for shipment in filtered:
                total_qty, _ = compute_totals(shipment)
                result.append(
                    {
                        "id": shipment.get("id"),
                        "imp": shipment.get("imp"),
                        "proveedor": shipment.get("proveedor"),
                        "estado_imp": shipment.get("estado_imp"),
                        "tipo_compra": shipment.get("tipo_compra"),
                        "fecha_llegada": shipment.get("fecha_llegada"),
                        "created_at": shipment.get("created_at"),
                        "updated_at": shipment.get("updated_at"),
                        "total_qty": total_qty,
                    }
                )
            return result
        clauses = []
        params = []

        def add_clause(sql, value=None):
            clauses.append(sql)
            if value is not None:
                params.append(value)

        imp = _normalize_filter_value(filters.get("imp") if filters else "")
        proveedor = _normalize_filter_value(filters.get("proveedor") if filters else "")
        estado = _normalize_filter_value(filters.get("estado") if filters else "")
        tipo_compra = _normalize_filter_value(filters.get("tipo_compra") if filters else "")
        fecha = (filters.get("fecha") if filters else "") or ""
        fecha_desde = (filters.get("fecha_desde") if filters else "") or ""
        fecha_hasta = (filters.get("fecha_hasta") if filters else "") or ""
        producto = _normalize_filter_value(filters.get("producto") if filters else "")
        marca = _normalize_filter_value(filters.get("marca") if filters else "")
        sku = _normalize_filter_value(filters.get("sku") if filters else "")
        canal = _normalize_filter_value(filters.get("canal") if filters else "")

        if imp:
            add_clause("LOWER(s.imp) LIKE %s", f"%{imp}%")
        if proveedor:
            add_clause("LOWER(s.proveedor) LIKE %s", f"%{proveedor}%")
        if estado:
            add_clause("LOWER(s.estado_imp) LIKE %s", f"%{estado}%")
        if tipo_compra:
            add_clause("LOWER(s.tipo_compra) = %s", tipo_compra)
        if fecha:
            add_clause("s.fecha_llegada = %s", fecha)
        if fecha_desde:
            add_clause("s.fecha_llegada >= %s", fecha_desde)
        if fecha_hasta:
            add_clause("s.fecha_llegada <= %s", fecha_hasta)

        product_filters = []
        product_params = []
        if producto:
            product_filters.append("LOWER(fp.producto) LIKE %s")
            product_params.append(f"%{producto}%")
        if marca:
            product_filters.append("LOWER(fp.marca) LIKE %s")
            product_params.append(f"%{marca}%")
        if sku:
            product_filters.append("LOWER(fp.sku) LIKE %s")
            product_params.append(f"%{sku}%")
        if canal and canal in {"retail", "resellers", "corporativo", "ecommerce", "telcom", "libre"}:
            product_filters.append(f"fp.{canal} > 0")

        if product_filters:
            clauses.append(
                "EXISTS (SELECT 1 FROM trackingsupli_shipment_products fp WHERE fp.shipment_id = s.id AND "
                + " AND ".join(product_filters)
                + ")"
            )
            params.extend(product_params)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT
                s.id,
                s.imp,
                s.proveedor,
                s.estado_imp,
                s.tipo_compra,
                s.fecha_llegada,
                s.created_at,
                s.updated_at,
                COALESCE(SUM(p.q_total), 0) AS total_qty
            FROM trackingsupli_shipments s
            LEFT JOIN trackingsupli_shipment_products p ON p.shipment_id = s.id
            {where_sql}
            GROUP BY s.id, s.imp, s.proveedor, s.estado_imp, s.tipo_compra, s.fecha_llegada, s.created_at, s.updated_at
            ORDER BY s.created_at DESC
        """
        rows = execute(query, tuple(params), fetchall=True) or []
        result = []
        for row in rows:
            result.append(
                {
                    "id": str(row.get("id")),
                    "imp": row.get("imp") or "",
                    "proveedor": row.get("proveedor") or "",
                    "estado_imp": row.get("estado_imp") or "",
                    "tipo_compra": row.get("tipo_compra") or "",
                    "fecha_llegada": _date_to_str(row.get("fecha_llegada")),
                    "created_at": utc_iso(row.get("created_at")),
                    "updated_at": utc_iso(row.get("updated_at")),
                    "total_qty": int(row.get("total_qty") or 0),
                }
            )
        return result

    shipments = load_shipments()
    filtered = _filter_shipments_locally(shipments, filters or {})
    result = []
    for shipment in filtered:
        total_qty, _ = compute_totals(shipment)
        result.append(
            {
                "id": shipment.get("id"),
                "imp": shipment.get("imp"),
                "proveedor": shipment.get("proveedor"),
                "estado_imp": shipment.get("estado_imp"),
                "tipo_compra": shipment.get("tipo_compra"),
                "fecha_llegada": shipment.get("fecha_llegada"),
                "created_at": shipment.get("created_at"),
                "updated_at": shipment.get("updated_at"),
                "total_qty": total_qty,
            }
        )
    return result

def _cache_enabled():
    return db_enabled() and CACHE_MODE != "off"


def _cache_file_mtime():
    try:
        return os.path.getmtime(CACHE_FILE)
    except OSError:
        return 0.0


def _cache_is_stale():
    if not os.path.exists(CACHE_FILE):
        return True
    if CACHE_TTL_SECONDS <= 0:
        return False
    return (time.time() - _cache_file_mtime()) > CACHE_TTL_SECONDS


def _get_shipments_cache():
    global _SHIPMENTS_CACHE, _SHIPMENTS_CACHE_AT
    if _SHIPMENTS_CACHE is None:
        return None
    if CACHE_TTL_SECONDS > 0:
        if time.time() - _SHIPMENTS_CACHE_AT > CACHE_TTL_SECONDS:
            _SHIPMENTS_CACHE = None
            return None
    return _SHIPMENTS_CACHE


def _set_shipments_cache(data):
    global _SHIPMENTS_CACHE, _SHIPMENTS_CACHE_AT
    _SHIPMENTS_CACHE = data
    _SHIPMENTS_CACHE_AT = time.time()


def _invalidate_shipments_cache():
    global _SHIPMENTS_CACHE
    _SHIPMENTS_CACHE = None


def _load_cache_file():
    return read_json(CACHE_FILE, [])


def _save_cache_file(data):
    write_json(CACHE_FILE, data)


def _load_shipments_from_db():
    rows = execute(
        "SELECT * FROM trackingsupli_shipments ORDER BY created_at DESC",
        fetchall=True,
    )
    if not rows:
        return []
    shipment_map = {str(r.get("id")): _shipment_row_to_dict(r) for r in rows}
    ids = tuple(shipment_map.keys())
    if ids:
        products = execute(
            "SELECT * FROM trackingsupli_shipment_products WHERE shipment_id IN %s ORDER BY id",
            (ids,),
            fetchall=True,
        ) or []
        for product in products:
            shipment_id = str(product.get("shipment_id"))
            shipment = shipment_map.get(shipment_id)
            if shipment is not None:
                shipment["productos"].append(_product_row_to_dict(product))
    return list(shipment_map.values())


def _refresh_cache_from_db():
    data = _load_shipments_from_db()
    _save_cache_file(data)
    _set_shipments_cache(data)
    return data


def _refresh_cache_if_enabled():
    if _cache_enabled():
        _refresh_cache_from_db()
    else:
        _invalidate_shipments_cache()


def ensure_shipments_file():
    if db_enabled():
        init_db()
        return
    if os.path.exists(SHIPMENTS_FILE):
        return
    write_json(SHIPMENTS_FILE, [])


def load_shipments():
    if db_enabled():
        init_db()
        if _cache_enabled():
            cached = _get_shipments_cache()
            if cached is not None and not _cache_is_stale():
                return cached
            if not _cache_is_stale():
                data = _load_cache_file()
                _set_shipments_cache(data)
                return data
            return _refresh_cache_from_db()
        return _load_shipments_from_db()
    ensure_shipments_file()
    return read_json(SHIPMENTS_FILE, [])


def save_shipments(shipments):
    if db_enabled():
        return
    write_json(SHIPMENTS_FILE, shipments)


def _to_number(value, default=0):
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    if pd.isna(value):
        return default
    try:
        if isinstance(value, str) and value.strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    return int(_to_number(value, default=default))


def _to_date_str(value):
    if value is None or value == "":
        return ""
    if pd.isna(value):
        return ""
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _safe_str(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def compute_totals(shipment):
    total_qty = 0
    totals = {
        "retail": 0,
        "resellers": 0,
        "corporativo": 0,
        "ecommerce": 0,
        "telcom": 0,
        "libre": 0,
    }
    for product in shipment.get("productos", []):
        total_qty += _to_int(product.get("q_total"))
        for key in totals:
            totals[key] += _to_int(product.get(key))
    return total_qty, totals


def list_shipments():
    shipments = load_shipments()
    result = []
    for shipment in shipments:
        total_qty, _ = compute_totals(shipment)
        item = dict(shipment)
        item["total_qty"] = total_qty
        result.append(item)
    return result


def get_shipment(shipment_id: str):
    if db_enabled():
        init_db()
        if _cache_enabled():
            shipments = load_shipments()
            for shipment in shipments:
                if shipment.get("id") == shipment_id:
                    return shipment
            return None
        row = execute(
            "SELECT * FROM trackingsupli_shipments WHERE id = %s",
            (shipment_id,),
            fetchone=True,
        )
        if not row:
            return None
        shipment = _shipment_row_to_dict(row)
        products = execute(
            "SELECT * FROM trackingsupli_shipment_products WHERE shipment_id = %s ORDER BY id",
            (shipment_id,),
            fetchall=True,
        ) or []
        shipment["productos"] = [_product_row_to_dict(p) for p in products]
        return shipment
    shipments = load_shipments()
    for shipment in shipments:
        if shipment.get("id") == shipment_id:
            return shipment
    return None


def create_shipment(payload: dict):
    if db_enabled():
        init_db()
        shipment_id = str(uuid.uuid4())
        now = datetime.utcnow()
        fecha_llegada = _parse_date(payload.get("fecha_llegada", "").strip())
        productos = payload.get("productos", []) or []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trackingsupli_shipments (id, imp, proveedor, estado_imp, tipo_compra, fecha_llegada, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        shipment_id,
                        payload.get("imp", "").strip(),
                        payload.get("proveedor", "").strip(),
                        payload.get("estado_imp", "").strip(),
                        payload.get("tipo_compra", "").strip(),
                        fecha_llegada,
                        now,
                        now,
                    ),
                )
                for product in productos:
                    p = _coerce_product(product)
                    cur.execute(
                        """
                        INSERT INTO trackingsupli_shipment_products (
                            shipment_id, producto, marca, upc, sku, q_total,
                            costo_fob_usd, costo_proyectado_ddp, retail, resellers,
                            corporativo, ecommerce, telcom, libre,
                            confirmacion_cantidades_recibidas, observaciones
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            shipment_id,
                            p["producto"],
                            p["marca"],
                            p["upc"],
                            p["sku"],
                            p["q_total"],
                            p["costo_fob_usd"],
                            p["costo_proyectado_ddp"],
                            p["retail"],
                            p["resellers"],
                            p["corporativo"],
                            p["ecommerce"],
                            p["telcom"],
                            p["libre"],
                            p["confirmacion_cantidades_recibidas"],
                            p["observaciones"],
                        ),
                    )
        _refresh_cache_if_enabled()
        return get_shipment(shipment_id)

    shipments = load_shipments()
    now = datetime.utcnow().isoformat()
    shipment = {
        "id": str(uuid.uuid4()),
        "imp": payload.get("imp", "").strip(),
        "proveedor": payload.get("proveedor", "").strip(),
        "estado_imp": payload.get("estado_imp", "").strip(),
        "tipo_compra": payload.get("tipo_compra", "").strip(),
        "fecha_llegada": payload.get("fecha_llegada", "").strip(),
        "productos": payload.get("productos", []),
        "created_at": now,
        "updated_at": now,
    }
    shipments.append(shipment)
    save_shipments(shipments)
    return shipment


def update_shipment(shipment_id: str, payload: dict):
    if db_enabled():
        init_db()
        existing = execute(
            "SELECT * FROM trackingsupli_shipments WHERE id = %s",
            (shipment_id,),
            fetchone=True,
        )
        if not existing:
            return None
        imp = payload.get("imp", existing.get("imp") or "")
        proveedor = payload.get("proveedor", existing.get("proveedor") or "")
        estado_imp = payload.get("estado_imp", existing.get("estado_imp") or "")
        tipo_compra = payload.get("tipo_compra", existing.get("tipo_compra") or "")
        fecha_raw = payload.get(
            "fecha_llegada", _date_to_str(existing.get("fecha_llegada"))
        )
        fecha_llegada = _parse_date(fecha_raw.strip() if isinstance(fecha_raw, str) else fecha_raw)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE trackingsupli_shipments
                    SET imp = %s, proveedor = %s, estado_imp = %s, tipo_compra = %s,
                        fecha_llegada = %s, updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        str(imp).strip(),
                        str(proveedor).strip(),
                        str(estado_imp).strip(),
                        str(tipo_compra).strip(),
                        fecha_llegada,
                        datetime.utcnow(),
                        shipment_id,
                    ),
                )
                if "productos" in payload:
                    cur.execute(
                        "DELETE FROM trackingsupli_shipment_products WHERE shipment_id = %s",
                        (shipment_id,),
                    )
                    for product in payload.get("productos", []) or []:
                        p = _coerce_product(product)
                        cur.execute(
                            """
                            INSERT INTO trackingsupli_shipment_products (
                                shipment_id, producto, marca, upc, sku, q_total,
                                costo_fob_usd, costo_proyectado_ddp, retail, resellers,
                                corporativo, ecommerce, telcom, libre,
                                confirmacion_cantidades_recibidas, observaciones
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                shipment_id,
                                p["producto"],
                                p["marca"],
                                p["upc"],
                                p["sku"],
                                p["q_total"],
                                p["costo_fob_usd"],
                                p["costo_proyectado_ddp"],
                                p["retail"],
                                p["resellers"],
                                p["corporativo"],
                                p["ecommerce"],
                                p["telcom"],
                                p["libre"],
                                p["confirmacion_cantidades_recibidas"],
                                p["observaciones"],
                        ),
                    )
        _refresh_cache_if_enabled()
        return get_shipment(shipment_id)

    shipments = load_shipments()
    for idx, shipment in enumerate(shipments):
        if shipment.get("id") == shipment_id:
            shipment["imp"] = payload.get("imp", shipment.get("imp", "")).strip()
            shipment["proveedor"] = payload.get("proveedor", shipment.get("proveedor", "")).strip()
            shipment["estado_imp"] = payload.get("estado_imp", shipment.get("estado_imp", "")).strip()
            shipment["tipo_compra"] = payload.get("tipo_compra", shipment.get("tipo_compra", "")).strip()
            shipment["fecha_llegada"] = payload.get("fecha_llegada", shipment.get("fecha_llegada", "")).strip()
            shipment["productos"] = payload.get("productos", shipment.get("productos", []))
            shipment["updated_at"] = datetime.utcnow().isoformat()
            shipments[idx] = shipment
            save_shipments(shipments)
            return shipment
    return None


def delete_shipment(shipment_id: str):
    if db_enabled():
        init_db()
        result = execute(
            "SELECT 1 FROM trackingsupli_shipments WHERE id = %s",
            (shipment_id,),
            fetchone=True,
        )
        if not result:
            return False
        execute("DELETE FROM trackingsupli_shipments WHERE id = %s", (shipment_id,))
        _refresh_cache_if_enabled()
        return True
    shipments = load_shipments()
    new_shipments = [s for s in shipments if s.get("id") != shipment_id]
    if len(new_shipments) == len(shipments):
        return False
    save_shipments(new_shipments)
    return True


def bulk_delete(shipment_ids):
    if db_enabled():
        init_db()
        if not shipment_ids:
            return 0
        ids = tuple(shipment_ids)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM trackingsupli_shipments WHERE id IN %s", (ids,))
                deleted = cur.rowcount
        _refresh_cache_if_enabled()
        return deleted
    shipments = load_shipments()
    shipment_ids = set(shipment_ids)
    new_shipments = [s for s in shipments if s.get("id") not in shipment_ids]
    deleted = len(shipments) - len(new_shipments)
    save_shipments(new_shipments)
    return deleted


def _normalize_row(row, column_map):
    def get(col):
        key = column_map.get(col, "")
        return row.get(key, "")

    product = {
        "producto": _safe_str(get("PRODUCTO")),
        "marca": _safe_str(get("MARCA")),
        "upc": _safe_str(get("UPC")),
        "sku": _safe_str(get("SKU")),
        "q_total": _to_int(get("Q_TOTAL")),
        "costo_fob_usd": _to_number(get("COSTO_FOB_USD")),
        "costo_proyectado_ddp": _to_number(get("COSTO_PROYECTADO_DDP")),
        "retail": _to_int(get("RETAIL")),
        "resellers": _to_int(get("RESELLERS")),
        "corporativo": _to_int(get("CORPORATIVO")),
        "ecommerce": _to_int(get("ECOMMERCE")),
        "telcom": _to_int(get("TELCOM")),
        "libre": _to_int(get("LIBRE")),
        "confirmacion_cantidades_recibidas": _safe_str(
            get("CONFIRMACION_CANTIDADES_RECIBIDAS")
        ),
        "observaciones": _safe_str(get("OBSERVACIONES")),
    }
    shipment = {
        "imp": _safe_str(get("IMP")),
        "proveedor": _safe_str(get("PROVEEDOR")),
        "estado_imp": _safe_str(get("ESTADO_IMP")),
        "tipo_compra": _safe_str(get("TIPO_COMPRA")),
        "fecha_llegada": _to_date_str(get("FECHA_LLEGADA")),
    }
    return shipment, product


def import_from_excel(file_storage):
    df = pd.read_excel(file_storage)
    if df.empty:
        return {"created": 0, "updated": 0, "rows": 0}

    column_map = {str(col).strip().upper(): col for col in df.columns}
    if db_enabled():
        init_db()
        created = 0
        updated = 0
        imps = set()
        for _, row in df.iterrows():
            imp_value = _safe_str(row.get(column_map.get("IMP", "IMP")))
            if imp_value:
                imps.add(imp_value)

        existing_by_imp = {}
        if imps:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, imp FROM trackingsupli_shipments WHERE imp = ANY(%s)",
                        (list(imps),),
                    )
                    for row in cur.fetchall() or []:
                        existing_by_imp[row.get("imp")] = str(row.get("id"))

        with get_conn() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    shipment_data, product = _normalize_row(row_dict, column_map)
                    imp = shipment_data.get("imp")
                    if not imp:
                        continue
                    if imp in existing_by_imp:
                        shipment_id = existing_by_imp[imp]
                        new_imp = shipment_data.get("imp") or None
                        new_proveedor = shipment_data.get("proveedor") or None
                        new_estado = shipment_data.get("estado_imp") or None
                        new_tipo = shipment_data.get("tipo_compra") or None
                        new_fecha = _parse_date(shipment_data.get("fecha_llegada"))
                        cur.execute(
                            """
                            UPDATE trackingsupli_shipments
                            SET imp = COALESCE(%s, imp),
                                proveedor = COALESCE(%s, proveedor),
                                estado_imp = COALESCE(%s, estado_imp),
                                tipo_compra = COALESCE(%s, tipo_compra),
                                fecha_llegada = COALESCE(%s, fecha_llegada),
                                updated_at = %s
                            WHERE id = %s
                            """,
                            (
                                new_imp,
                                new_proveedor,
                                new_estado,
                                new_tipo,
                                new_fecha,
                                datetime.utcnow(),
                                shipment_id,
                            ),
                        )
                        p = _coerce_product(product)
                        cur.execute(
                            """
                            INSERT INTO trackingsupli_shipment_products (
                                shipment_id, producto, marca, upc, sku, q_total,
                                costo_fob_usd, costo_proyectado_ddp, retail, resellers,
                                corporativo, ecommerce, telcom, libre,
                                confirmacion_cantidades_recibidas, observaciones
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                shipment_id,
                                p["producto"],
                                p["marca"],
                                p["upc"],
                                p["sku"],
                                p["q_total"],
                                p["costo_fob_usd"],
                                p["costo_proyectado_ddp"],
                                p["retail"],
                                p["resellers"],
                                p["corporativo"],
                                p["ecommerce"],
                                p["telcom"],
                                p["libre"],
                                p["confirmacion_cantidades_recibidas"],
                                p["observaciones"],
                            ),
                        )
                        updated += 1
                    else:
                        shipment_id = str(uuid.uuid4())
                        fecha = _parse_date(shipment_data.get("fecha_llegada"))
                        cur.execute(
                            """
                            INSERT INTO trackingsupli_shipments (id, imp, proveedor, estado_imp, tipo_compra, fecha_llegada, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                shipment_id,
                                shipment_data.get("imp"),
                                shipment_data.get("proveedor"),
                                shipment_data.get("estado_imp"),
                                shipment_data.get("tipo_compra"),
                                fecha,
                                datetime.utcnow(),
                                datetime.utcnow(),
                            ),
                        )
                        p = _coerce_product(product)
                        cur.execute(
                            """
                            INSERT INTO trackingsupli_shipment_products (
                                shipment_id, producto, marca, upc, sku, q_total,
                                costo_fob_usd, costo_proyectado_ddp, retail, resellers,
                                corporativo, ecommerce, telcom, libre,
                                confirmacion_cantidades_recibidas, observaciones
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                shipment_id,
                                p["producto"],
                                p["marca"],
                                p["upc"],
                                p["sku"],
                                p["q_total"],
                                p["costo_fob_usd"],
                                p["costo_proyectado_ddp"],
                                p["retail"],
                                p["resellers"],
                                p["corporativo"],
                                p["ecommerce"],
                                p["telcom"],
                                p["libre"],
                                p["confirmacion_cantidades_recibidas"],
                                p["observaciones"],
                            ),
                        )
                        existing_by_imp[imp] = shipment_id
                        created += 1
        _refresh_cache_if_enabled()
        return {"created": created, "updated": updated, "rows": int(len(df.index))}

    shipments = load_shipments()
    existing_by_imp = {s.get("imp"): s for s in shipments if s.get("imp")}

    created = 0
    updated = 0

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        shipment_data, product = _normalize_row(row_dict, column_map)
        imp = shipment_data.get("imp")
        if not imp:
            continue

        if imp in existing_by_imp:
            shipment = existing_by_imp[imp]
            shipment.update({k: v for k, v in shipment_data.items() if v})
            shipment.setdefault("productos", [])
            shipment["productos"].append(product)
            shipment["updated_at"] = datetime.utcnow().isoformat()
            updated += 1
        else:
            new_shipment = {
                "id": str(uuid.uuid4()),
                "imp": shipment_data.get("imp"),
                "proveedor": shipment_data.get("proveedor"),
                "estado_imp": shipment_data.get("estado_imp"),
                "tipo_compra": shipment_data.get("tipo_compra"),
                "fecha_llegada": shipment_data.get("fecha_llegada"),
                "productos": [product],
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }
            shipments.append(new_shipment)
            existing_by_imp[imp] = new_shipment
            created += 1

    save_shipments(shipments)
    return {"created": created, "updated": updated, "rows": int(len(df.index))}


def export_to_excel(shipments=None):
    if shipments is None:
        shipments = load_shipments()
    rows = []
    for shipment in shipments:
        for product in shipment.get("productos", []):
            row = {
                "IMP": shipment.get("imp", ""),
                "PROVEEDOR": shipment.get("proveedor", ""),
                "ESTADO_IMP": shipment.get("estado_imp", ""),
                "TIPO_COMPRA": shipment.get("tipo_compra", ""),
                "FECHA_LLEGADA": shipment.get("fecha_llegada", ""),
                "PRODUCTO": product.get("producto", ""),
                "MARCA": product.get("marca", ""),
                "UPC": product.get("upc", ""),
                "SKU": product.get("sku", ""),
                "Q_TOTAL": product.get("q_total", 0),
                "COSTO_FOB_USD": product.get("costo_fob_usd", 0),
                "COSTO_PROYECTADO_DDP": product.get("costo_proyectado_ddp", 0),
                "RETAIL": product.get("retail", 0),
                "RESELLERS": product.get("resellers", 0),
                "CORPORATIVO": product.get("corporativo", 0),
                "ECOMMERCE": product.get("ecommerce", 0),
                "TELCOM": product.get("telcom", 0),
                "LIBRE": product.get("libre", 0),
                "CONFIRMACION_CANTIDADES_RECIBIDAS": product.get(
                    "confirmacion_cantidades_recibidas", ""
                ),
                "OBSERVACIONES": product.get("observaciones", ""),
            }
            rows.append(row)

    df = pd.DataFrame(rows, columns=EXCEL_COLUMNS)
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return output


def export_template():
    df = pd.DataFrame([], columns=EXCEL_COLUMNS)
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return output
