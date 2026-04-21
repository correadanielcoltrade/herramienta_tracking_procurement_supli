"""
shipments_service.py - Logica de negocio de embarques via almacenamiento JSON primario.

Cada registro en ts_shipments.json tiene la estructura:
    {"id": int, "created_at": str, "updated_at": str, "data_json": {campos embarque}}

Los campos de data_json son exactamente los mismos que antes:
    id (uuid str), imp, proveedor, estado_imp, tipo_compra, fecha_llegada,
    productos (lista), created_at, updated_at
"""

import uuid
from datetime import datetime
from io import BytesIO

import pandas as pd

from queries.storage import (
    _now_iso,
    load_records,
    make_record,
    next_id,
    save_records,
)

FILE_KEY = "ts_shipments"

EXCEL_COLUMNS = [
    "IMP",
    "PROVEEDOR",
    "ESTADO_IMP",
    "TIPO_COMPRA",
    "FECHA_LLEGADA",
    "FECHA_INICIAL_PROYECTADA",
    "NOVEDADES",
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
    "FECHA_INICIAL_PROYECTADA": "fecha_inicial_proyectada",
    "NOVEDADES": "novedades",
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


# ---------------------------------------------------------------------------
# Helpers de conversion
# ---------------------------------------------------------------------------

def _parse_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _date_to_str(value) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _to_number(value, default=0):
    if value is None:
        return default
    try:
        if isinstance(value, float) and pd.isna(value):
            return default
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    try:
        if isinstance(value, str) and value.strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0) -> int:
    return int(_to_number(value, default=default))


def _to_date_str(value) -> str:
    if value is None or value == "":
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _safe_str(value) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and pd.isna(value):
            return ""
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


# ---------------------------------------------------------------------------
# Helpers de estructura
# ---------------------------------------------------------------------------

def _coerce_product(product: dict) -> dict:
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


def _record_to_shipment(record: dict) -> dict:
    """Extrae el dict de embarque a partir de un registro de storage."""
    return record.get("data_json", {})


# ---------------------------------------------------------------------------
# Filtrado local
# ---------------------------------------------------------------------------

def _matches_text(value, term) -> bool:
    if not term:
        return True
    return term in (value or "").lower()


def _within_range(date_str, from_date, to_date) -> bool:
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


def _normalize_filter_value(value) -> str:
    return (value or "").strip().lower()


def _filter_shipments_locally(shipments: list, filters: dict) -> list:
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
                if producto and not _matches_text(
                    (product.get("producto") or "").lower(), producto
                ):
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


# ---------------------------------------------------------------------------
# Carga y guardado principal
# ---------------------------------------------------------------------------

def load_shipments() -> list:
    """Devuelve la lista plana de dicts de embarque desde ts_shipments.json."""
    records = load_records(FILE_KEY)
    return [_record_to_shipment(r) for r in records]


def save_shipments(shipments: list) -> None:
    """
    Persiste la lista completa de dicts de embarque.
    Reconstruye los registros de storage manteniendo id/timestamps existentes.
    """
    records = load_records(FILE_KEY)

    # Indice uuid -> record existente
    existing_by_uuid = {
        r.get("data_json", {}).get("id", ""): r
        for r in records
    }

    new_records = []
    next_auto_id = next_id(FILE_KEY)

    for shipment in shipments:
        ship_uuid = shipment.get("id", "")
        existing = existing_by_uuid.get(ship_uuid)
        if existing:
            new_rec = dict(existing)
            new_rec["data_json"] = dict(shipment)
            new_rec["updated_at"] = _now_iso()
            new_records.append(new_rec)
        else:
            new_rec = make_record(next_auto_id, dict(shipment))
            new_records.append(new_rec)
            next_auto_id += 1

    save_records(FILE_KEY, new_records)


# ---------------------------------------------------------------------------
# Calculo de totales
# ---------------------------------------------------------------------------

def compute_totals(shipment: dict):
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


# ---------------------------------------------------------------------------
# CRUD publico de embarques
# ---------------------------------------------------------------------------

def list_shipments() -> list:
    """Devuelve todos los embarques con total_qty calculado."""
    shipments = load_shipments()
    result = []
    for shipment in shipments:
        total_qty, _ = compute_totals(shipment)
        item = dict(shipment)
        item["total_qty"] = total_qty
        result.append(item)
    return result


def list_shipments_summary(filters=None) -> list:
    """Devuelve lista resumida de embarques, opcionalmente filtrada."""
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
                "fecha_inicial_proyectada": shipment.get("fecha_inicial_proyectada"),
                "novedades": shipment.get("novedades"),
                "created_at": shipment.get("created_at"),
                "updated_at": shipment.get("updated_at"),
                "total_qty": total_qty,
            }
        )
    return result


def get_shipment(shipment_id: str) -> dict | None:
    """Devuelve el embarque con ese uuid, o None."""
    for shipment in load_shipments():
        if shipment.get("id") == shipment_id:
            return shipment
    return None


def create_shipment(payload: dict) -> dict:
    """Crea un nuevo embarque y lo persiste. Devuelve el dict del embarque."""
    now = _now_iso()
    shipment = {
        "id": str(uuid.uuid4()),
        "imp": (payload.get("imp") or "").strip(),
        "proveedor": (payload.get("proveedor") or "").strip(),
        "estado_imp": (payload.get("estado_imp") or "").strip(),
        "tipo_compra": (payload.get("tipo_compra") or "").strip(),
        "fecha_llegada": (payload.get("fecha_llegada") or "").strip(),
        "fecha_inicial_proyectada": (payload.get("fecha_inicial_proyectada") or "").strip(),
        "novedades": (payload.get("novedades") or "").strip(),
        "productos": [
            _coerce_product(p) for p in (payload.get("productos") or [])
        ],
        "created_at": now,
        "updated_at": now,
    }

    records = load_records(FILE_KEY)
    new_id = next_id(FILE_KEY)
    records.append(make_record(new_id, shipment))
    save_records(FILE_KEY, records)
    return shipment


def update_shipment(shipment_id: str, payload: dict) -> dict | None:
    """
    Actualiza un embarque existente identificado por su uuid.
    Devuelve el dict actualizado, o None si no existe.
    """
    records = load_records(FILE_KEY)
    for idx, record in enumerate(records):
        shipment = record.get("data_json", {})
        if shipment.get("id") != shipment_id:
            continue

        shipment["imp"] = (
            payload.get("imp", shipment.get("imp", "")) or ""
        ).strip()
        shipment["proveedor"] = (
            payload.get("proveedor", shipment.get("proveedor", "")) or ""
        ).strip()
        shipment["estado_imp"] = (
            payload.get("estado_imp", shipment.get("estado_imp", "")) or ""
        ).strip()
        shipment["tipo_compra"] = (
            payload.get("tipo_compra", shipment.get("tipo_compra", "")) or ""
        ).strip()
        fecha_raw = payload.get(
            "fecha_llegada", shipment.get("fecha_llegada", "")
        ) or ""
        shipment["fecha_llegada"] = (
            fecha_raw.strip() if isinstance(fecha_raw, str) else str(fecha_raw)
        )
        fecha_inicial_raw = payload.get(
            "fecha_inicial_proyectada", shipment.get("fecha_inicial_proyectada", "")
        ) or ""
        shipment["fecha_inicial_proyectada"] = (
            fecha_inicial_raw.strip() if isinstance(fecha_inicial_raw, str) else str(fecha_inicial_raw)
        )
        shipment["novedades"] = (
            payload.get("novedades", shipment.get("novedades", "")) or ""
        ).strip()

        if "productos" in payload:
            shipment["productos"] = [
                _coerce_product(p) for p in (payload.get("productos") or [])
            ]

        now = _now_iso()
        shipment["updated_at"] = now
        records[idx]["data_json"] = shipment
        records[idx]["updated_at"] = now
        save_records(FILE_KEY, records)
        return shipment

    return None


def delete_shipment(shipment_id: str) -> bool:
    """Elimina el embarque con ese uuid. Devuelve True si se elimino."""
    records = load_records(FILE_KEY)
    new_records = [
        r for r in records if r.get("data_json", {}).get("id") != shipment_id
    ]
    if len(new_records) == len(records):
        return False
    save_records(FILE_KEY, new_records)
    return True


def bulk_delete(shipment_ids) -> int:
    """Elimina multiples embarques por uuid. Devuelve el numero eliminado."""
    if not shipment_ids:
        return 0
    ids_set = set(shipment_ids)
    records = load_records(FILE_KEY)
    new_records = [
        r for r in records if r.get("data_json", {}).get("id") not in ids_set
    ]
    deleted = len(records) - len(new_records)
    save_records(FILE_KEY, new_records)
    return deleted


# ---------------------------------------------------------------------------
# Importacion desde Excel
# ---------------------------------------------------------------------------

def _normalize_row(row: dict, column_map: dict):
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
        "fecha_inicial_proyectada": _to_date_str(get("FECHA_INICIAL_PROYECTADA")),
        "novedades": _safe_str(get("NOVEDADES")),
    }
    return shipment, product


def import_from_excel(file_storage) -> dict:
    stream = BytesIO(file_storage.read())
    df = pd.read_excel(stream)
    if df.empty:
        return {"created": 0, "updated": 0, "rows": 0}

    column_map = {str(col).strip().upper(): col for col in df.columns}
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
            shipment["updated_at"] = _now_iso()
            updated += 1
        else:
            now = _now_iso()
            new_shipment = {
                "id": str(uuid.uuid4()),
                "imp": shipment_data.get("imp"),
                "proveedor": shipment_data.get("proveedor"),
                "estado_imp": shipment_data.get("estado_imp"),
                "tipo_compra": shipment_data.get("tipo_compra"),
                "fecha_llegada": shipment_data.get("fecha_llegada"),
                "productos": [product],
                "created_at": now,
                "updated_at": now,
            }
            shipments.append(new_shipment)
            existing_by_imp[imp] = new_shipment
            created += 1

    save_shipments(shipments)
    return {"created": created, "updated": updated, "rows": int(len(df.index))}


# ---------------------------------------------------------------------------
# Exportacion a Excel
# ---------------------------------------------------------------------------

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
                "FECHA_INICIAL_PROYECTADA": shipment.get("fecha_inicial_proyectada", ""),
                "NOVEDADES": shipment.get("novedades", ""),
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
