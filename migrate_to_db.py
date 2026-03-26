"""
migrate_to_db.py - Migra JSON local -> PostgreSQL con nuevo esquema JSONB.

Pasos que ejecuta:
1. Agrega al JSON local el registro de produccion que solo estaba en DB (Buhotec)
2. Elimina las tablas antiguas (esquema relacional)
3. Crea las nuevas tablas (id, created_at, updated_at, data_json JSONB)
4. Inserta todos los registros del JSON en las nuevas tablas

Ejecutar una sola vez: python migrate_to_db.py
"""

import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json, RealDictCursor
except ImportError:
    print("ERROR: psycopg2 no instalado. Ejecuta: pip install psycopg2-binary")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Rutas de archivos JSON
JSON_SHIPMENTS = os.path.join(DATA_DIR, "ts_shipments.json")
JSON_USERS     = os.path.join(DATA_DIR, "ts_users.json")

# Tablas nuevas (JSONB)
TABLE_SHIPMENTS = "trackingsupli_shipments"
TABLE_USERS     = "trackingsupli_users"

# Tablas antiguas a eliminar
OLD_TABLES = ["trackingsupli_shipment_products", "trackingsupli_shipments", "trackingsupli_users"]


def connect():
    url = os.getenv("DATABASE_URL")
    sslmode = os.getenv("DB_SSLMODE", "require")
    if url:
        conn = psycopg2.connect(url, sslmode=sslmode)
    else:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            sslmode=sslmode,
        )
    conn.autocommit = False
    return conn


def load_json(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def to_str(v):
    if v is None:
        return None
    return str(v)


# ─── PASO 1: Sincronizar JSON local con el registro de produccion ─────────────

def sync_production_record(conn):
    """
    Lee todos los registros de la DB antigua y los combina con el JSON local.
    Agrega cualquier embarque que este en DB pero no en el JSON.
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Leer embarques de DB antigua
    cur.execute("SELECT * FROM trackingsupli_shipments ORDER BY created_at")
    db_shipments = [dict(r) for r in cur.fetchall()]

    # Leer productos de DB antigua
    cur.execute("SELECT * FROM trackingsupli_shipment_products ORDER BY shipment_id, id")
    db_products = [dict(r) for r in cur.fetchall()]

    # Agrupar productos por shipment_id
    products_by_shipment = {}
    for p in db_products:
        sid = str(p["shipment_id"])
        if sid not in products_by_shipment:
            products_by_shipment[sid] = []
        products_by_shipment[sid].append({
            "producto":  p.get("producto", ""),
            "marca":     p.get("marca", ""),
            "upc":       p.get("upc", "") or "",
            "sku":       p.get("sku", "") or "",
            "q_total":   int(p.get("q_total") or 0),
            "costo_fob_usd":          float(p.get("costo_fob_usd") or 0),
            "costo_proyectado_ddp":   float(p.get("costo_proyectado_ddp") or 0),
            "retail":       int(p.get("retail") or 0),
            "resellers":    int(p.get("resellers") or 0),
            "corporativo":  int(p.get("corporativo") or 0),
            "ecommerce":    int(p.get("ecommerce") or 0),
            "telcom":       int(p.get("telcom") or 0),
            "libre":        int(p.get("libre") or 0),
            "confirmacion_cantidades_recibidas": p.get("confirmacion_cantidades_recibidas") or "",
            "observaciones": p.get("observaciones") or "",
        })

    # Leer JSON local actual
    local_records = load_json(JSON_SHIPMENTS)
    local_uuids = {r["data_json"]["id"] for r in local_records if r.get("data_json", {}).get("id")}

    added = 0
    next_auto_id = max((r.get("id", 0) for r in local_records), default=0) + 1

    for s in db_shipments:
        sid = str(s["id"])
        if sid in local_uuids:
            continue  # ya existe en JSON local

        fecha = s.get("fecha_llegada")
        fecha_str = fecha.isoformat() if hasattr(fecha, "isoformat") else str(fecha or "")
        created = s.get("created_at")
        created_str = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
        updated = s.get("updated_at")
        updated_str = updated.isoformat() if hasattr(updated, "isoformat") else str(updated or "")

        shipment_data = {
            "id":           sid,
            "imp":          s.get("imp") or "",
            "proveedor":    s.get("proveedor") or "",
            "estado_imp":   s.get("estado_imp") or "",
            "tipo_compra":  s.get("tipo_compra") or "",
            "fecha_llegada": fecha_str,
            "productos":    products_by_shipment.get(sid, []),
            "created_at":   created_str,
            "updated_at":   updated_str,
        }

        local_records.append({
            "id":         next_auto_id,
            "created_at": created_str,
            "updated_at": updated_str,
            "data_json":  shipment_data,
        })
        next_auto_id += 1
        added += 1
        print(f"    + Agregado al JSON: {s.get('imp')} | {s.get('proveedor')}")

    if added:
        save_json(JSON_SHIPMENTS, local_records)
        print(f"  JSON actualizado con {added} registro(s) de produccion.")
    else:
        print("  JSON ya estaba al dia.")

    return local_records


# ─── PASO 2 y 3: Reemplazar tablas antiguas con esquema JSONB ────────────────

def recreate_tables(conn):
    with conn.cursor() as cur:
        # Eliminar tablas en orden (products primero por FK)
        for t in OLD_TABLES:
            cur.execute(f"SELECT to_regclass(%s)", (t,))
            exists = cur.fetchone()[0] is not None
            if exists:
                cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
                print(f"  Tabla eliminada: {t}")

        # Crear nuevas tablas con esquema JSONB
        for table in [TABLE_SHIPMENTS, TABLE_USERS]:
            cur.execute(f"""
                CREATE TABLE {table} (
                    id         INTEGER PRIMARY KEY,
                    created_at TEXT,
                    updated_at TEXT,
                    data_json  JSONB
                )
            """)
            print(f"  Tabla creada: {table}")

    conn.commit()


# ─── PASO 4: Insertar registros JSON en las nuevas tablas ───────────────────

def insert_records(conn, table_name, records):
    if not records:
        return 0
    rows = [
        (
            r.get("id"),
            to_str(r.get("created_at")),
            to_str(r.get("updated_at")),
            Json(r.get("data_json") or {}),
        )
        for r in records
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {table_name} (id, created_at, updated_at, data_json) VALUES %s",
            rows,
        )
    conn.commit()
    return len(rows)


# ─── Main ────────────────────────────────────────────────────────────────────

def migrate():
    print("=" * 55)
    print("  MIGRACION JSON -> PostgreSQL (nuevo esquema JSONB)")
    print("=" * 55)

    print("\nConectando a la base de datos...")
    conn = connect()
    conn.autocommit = False
    print("  Conexion exitosa.")

    try:
        # Paso 1: Traer registro de produccion al JSON local
        print("\n[1/4] Sincronizando JSON local con registros de produccion...")
        shipment_records = sync_production_record(conn)

        # Paso 2 y 3: Reemplazar tablas
        print("\n[2/4] Eliminando tablas antiguas...")
        print("[3/4] Creando nuevas tablas con esquema JSONB...")
        recreate_tables(conn)

        # Paso 4: Insertar embarques
        print("\n[4/4] Insertando registros en nuevas tablas...")
        n_ships = insert_records(conn, TABLE_SHIPMENTS, shipment_records)
        print(f"  {TABLE_SHIPMENTS}: {n_ships} embarques insertados")

        user_records = load_json(JSON_USERS)
        n_users = insert_records(conn, TABLE_USERS, user_records)
        print(f"  {TABLE_USERS}: {n_users} usuarios insertados")

        print("\n" + "=" * 55)
        print("  MIGRACION COMPLETADA EXITOSAMENTE")
        print("=" * 55)

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
