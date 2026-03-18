from datetime import datetime

from flask import Blueprint, g, jsonify, render_template, request, send_file

from functions.auth_service import require_auth
from functions.shipments_service import (
    compute_totals,
    export_to_excel,
    get_shipment,
    list_shipments,
    list_shipments_summary,
)

user_bp = Blueprint("user", __name__)


@user_bp.route("/dashboard", methods=["GET"])
@require_auth(roles=["ADMIN", "USER"], redirect_to_login=True)
def dashboard_view():
    return render_template("dashboard.html", user=g.current_user)


@user_bp.route("/api/shipments-summary", methods=["GET"])
@require_auth(roles=["ADMIN", "USER"])
def api_shipments_summary():
    args = request.args
    filters = {
        "imp": args.get("imp", "").strip(),
        "proveedor": args.get("proveedor", "").strip(),
        "estado": args.get("estado", "").strip(),
        "tipo_compra": args.get("tipo_compra", "").strip(),
        "producto": args.get("producto", "").strip(),
        "marca": args.get("marca", "").strip(),
        "sku": args.get("sku", "").strip(),
        "canal": args.get("canal", "").strip(),
        "fecha_desde": args.get("fecha_desde", "").strip(),
        "fecha_hasta": args.get("fecha_hasta", "").strip(),
    }
    return jsonify({"data": list_shipments_summary(filters)})


@user_bp.route("/api/shipments", methods=["GET"])
@require_auth(roles=["ADMIN", "USER"])
def api_shipments():
    return jsonify({"data": list_shipments()})


@user_bp.route("/api/shipments/<shipment_id>", methods=["GET"])
@require_auth(roles=["ADMIN", "USER"])
def api_shipment_detail(shipment_id):
    shipment = get_shipment(shipment_id)
    if not shipment:
        return jsonify({"error": "Not found"}), 404
    _, totals = compute_totals(shipment)
    return jsonify(
        {
            "data": {
                "shipment": shipment,
                "totals": totals,
            }
        }
    )


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


@user_bp.route("/api/export-excel-filtered", methods=["GET"])
@require_auth(roles=["ADMIN", "USER"])
def api_export_excel_filtered():
    args = request.args
    imp = args.get("imp", "").strip().lower()
    proveedor = args.get("proveedor", "").strip().lower()
    estado = args.get("estado", "").strip().lower()
    tipo_compra = args.get("tipo_compra", "").strip().lower()
    producto = args.get("producto", "").strip().lower()
    marca = args.get("marca", "").strip().lower()
    sku = args.get("sku", "").strip().lower()
    canal = args.get("canal", "").strip().lower()
    fecha_desde = args.get("fecha_desde", "").strip()
    fecha_hasta = args.get("fecha_hasta", "").strip()

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

    shipments = list_shipments()
    filtered = []
    product_filter_active = bool(producto or marca or sku or canal)

    for shipment in shipments:
        if not _matches_text(shipment.get("imp", "").lower(), imp):
            continue
        if not _matches_text(shipment.get("proveedor", "").lower(), proveedor):
            continue
        if not _matches_text(shipment.get("estado_imp", "").lower(), estado):
            continue
        if tipo_compra and shipment.get("tipo_compra", "").lower() != tipo_compra:
            continue
        if not _within_range(shipment.get("fecha_llegada", ""), from_date, to_date):
            continue

        products = shipment.get("productos", [])
        if product_filter_active:
            filtered_products = []
            for product in products:
                if producto and not _matches_text((product.get("producto") or "").lower(), producto):
                    continue
                if marca and not _matches_text((product.get("marca") or "").lower(), marca):
                    continue
                if sku and not _matches_text((product.get("sku") or "").lower(), sku):
                    continue
                if canal and float(product.get(canal) or 0) <= 0:
                    continue
                filtered_products.append(product)
            if not filtered_products:
                continue
            shipment_copy = dict(shipment)
            shipment_copy["productos"] = filtered_products
            filtered.append(shipment_copy)
        else:
            filtered.append(shipment)

    output = export_to_excel(filtered)
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="embarques_filtrados.xlsx",
    )
