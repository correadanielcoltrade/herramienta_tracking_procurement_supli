from flask import Blueprint, g, jsonify, render_template, request, send_file

from functions.auth_service import require_auth
from functions.shipments_service import (
    bulk_delete,
    create_shipment,
    delete_shipment,
    export_template,
    export_to_excel,
    get_shipment,
    import_from_excel,
    list_shipments_summary,
    list_shipments,
    update_shipment,
)
from functions.users_service import create_user, delete_user, list_users, update_user

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


@admin_bp.route("/embarques", methods=["GET"])
@require_auth(roles=["ADMIN"], redirect_to_login=True)
def embarques_view():
    return render_template("admin_embarques.html", user=g.current_user)


@admin_bp.route("/usuarios", methods=["GET"])
@require_auth(roles=["ADMIN"], redirect_to_login=True)
def usuarios_view():
    return render_template("admin_users.html", user=g.current_user)


@admin_bp.route("/api/shipments", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_list_shipments():
    return jsonify({"data": list_shipments()})


@admin_bp.route("/api/shipments-summary", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_list_shipments_summary():
    args = request.args
    filters = {
        "imp": args.get("imp", "").strip(),
        "proveedor": args.get("proveedor", "").strip(),
        "estado": args.get("estado", "").strip(),
        "tipo_compra": args.get("tipo_compra", "").strip(),
        "fecha": args.get("fecha", "").strip(),
        "producto": args.get("producto", "").strip(),
        "marca": args.get("marca", "").strip(),
        "sku": args.get("sku", "").strip(),
    }
    return jsonify({"data": list_shipments_summary(filters)})


@admin_bp.route("/api/shipments", methods=["POST"])
@require_auth(roles=["ADMIN"])
def api_create_shipment():
    payload = request.get_json(silent=True) or {}
    shipment = create_shipment(payload)
    return jsonify({"data": shipment})


@admin_bp.route("/api/shipments/<shipment_id>", methods=["PUT"])
@require_auth(roles=["ADMIN"])
def api_update_shipment(shipment_id):
    payload = request.get_json(silent=True) or {}
    shipment = update_shipment(shipment_id, payload)
    if not shipment:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"data": shipment})


@admin_bp.route("/api/shipments/<shipment_id>", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_get_shipment(shipment_id):
    shipment = get_shipment(shipment_id)
    if not shipment:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"data": shipment})


@admin_bp.route("/api/shipments/<shipment_id>", methods=["DELETE"])
@require_auth(roles=["ADMIN"])
def api_delete_shipment(shipment_id):
    ok = delete_shipment(shipment_id)
    if not ok:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"status": "deleted"})


@admin_bp.route("/api/shipments/bulk-delete", methods=["POST"])
@require_auth(roles=["ADMIN"])
def api_bulk_delete():
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids", [])
    deleted = bulk_delete(ids)
    return jsonify({"deleted": deleted})


@admin_bp.route("/api/import-excel", methods=["POST"])
@require_auth(roles=["ADMIN"])
def api_import_excel():
    if "file" not in request.files:
        return jsonify({"error": "Archivo requerido"}), 400
    file_storage = request.files["file"]
    result = import_from_excel(file_storage)
    return jsonify({"data": result})


@admin_bp.route("/api/export-excel", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_export_excel():
    output = export_to_excel()
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="embarques.xlsx",
    )


@admin_bp.route("/api/export-json", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_export_json():
    data = list_shipments()
    return jsonify({"data": data})


@admin_bp.route("/api/template-excel", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_template_excel():
    output = export_template()
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="plantilla_embarques.xlsx",
    )


@admin_bp.route("/api/users", methods=["GET"])
@require_auth(roles=["ADMIN"])
def api_list_users():
    return jsonify({"data": list_users()})


@admin_bp.route("/api/users", methods=["POST"])
@require_auth(roles=["ADMIN"])
def api_create_user():
    payload = request.get_json(silent=True) or {}
    user, error = create_user(payload)
    if error:
        return jsonify({"error": error}), 400
    return jsonify({"data": {"username": user.get("username"), "name": user.get("name"), "role": user.get("role")}})


@admin_bp.route("/api/users/<username>", methods=["PUT"])
@require_auth(roles=["ADMIN"])
def api_update_user(username):
    payload = request.get_json(silent=True) or {}
    user, error = update_user(username, payload)
    if error:
        return jsonify({"error": error}), 400
    return jsonify({"data": {"username": user.get("username"), "name": user.get("name"), "role": user.get("role")}})


@admin_bp.route("/api/users/<username>", methods=["DELETE"])
@require_auth(roles=["ADMIN"])
def api_delete_user(username):
    ok, error = delete_user(username)
    if not ok:
        return jsonify({"error": error}), 400
    return jsonify({"status": "deleted"})
