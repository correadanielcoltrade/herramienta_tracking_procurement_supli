from flask import Blueprint, current_app, g, jsonify, make_response, redirect, render_template, request

from functions.auth_service import authenticate, create_token, require_auth, try_get_user_from_request

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.route("/login", methods=["GET"])
def login():
    user = try_get_user_from_request()
    if user:
        return redirect("/dashboard")
    return render_template("login.html")


@auth_bp.route("/login", methods=["POST"])
def login_post():
    if request.is_json:
        data = request.get_json(silent=True) or {}
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", "")).strip()
    else:
        username = str(request.form.get("username", "")).strip()
        password = str(request.form.get("password", "")).strip()

    user = authenticate(username, password)
    if not user:
        return jsonify({"error": "Credenciales invalidas"}), 401

    token = create_token(user, current_app.config["SECRET_KEY"])
    response = make_response(
        jsonify(
            {
                "token": token,
                "user": {
                    "username": user.get("username"),
                    "name": user.get("name"),
                    "role": user.get("role"),
                },
            }
        )
    )
    response.set_cookie("access_token", token, samesite="Lax", max_age=60 * 60 * 8)
    return response


@auth_bp.route("/logout", methods=["GET"])
def logout():
    response = make_response(redirect("/auth/login"))
    response.set_cookie("access_token", "", expires=0)
    return response


@auth_bp.route("/me", methods=["GET"])
@require_auth()
def me():
    return jsonify({"user": g.current_user})
