import logging
import os
import threading
import webbrowser
from flask import Flask, redirect

from dotenv import load_dotenv

from Blueprint.auth import auth_bp
from Blueprint.admin import admin_bp
from Blueprint.user import user_bp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

load_dotenv()

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.config["SECRET_KEY"] = os.environ.get("APP_SECRET", "dev-secret-change-me")

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(user_bp)

logging.basicConfig(level=logging.INFO)

# La BD es opcional: si no hay DATABASE_URL / DB_HOST configurados, el
# sistema opera completamente con JSON (solo para desarrollo local).
from queries.storage import _db_enabled, _db_schema, init_storage

if _db_enabled():
    try:
        # Pre-calentar cache desde DB para que el primer request sea inmediato
        counts = init_storage()
        logging.info("DB conectada (schema=%s): %s", _db_schema() or "public", counts)
    except Exception:
        logging.exception("No se pudo conectar a la DB al iniciar")
else:
    logging.warning(
        "DB NO configurada: los datos se guardan en JSON local y en Render "
        "se perderan al reiniciar. Configura DB_HOST/DB_NAME/DB_USER/DB_PASSWORD."
    )


@app.route("/")
def index():
    return redirect("/dashboard")


def abrir_navegador():
    webbrowser.open_new("http://127.0.0.1:3000/")


if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        threading.Timer(1, abrir_navegador).start()
    app.run(host="0.0.0.0", port=3000, debug=True, threaded=True)
