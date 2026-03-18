import os
import threading
import webbrowser
from flask import Flask, redirect

from dotenv import load_dotenv

from Blueprint.auth import auth_bp
from Blueprint.admin import admin_bp
from Blueprint.user import user_bp
from functions.auth_service import try_get_user_from_request
from queries.db import init_db

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

load_dotenv()

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.config["SECRET_KEY"] = os.environ.get("APP_SECRET", "dev-secret-change-me")

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(user_bp)

init_db()


@app.route("/")
def index():
    return redirect("/dashboard")


def abrir_navegador():
    webbrowser.open_new("http://127.0.0.1:3000/")


if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        threading.Timer(1, abrir_navegador).start()
    app.run(host="0.0.0.0", port=3000, debug=True, threaded=True)
