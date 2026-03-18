from flask import Blueprint, render_template


inicio_bp = Blueprint('inicio', __name__, url_prefix='/inicio')

@inicio_bp.route('/')

def inicio():
    return render_template('index.html')