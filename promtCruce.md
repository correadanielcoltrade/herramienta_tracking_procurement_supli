DDAME LOS CODIGOS COMPLETOS, los dos de Python y los dos HTML, cruce.py y cruce_detalle.py y los dos correspondietes de HTML

en esta carpeta

C:\Users\Julian Herreño\OneDrive - Colombian Trade Company SAS\DATA\02. AREAS\DATA\Julian Estif Herreno Palacios\10-Portafolio-Mayoritas\data

tengo estos dos archivos:

C:\Users\Julian Herreño\OneDrive - Colombian Trade Company SAS\DATA\02. AREAS\DATA\Julian Estif Herreno Palacios\10-Portafolio-Mayoritas\data\cotizaciones.json

C:\Users\Julian Herreño\OneDrive - Colombian Trade Company SAS\DATA\02. AREAS\DATA\Julian Estif Herreno Palacios\10-Portafolio-Mayoritas\data\mercadolibre.json

cotizaciones.json tiene la siguiente estructura:

 {
        "Fecha_Cotización": "01/08/2025",
        "Proveedor": "Colombian Trade Marketing",
        "Producto": "Nintendo",
        "Precio": 148000
    }

y mercadolibre.json

{
        "title": "Disco Estado Sólido Ssd Kingston 2.5 A400 480gb Color Negro",
        "link": "https://www.mercadolibre.com.co/disco-estado-solido-ssd-kingston-25-a400-480gb-color-negro/p/MCO17978326#polycard_client=search_best-seller&tracking_id=377be399-aa6e-4c63-a509-cc2466d5570d&wid=MCO1646118613&sid=search",
        "img": "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7",
        "badge": "10º MÁS VENDIDO",
        "seller": null,
        "rating": 4.9,
        "reviews_total": 3255,
        "previous_price": null,
        "current_price": 174300,
        "discount": null,
        "installments": "3 cuotas de$58.100con 0% interés",
        "shipping": null,
        "source_url": "https://www.mercadolibre.com.co/mas-vendidos/MCO430598",
        "scraped_at": "26/08/2025"
    }

Entonces acá quiero hacer lo siguiente:

from flask import Blueprint, render_template

cruce_bp = Blueprint('cruce', __name__, url_prefix='/cruce')

@cruce_bp.route('/')

def cruce():
    return render_template('cruce.html')

{% extends "navbar.html" %}

{% block title %}Inicio - Mi Sitio{% endblock %}

{% block content %}

<link rel="stylesheet" href="{{ url_for('static', filename='css/cruce.css') }}">
<script src="{{ url_for('static', filename='js/cruce.js') }}"></script>
<h1>Cruce de Datos</h1>
{% endblock %}

Como ves en mercadolibre.json está el titulo del producto: "title": "Disco Estado Sólido Ssd Kingston 2.5 A400 480gb Color Negro", entonces lo que quiero es mostrar los campos de el archivo mercadolibre.json en una tabla y en un enlace haya un campo que diga ver oportunidades de cotización y haya un link entonces allí me abra un pagina donde este en la parte superior el nombre del producto de mercado libre, el precio y el enlace y debajo un listado de los productos que probablemente se relacionen con ese producto de mayor a menor por precio. Lo único es pues que usa alguna librería que relacione los productos lo mejor posible

entonces acá estaría el detalle

from flask import Blueprint, render_template

crucedetalle_bp = Blueprint('cruce_detalle_id', __name__, url_prefix='/cruce_detalle_id')

@crucedetalle_bp.route('/')

def cruce_detalle():
    return render_template('cruce_detalle.html')

{% extends "navbar.html" %}

{% block title %}Inicio - Mi Sitio{% endblock %}

{% block content %}

<link rel="stylesheet" href="{{ url_for('static', filename='css/cruce_detalle.css') }}">
<script src="{{ url_for('static', filename='js/cruce_detalle.js') }}"></script>
<h1>Cruce de Detalle</h1>
{% endblock %}
