from flask import Flask, request, jsonify
import joblib
import pandas as pd
from datetime import datetime
import asyncio
import asyncpg

from heatmap_service import cargar_datos_heatmap

app = Flask(__name__)

# Cargar el modelo entrenado
model = joblib.load('random_forest_model.joblib')

# --- CONFIG DB ---
DB_CONFIG = {
    "user": "urbanguard_user",
    "password": "9MRt7buKSTjgrRN3rIpn7dSlaUtbtfwz",
    "database": "urbanguard",
    "host": "dpg-d0kh60nfte5s738mqpeg-a.oregon-postgres.render.com"
}

async def get_conn():
    return await asyncpg.connect(**DB_CONFIG)

# --- ENDPOINTS ML Y HEATMAP ---
@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    latitud = data.get('latitud')
    longitud = data.get('longitud')
    hora = data.get('hora')

    df_nuevo = pd.DataFrame({
        'latitud_denuncia': [latitud],
        'longitud_denuncia': [longitud],
        'mes': [datetime.now().month],
        'hora_hecho': [hora],
        'dia_semana': [datetime.now().weekday()],
        'distancia_comisaria': [0],
        'distrito': [1]
    })

    prediccion = model.predict_proba(df_nuevo)
    resultado = {
        'homicidio': prediccion[0][0] * 100,
        'hurto': prediccion[0][1] * 100,
        'robo': prediccion[0][2] * 100,
        'violacion': prediccion[0][3] * 100
    }

    return jsonify(resultado)

@app.route('/heatmap_data', methods=['GET'])
def obtener_datos_heatmap():
    data = asyncio.run(cargar_datos_heatmap())
    return jsonify(data)

# --- ENDPOINTS USUARIOS ---
@app.route('/usuarios/registro', methods=['POST'])
def registrar_usuario():
    data = request.get_json()
    # Validación básica
    for campo in ['nombre', 'correo', 'contrasena', 'celular']:
        if not data.get(campo):
            return {"error": f"El campo {campo} es obligatorio"}, 400

    async def _register():
        conn = await get_conn()
        try:
            await conn.execute(
                "INSERT INTO usuarios (nombre, correo, contrasena, celular) VALUES ($1, $2, $3, $4)",
                data['nombre'], data['correo'], data['contrasena'], data['celular']
            )
            return {"mensaje": "Usuario registrado exitosamente"}
        except asyncpg.exceptions.UniqueViolationError as e:
            error_msg = str(e)
            if 'correo' in error_msg:
                return {"error": "Correo ya registrado"}, 400
            elif 'celular' in error_msg:
                return {"error": "Celular ya registrado"}, 400
            else:
                return {"error": "Correo o celular ya registrado"}, 400
        except Exception as e:
            print("Error en registro:", e)
            return {"error": f"Error interno del servidor: {str(e)}"}, 500
        finally:
            await conn.close()
    return asyncio.run(_register())

@app.route('/usuarios/login', methods=['POST'])
def login_usuario():
    data = request.get_json()
    async def _login():
        conn = await get_conn()
        usuario = await conn.fetchrow(
            "SELECT id, nombre, correo, celular FROM usuarios WHERE correo=$1 AND contrasena=$2",
            data['correo'], data['contrasena']
        )
        await conn.close()
        if usuario:
            return dict(usuario)
        else:
            return {"error": "Credenciales incorrectas"}, 401
    return asyncio.run(_login())

@app.route('/usuarios/recuperar', methods=['POST'])
def recuperar_contrasena():
    data = request.get_json()
    async def _recuperar():
        conn = await get_conn()
        usuario = await conn.fetchrow(
            "SELECT id FROM usuarios WHERE correo=$1 AND celular=$2",
            data['correo'], data['celular']
        )
        if not usuario:
            await conn.close()
            return {"error": "Datos incorrectos"}, 404

        # Si viene nueva contraseña, actualiza
        if 'nueva_contrasena' in data:
            await conn.execute(
                "UPDATE usuarios SET contrasena=$1 WHERE id=$2",
                data['nueva_contrasena'], usuario['id']
            )
            await conn.close()
            return {"mensaje": "Contraseña actualizada correctamente"}
        else:
            await conn.close()
            return {"mensaje": "Datos verificados, puede cambiar la contraseña"}

    return asyncio.run(_recuperar())

@app.route('/usuarios/<int:usuario_id>', methods=['GET'])
def obtener_usuario(usuario_id):
    async def _get():
        conn = await get_conn()
        usuario = await conn.fetchrow(
            "SELECT id, nombre, correo, celular FROM usuarios WHERE id=$1", usuario_id
        )
        await conn.close()
        if usuario:
            return dict(usuario)
        else:
            return {"error": "Usuario no encontrado"}, 404
    return asyncio.run(_get())

# --- ENDPOINTS INCIDENTES (solo consulta, el registro masivo ya lo haces con tu script) ---
@app.route('/incidentes', methods=['GET'])
def listar_incidentes():
    async def _get():
        conn = await get_conn()
        incidentes = await conn.fetch("SELECT * FROM incidentes")
        await conn.close()
        return [dict(i) for i in incidentes]
    return jsonify(asyncio.run(_get()))

# --- ENDPOINTS RUTAS ---
@app.route('/rutas', methods=['POST'])
def guardar_ruta():
    data = request.get_json()
    async def _save():
        conn = await get_conn()
        await conn.execute(
            "INSERT INTO rutas (usuario_id, origen_lat, origen_lng, destino_lat, destino_lng, calificacion) VALUES ($1, $2, $3, $4, $5, $6)",
            data['usuario_id'], data['origen_lat'], data['origen_lng'], data['destino_lat'], data['destino_lng'], data.get('calificacion')
        )
        await conn.close()
        return {"mensaje": "Ruta guardada"}
    return asyncio.run(_save())

@app.route('/rutas/<int:usuario_id>', methods=['GET'])
def obtener_rutas_usuario(usuario_id):
    async def _get():
        conn = await get_conn()
        rutas = await conn.fetch("SELECT * FROM rutas WHERE usuario_id=$1", usuario_id)
        await conn.close()
        return [dict(r) for r in rutas]
    return jsonify(asyncio.run(_get()))

# --- ENDPOINTS CONTACTOS DE EMERGENCIA ---

@app.route('/contactos', methods=['POST'])
def agregar_contacto():
    data = request.get_json()
    async def _add():
        conn = await get_conn()
        await conn.execute(
            "INSERT INTO contactos_emergencia (usuario_id, nombre, telefono) VALUES ($1, $2, $3)",
            data['usuario_id'], data['nombre'], data['telefono']
        )
        await conn.close()
        return {"mensaje": "Contacto agregado"}
    return asyncio.run(_add())

@app.route('/contactos/<int:usuario_id>', methods=['GET'])
def obtener_contactos(usuario_id):
    async def _get():
        conn = await get_conn()
        contactos = await conn.fetch("SELECT * FROM contactos_emergencia WHERE usuario_id=$1", usuario_id)
        await conn.close()
        return [dict(c) for c in contactos]
    return jsonify(asyncio.run(_get()))

@app.route('/contactos/<int:contacto_id>', methods=['PUT'])
def modificar_contacto(contacto_id):
    data = request.get_json()
    async def _update():
        conn = await get_conn()
        await conn.execute(
            "UPDATE contactos_emergencia SET nombre=$1, telefono=$2 WHERE id=$3",
            data['nombre'], data['telefono'], contacto_id
        )
        await conn.close()
        return {"mensaje": "Contacto modificado"}
    return asyncio.run(_update())

@app.route('/contactos/<int:contacto_id>', methods=['DELETE'])
def eliminar_contacto(contacto_id):
    async def _delete():
        conn = await get_conn()
        await conn.execute(
            "DELETE FROM contactos_emergencia WHERE id=$1", contacto_id
        )
        await conn.close()
        return {"mensaje": "Contacto eliminado"}
    return asyncio.run(_delete())

# --- ENDPOINTS CONSEJOS DE SEGURIDAD ---
@app.route('/consejos', methods=['POST'])
def agregar_consejo():
    data = request.get_json()
    async def _add():
        conn = await get_conn()
        await conn.execute(
            "INSERT INTO consejos_seguridad (texto) VALUES ($1)",
            data['texto']
        )
        await conn.close()
        return {"mensaje": "Consejo agregado"}
    return asyncio.run(_add())

@app.route('/consejos', methods=['GET'])
def obtener_consejos():
    async def _get():
        conn = await get_conn()
        consejos = await conn.fetch("SELECT * FROM consejos_seguridad")
        await conn.close()
        return [dict(c) for c in consejos]
    return jsonify(asyncio.run(_get()))

# --- ENDPOINTS CALIFICACIONES DE RUTAS ---
@app.route('/calificaciones', methods=['POST'])
def calificar_ruta():
    data = request.get_json()
    async def _add():
        conn = await get_conn()
        await conn.execute(
            "INSERT INTO calificaciones_rutas (usuario_id, ruta_id, calificacion, comentario) VALUES ($1, $2, $3, $4)",
            data['usuario_id'], data['ruta_id'], data['calificacion'], data.get('comentario')
        )
        await conn.close()
        return {"mensaje": "Calificación registrada"}
    return asyncio.run(_add())

@app.route('/calificaciones/<int:ruta_id>', methods=['GET'])
def obtener_calificaciones(ruta_id):
    async def _get():
        conn = await get_conn()
        calificaciones = await conn.fetch("SELECT * FROM calificaciones_rutas WHERE ruta_id=$1", ruta_id)
        await conn.close()
        return [dict(c) for c in calificaciones]
    return jsonify(asyncio.run(_get()))


# ---------- ENDPOINTS REPORTES DE INCIDENTES ----------
TIPOS_VALIDOS = {'homicidio', 'robo', 'hurto', 'violacion'}

@app.route('/reportes', methods=['POST'])
def crear_reporte():
    data = request.get_json()
    for campo in ['usuario_id', 'tipo_incidente', 'latitud', 'longitud', 'descripcion']:
        if not data.get(campo):
            return {"error": f"El campo {campo} es obligatorio"}, 400
    if data['tipo_incidente'].lower() not in TIPOS_VALIDOS:
        return {"error": "Tipo de incidente no permitido"}, 400

    async def _save():
        conn = await get_conn()
        await conn.execute(
            """INSERT INTO reportes_incidentes
               (usuario_id, tipo_incidente, latitud, longitud, descripcion)
               VALUES ($1,$2,$3,$4,$5)""",
            data['usuario_id'], data['tipo_incidente'], data['latitud'],
            data['longitud'], data['descripcion']
        )
        await conn.close()
        return {"mensaje": "Incidente reportado correctamente"}
    return asyncio.run(_save())

@app.route('/reportes/<int:usuario_id>', methods=['GET'])
def listar_reportes(usuario_id):
    async def _get():
        conn = await get_conn()
        rep = await conn.fetch(
            "SELECT * FROM reportes_incidentes WHERE usuario_id=$1 ORDER BY fecha DESC",
            usuario_id
        )
        await conn.close()
        return [dict(r) for r in rep]
    return jsonify(asyncio.run(_get()))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)