from flask import Flask, request, jsonify
import joblib
import pandas as pd
from datetime import datetime
import asyncio
from heatmap_service import cargar_datos_heatmap

app = Flask(__name__)

# Cargar el modelo entrenado
model = joblib.load('random_forest_model.joblib')

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    latitud = data.get('latitud')
    longitud = data.get('longitud')
    hora = data.get('hora')

    # Lógica de predicción
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
