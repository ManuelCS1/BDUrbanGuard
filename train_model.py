import pandas as pd
import asyncpg
import asyncio
import numpy as np
from geopy.distance import geodesic
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from joblib import dump
from datetime import datetime

# Función para cargar los datos desde PostgreSQL
async def cargar_datos():
    conn = await asyncpg.connect(user='urbanguard_user', password='9MRt7buKSTjgrRN3rIpn7dSlaUtbtfwz', database='urbanguard', host='dpg-d0kh60nfte5s738mqpeg-a.oregon-postgres.render.com')

    # Cargar los datos de la tabla incidentes
    df = await conn.fetch("SELECT * FROM incidentes")

    # Convertir los datos a DataFrame de pandas
    df = pd.DataFrame([dict(row) for row in df])

    # Cerrar la conexión
    await conn.close()
    return df

# Función para calcular la distancia entre el lugar del delito y la comisaría más cercana
def calcular_distancia(lat1, lon1, lat2, lon2):
    if pd.isnull(lat1) or pd.isnull(lon1) or pd.isnull(lat2) or pd.isnull(lon2):
        return np.nan
    return geodesic((lat1, lon1), (lat2, lon2)).meters

# Preprocesamiento de datos y entrenamiento del modelo
async def entrenar_modelo():
    df = await cargar_datos()

    # Preprocesamiento: codificación de variables categóricas
    label_encoder_subtipo = LabelEncoder()
    label_encoder_mes = LabelEncoder()
    label_encoder_distrito = LabelEncoder()

    df['sub_tipo'] = label_encoder_subtipo.fit_transform(df['sub_tipo'])
    df['mes'] = label_encoder_mes.fit_transform(df['mes'].astype(str))
    df['distrito'] = label_encoder_distrito.fit_transform(df['distrito'])

    # Convertir fecha y extraer el día de la semana
    df['fecha_registro'] = pd.to_datetime(df['fecha_registro'])
    df['dia_semana'] = df['fecha_registro'].dt.dayofweek

    # Aquí eliminamos la manipulación de 'hora_hecho', ya que es numérica
    # df['hora_hecho'] ya es un entero, por lo que lo usamos directamente

    # Calcular la distancia a la comisaría más cercana
    df['distancia_comisaria'] = df.apply(lambda row: calcular_distancia(
        row['latitud_denuncia'], row['longitud_denuncia'],
        row['latitud_comisaria_cercana'], row['longitud_comisaria_cercana']
    ), axis=1)

    # Características de entrada (usamos 'hora_hecho' directamente)
    X = df[['latitud_denuncia', 'longitud_denuncia', 'mes',
            'hora_hecho', 'dia_semana', 'distancia_comisaria', 'distrito']]
    y = df['sub_tipo']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Modelo Random Forest con pesos balanceados
    model = RandomForestClassifier(n_estimators=50, max_depth=10, class_weight='balanced', random_state=42)
    model.fit(X_train, y_train)



    # Evaluación del modelo: Accuracy
    y_pred_train = model.predict(X_train)

    train_acc = accuracy_score(y_train, y_pred_train) * 100

    print(f"Accuracy: {train_acc:.2f}%")

    # Mostrar la importancia de las características (variables)
    feature_importances = model.feature_importances_
    features = ['latitud_denuncia', 'longitud_denuncia', 'mes', 'hora_hecho',
                'dia_semana', 'distancia_comisaria', 'distrito']

    # Guardar el modelo entrenado
    dump(model, 'random_forest_model.joblib')

    return model, label_encoder_subtipo

# Predicción con probabilidades para los 4 subtipos, basándose en latitud, longitud y hora
def predecir(modelo, latitud, longitud, hora, label_encoder_subtipo):
    # Definir mes y convertir la hora a formato numérico
    mes_actual = datetime.now().month
    hora_hecho = int(hora)

    # Simulación de un nuevo dato de entrada con valores calculados
    df_nuevo = pd.DataFrame({
        'latitud_denuncia': [latitud],
        'longitud_denuncia': [longitud],
        'mes': [mes_actual],
        'hora_hecho': [hora_hecho],
        'dia_semana': [datetime.now().weekday()],  # Día actual
        'distancia_comisaria': [0],  # Asumimos 0 o calculamos la distancia si es necesario
        'distrito': [1]  # Valor predeterminado de distrito
    })

    # Predecir las probabilidades de cada subtipo de delito
    probabilidades = modelo.predict_proba(df_nuevo)

    # Mostrar la probabilidad para cada subtipo
    for i, subtipo in enumerate(label_encoder_subtipo.classes_):
        print(f"Probabilidad de {subtipo}: {probabilidades[0][i] * 100:.2f}%")

# Entrenamiento y predicción
model, label_encoder_subtipo = asyncio.run(entrenar_modelo())

# Solicitud de 3 inputs al usuario
latitud = float(input("Ingresa la latitud: "))
longitud = float(input("Ingresa la longitud: "))
hora = int(input("Ingresa la hora (en formato militar, ej. 1300 para 13:00): "))

# Realizar predicción con los inputs del usuario
predecir(model, latitud, longitud, hora, label_encoder_subtipo)
