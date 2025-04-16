import pandas as pd
import asyncpg
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder
from joblib import dump
from geopy.distance import geodesic
import numpy as np

# Función para cargar datos desde PostgreSQL usando asyncpg
async def cargar_datos():
    conn = await asyncpg.connect(user='postgres', password='1234', database='DbUrbanGuard', host='127.0.0.1')

    # Cargar los datos de la tabla incidentes, incluyendo 'id'
    df = await conn.fetch("SELECT * FROM incidentes")

    # Convertir los datos a DataFrame de pandas, incluyendo la columna 'id'
    df = pd.DataFrame([dict(row) for row in df], columns=[
        'id', 'FECHA_REGISTRO', 'UBIGEO', 'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO',
        'TIPO_DE_DENUNCIA', 'SITUACION_DENUNCIA', 'TIPO', 'SUB_TIPO', 'MODALIDAD',
        'FECHA_HECHO', 'HORA_HECHO', 'HORA', 'TURNO', 'UBICACION', 'DESCRIPCION',
        'FECHA_NACIMIENTO', 'EDAD_PERSONA', 'SEXO', 'ESTADO_CIVIL', 'GRADO_INSTRUCCION',
        'OCUPACION', 'PAIS_NATAL', 'MES', 'LONGITUD_DENUNCIA', 'LATITUD_DENUNCIA',
        'LATITUD_COMISARIA_CERCANA', 'LONGITUD_COMISARIA_CERCANA', 'NOMBRE_COMISARIA'])

    # Asegurarse de que las columnas de latitud y longitud son de tipo float
    df['LATITUD_DENUNCIA'] = df['LATITUD_DENUNCIA'].astype(float)
    df['LONGITUD_DENUNCIA'] = df['LONGITUD_DENUNCIA'].astype(float)
    df['LATITUD_COMISARIA_CERCANA'] = df['LATITUD_COMISARIA_CERCANA'].astype(float)
    df['LONGITUD_COMISARIA_CERCANA'] = df['LONGITUD_COMISARIA_CERCANA'].astype(float)

    # Cerrar la conexión
    await conn.close()
    return df

# Preprocesamiento y entrenamiento del modelo
async def entrenar_modelo():
    df = await cargar_datos()

    # Preprocesamiento
    label_encoder_subtipo = LabelEncoder()
    label_encoder_modalidad = LabelEncoder()
    label_encoder_mes = LabelEncoder()
    label_encoder_turno = LabelEncoder()
    label_encoder_distrito = LabelEncoder()

    df['SUB_TIPO'] = label_encoder_subtipo.fit_transform(df['SUB_TIPO'])
    df['MODALIDAD'] = label_encoder_modalidad.fit_transform(df['MODALIDAD'])
    df['MES'] = label_encoder_mes.fit_transform(df['MES'].astype(str))
    df['TURNO'] = label_encoder_turno.fit_transform(df['TURNO'])
    df['DISTRITO'] = label_encoder_distrito.fit_transform(df['DISTRITO'])

    df['FECHA_REGISTRO'] = pd.to_datetime(df['FECHA_REGISTRO'])
    df['DIA_SEMANA'] = df['FECHA_REGISTRO'].dt.dayofweek

    def calcular_distancia(lat1, lon1, lat2, lon2):
        if pd.isnull(lat1) or pd.isnull(lon1) or pd.isnull(lat2) or pd.isnull(lon2):
            return np.nan  # Retorna NaN si alguna coordenada es inválida
        return geodesic((lat1, lon1), (lat2, lon2)).meters

    # Calcular la distancia entre la denuncia y la comisaría más cercana
    df['DISTANCIA_COMISARIA'] = df.apply(lambda row: calcular_distancia(
        row['LATITUD_DENUNCIA'], row['LONGITUD_DENUNCIA'],
        row['LATITUD_COMISARIA_CERCANA'], row['LONGITUD_COMISARIA_CERCANA']), axis=1)

    # Seleccionar características (sin incluir 'id')
    X = df[['LATITUD_DENUNCIA', 'LONGITUD_DENUNCIA', 'MODALIDAD', 'MES', 'TURNO', 'DIA_SEMANA', 'DISTANCIA_COMISARIA', 'DISTRITO']]
    y = df['SUB_TIPO']

    # Dividir en conjunto de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo
    model = RandomForestClassifier(n_estimators=50, max_depth=10, class_weight='balanced', random_state=42)
    model.fit(X_train, y_train)

    # Guardar el modelo
    dump(model, 'random_forest_model.joblib')

    # Evaluar el modelo
    train_acc = accuracy_score(y_train, model.predict(X_train)) * 100
    test_acc = accuracy_score(y_test, model.predict(X_test)) * 100
    print(f"Accuracy en entrenamiento: {train_acc:.2f}%")
    print(f"Accuracy en prueba: {test_acc:.2f}%")
    print("\nReporte de clasificación en conjunto de prueba:")
    print(classification_report(y_test, model.predict(X_test)))

# Ejecutar la función de entrenamiento
import asyncio
asyncio.run(entrenar_modelo())
