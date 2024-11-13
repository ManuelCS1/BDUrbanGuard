import pandas as pd
import asyncpg
import asyncio
from datetime import datetime

# Función para convertir la hora en formato 'HH:MM' a un número entero
def convertir_hora(hora_str):
    if isinstance(hora_str, str):
        try:
            horas, minutos = map(int, hora_str.split(':'))
            return horas * 100 + minutos
        except ValueError:
            return None
    elif isinstance(hora_str, int):
        return hora_str
    else:
        return None

# Función para convertir la fecha en formato 'YYYYMMDD' a un objeto datetime.date
def convertir_fecha(fecha_int):
    try:
        return datetime.strptime(str(fecha_int), '%Y%m%d').date()
    except ValueError:
        return None

# Leer el archivo CSV con pandas
def leer_csv(file_path):
    df = pd.read_csv(file_path)

    # Convertir las horas de 'HORA_HECHO' y 'HORA' en enteros
    df['HORA_HECHO'] = df['HORA_HECHO'].apply(convertir_hora)
    df['HORA'] = df['HORA'].apply(convertir_hora)

    # Convertir la columna FECHA_NACIMIENTO a fecha
    df['FECHA_NACIMIENTO'] = df['FECHA_NACIMIENTO'].apply(convertir_fecha)

    print("Columnas en el CSV:", df.columns)  # Imprime los nombres de las columnas
    return df

# Insertar los datos en la tabla 'incidentes' usando asyncpg
async def insertar_datos(df):
    conn = await asyncpg.connect(user='postgres', password='1234',
                                 database='DbUrbanGuard', host='127.0.0.1')

    # Iterar sobre cada fila del DataFrame y hacer la inserción
    for _, row in df.iterrows():
        await conn.execute('''
            INSERT INTO incidentes (
                FECHA_REGISTRO, UBIGEO, DEPARTAMENTO, PROVINCIA, DISTRITO, TIPO_DE_DENUNCIA,
                SITUACION_DENUNCIA, TIPO, SUB_TIPO, MODALIDAD, FECHA_HECHO, HORA_HECHO,
                HORA, TURNO, UBICACION, DESCRIPCION, FECHA_NACIMIENTO, EDAD_PERSONA, SEXO,
                ESTADO_CIVIL, GRADO_INSTRUCCION, OCUPACION, PAIS_NATAL, MES, LONGITUD_DENUNCIA,
                LATITUD_DENUNCIA, LATITUD_COMISARIA_CERCANA, LONGITUD_COMISARIA_CERCANA, NOMBRE_COMISARIA
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                      $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
        ''',
                           row['FECHA_REGISTRO'], row['UBIGEO'], row['DEPARTAMENTO'], row['PROVINCIA'], row['DISTRITO'],
                           row['TIPO_DE_DENUNCIA'], row['SITUACION_DENUNCIA'], row['TIPO'], row['SUB_TIPO'],
                           row['MODALIDAD'], row['FECHA_HECHO'], row['HORA_HECHO'], row['HORA'], row['TURNO'],
                           row['UBICACION'], row['DESCRIPCION'], row['FECHA_NACIMIENTO'], row['EDAD_PERSONA'],
                           row['SEXO'], row['ESTADO_CIVIL'], row['GRADO_INSTRUCCION'], row['OCUPACION'],
                           row['PAIS_NATAL'], row['MES'], row['LONGITUD_DENUNCIA'], row['LATITUD_DENUNCIA'],
                           row['LATITUD_COMISARIA_CERCANA'], row['LONGITUD_COMISARIA_CERCANA'], row['NOMBRE_COMISARIA']
                           )

    print("Datos insertados con éxito.")

    # Cerrar la conexión
    await conn.close()

# Función principal para coordinar la lectura e inserción de datos
async def main():
    file_path = 'DatasetProcesadoUltimo.csv'  # Coloca aquí la ruta de tu archivo CSV
    df = leer_csv(file_path)
    await insertar_datos(df)

# Ejecutar la función principal
asyncio.run(main())
