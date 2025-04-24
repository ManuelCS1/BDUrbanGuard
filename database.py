import asyncio
import asyncpg
import pandas as pd
import sys

DB_CONFIG = {
    "user": "bd_urbanguard_r2ew_user",
    "password": "ZjKee2fBcsygKTYdIsyPzOUJzpFgZcsO",
    "database": "bd_urbanguard_r2ew",
    "host": "dpg-cvvg3n24d50c739bteo0-a.oregon-postgres.render.com"
}

async def create_all_tables():
    conn = await asyncpg.connect(**DB_CONFIG)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(100) NOT NULL,
            correo VARCHAR(100) UNIQUE NOT NULL,
            celular VARCHAR(20) UNIQUE NOT NULL,
            contrasena VARCHAR(255) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rutas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id),
            origen_lat DOUBLE PRECISION,
            origen_lng DOUBLE PRECISION,
            destino_lat DOUBLE PRECISION,
            destino_lng DOUBLE PRECISION,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            calificacion INTEGER
        );
        CREATE TABLE IF NOT EXISTS contactos_emergencia (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id),
            nombre VARCHAR(100),
            telefono VARCHAR(20)
        );
        CREATE TABLE IF NOT EXISTS consejos_seguridad (
            id SERIAL PRIMARY KEY,
            texto TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS calificaciones_rutas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id),
            ruta_id INTEGER REFERENCES rutas(id),
            calificacion INTEGER,
            comentario TEXT
        );
        CREATE TABLE IF NOT EXISTS reportes_incidentes (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
            tipo_incidente VARCHAR(50) NOT NULL,
            latitud VARCHAR(20),
            longitud VARCHAR(20),
            descripcion TEXT,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
    ''')
    await conn.close()

async def create_table_and_load_data():
    conn = None
    try:
        print("Conectando a la base de datos...")
        conn = await asyncpg.connect(**DB_CONFIG)
        print("Conexión exitosa!")

        # Eliminar y crear tabla de incidentes
        print("Eliminando tabla si existe...")
        await conn.execute('DROP TABLE IF EXISTS incidentes')
        print("Creando tabla...")
        await conn.execute('''
            CREATE TABLE incidentes (
                id SERIAL PRIMARY KEY,
                fecha_registro INTEGER,
                ubigeo INTEGER,
                departamento VARCHAR(50),
                provincia VARCHAR(50),
                distrito VARCHAR(50),
                tipo_de_denuncia VARCHAR(100),
                situacion_denuncia VARCHAR(50),
                tipo VARCHAR(100),
                sub_tipo VARCHAR(100),
                modalidad VARCHAR(200),
                fecha_hecho INTEGER,
                hora_hecho INTEGER,
                hora VARCHAR(20),
                turno VARCHAR(20),
                ubicacion TEXT,
                descripcion TEXT,
                fecha_nacimiento INTEGER,
                edad_persona INTEGER,
                sexo VARCHAR(20),
                estado_civil VARCHAR(50),
                grado_instruccion VARCHAR(100),
                ocupacion VARCHAR(100),
                pais_natal VARCHAR(50),
                mes INTEGER,
                longitud_denuncia DOUBLE PRECISION,
                latitud_denuncia DOUBLE PRECISION,
                latitud_comisaria_cercana DOUBLE PRECISION,
                longitud_comisaria_cercana DOUBLE PRECISION,
                nombre_comisaria VARCHAR(100)
            )
        ''')

        print("Leyendo archivo CSV...")
        df = pd.read_csv('DatasetProcesadoUltimo.csv')
        print(f"Se encontraron {len(df)} registros para cargar")
        df.columns = df.columns.str.lower()
        values = [tuple(x) for x in df.to_numpy()]
        columns = df.columns.tolist()
        print("Insertando datos en la base de datos...")
        await conn.copy_records_to_table(
            'incidentes',
            records=values,
            columns=columns
        )
        print("¡Datos cargados exitosamente!")
        count = await conn.fetchval('SELECT COUNT(*) FROM incidentes')
        print(f"Total de registros en la tabla: {count}")

    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")
        sys.exit(1)
    finally:
        if conn:
            print("Cerrando conexión...")
            await conn.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    print("Creando tablas adicionales...")
    asyncio.run(create_all_tables())
    print("Cargando datos de incidentes...")
    asyncio.run(create_table_and_load_data())
    print("Proceso completado")