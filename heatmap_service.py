import asyncpg
import asyncio

async def cargar_datos_heatmap():
    conn = await asyncpg.connect(user='postgres', password='1234', database='DbUrbanGuard', host='127.0.0.1')

    # Consulta para obtener los datos necesarios para el heatmap
    query = "SELECT latitud_denuncia, longitud_denuncia, sub_tipo FROM incidentes"
    registros = await conn.fetch(query)

    data = [{"latitud": r["latitud_denuncia"], "longitud": r["longitud_denuncia"], "sub_tipo": r["sub_tipo"]} for r in registros]

    await conn.close()
    return data
