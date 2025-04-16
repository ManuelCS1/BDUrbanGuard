import asyncpg
import asyncio

async def cargar_datos_heatmap():
    conn = await asyncpg.connect(user='bd_urbanguard_r2ew_user', password='ZjKee2fBcsygKTYdIsyPzOUJzpFgZcsO', database='bd_urbanguard_r2ew', host='dpg-cvvg3n24d50c739bteo0-a.oregon-postgres.render.com')

    # Consulta para obtener los datos necesarios para el heatmap
    query = "SELECT latitud_denuncia, longitud_denuncia, sub_tipo FROM incidentes"
    registros = await conn.fetch(query)

    data = [{"latitud": r["latitud_denuncia"], "longitud": r["longitud_denuncia"], "sub_tipo": r["sub_tipo"]} for r in registros]

    await conn.close()
    return data
