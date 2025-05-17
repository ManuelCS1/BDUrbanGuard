import asyncpg
import asyncio

async def cargar_datos_heatmap():
    conn = await asyncpg.connect(user='urbanguard_user', password='9MRt7buKSTjgrRN3rIpn7dSlaUtbtfwz', database='urbanguard', host='dpg-d0kh60nfte5s738mqpeg-a.oregon-postgres.render.com')

    # Consulta para obtener los datos necesarios para el heatmap
    query = "SELECT latitud_denuncia, longitud_denuncia, sub_tipo FROM incidentes"
    registros = await conn.fetch(query)

    data = [{"latitud": r["latitud_denuncia"], "longitud": r["longitud_denuncia"], "sub_tipo": r["sub_tipo"]} for r in registros]

    await conn.close()
    return data
