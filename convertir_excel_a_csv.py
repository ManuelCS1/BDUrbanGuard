import pandas as pd

# Ruta del archivo dentro de la carpeta del proyecto
file_path = 'DatasetProcesadoUltimo.xlsx'  # Asegúrate de que el archivo esté en la raíz del proyecto

# Leer el archivo Excel
df = pd.read_excel(file_path)

# Guardar como archivo CSV
df.to_csv('DatasetProcesadoUltimo.csv', index=False)

print("Archivo convertido exitosamente a CSV.")
