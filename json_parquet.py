import pyarrow.parquet as pq
import json
import os
from datetime import datetime, date

def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def convertir_parquet_a_json(ruta_archivo, max_filas=100000):
    if not os.path.exists(ruta_archivo):
        print(f"Archivo no encontrado: {ruta_archivo}")
        return

    # Leer el archivo parquet
    tabla = pq.read_table(ruta_archivo)
    subset = tabla.slice(0, max_filas)

    # Convertir a diccionario
    registros = subset.to_pydict()
    json_data = [dict(zip(registros, valores)) for valores in zip(*registros.values())]

    # Serializar convirtiendo datetime a string
    nombre_salida = os.path.splitext(ruta_archivo)[0] + ".json"
    with open(nombre_salida, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=convertir_a_serializable)

    print(f"Archivo JSON guardado como: {nombre_salida}")

if __name__ == "__main__":
    convertir_parquet_a_json("data_2025-01-01.parquet")
