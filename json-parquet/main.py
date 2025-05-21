import pyarrow.parquet as pq
import json
import os
from datetime import datetime, date
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

BUCKET_NAME = "bucket_grupo3"
CARPETA_BRONCE = "bronce/"

def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def subir_a_bucket(ruta_local, bucket_name, carpeta_destino):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        nombre_archivo = os.path.basename(ruta_local)
        blob = bucket.blob(os.path.join(carpeta_destino, nombre_archivo))
        blob.upload_from_filename(ruta_local)
        print(f"[OK] JSON subido a: gs://{bucket_name}/{blob.name}")
        os.remove(ruta_local)
    except DefaultCredentialsError:
        print("[ERROR] Credenciales no encontradas.")
    except Exception as e:
        print(f"[ERROR] Fallo al subir a bucket: {e}")

def convertir_parquet_a_json(ruta_archivo_local, nombre_base):
    tabla = pq.read_table(ruta_archivo_local)
    subset = tabla.slice(0, 100000)
    registros = subset.to_pydict()
    json_data = [dict(zip(registros, valores)) for valores in zip(*registros.values())]

    json_filename = f"{nombre_base}.json"
    ruta_json = f"/tmp/{json_filename}"

    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=convertir_a_serializable)

    subir_a_bucket(ruta_json, BUCKET_NAME, CARPETA_BRONCE)

def parquet_a_json_handler(event, context):
    """Punto de entrada para Cloud Function"""
    bucket_name = event['bucket']
    nombre_objeto = event['name']

    if not nombre_objeto.endswith(".parquet"):
        print(f"[INFO] No es un archivo Parquet: {nombre_objeto}")
        return

    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(nombre_objeto)

    ruta_local = f"/tmp/{os.path.basename(nombre_objeto)}"
    blob.download_to_filename(ruta_local)
    print(f"[INFO] Archivo Parquet descargado: {ruta_local}")

    nombre_base = os.path.splitext(os.path.basename(nombre_objeto))[0]
    convertir_parquet_a_json(ruta_local, nombre_base)
import pyarrow.parquet as pq
import json
import os
from datetime import datetime, date
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

BUCKET_NAME = "bucket_grupo3"
CARPETA_BRONCE = "bronce/"

def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def subir_a_bucket(ruta_local, bucket_name, carpeta_destino):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        nombre_archivo = os.path.basename(ruta_local)
        blob = bucket.blob(os.path.join(carpeta_destino, nombre_archivo))
        blob.upload_from_filename(ruta_local)
        print(f"[OK] JSON subido a: gs://{bucket_name}/{blob.name}")
        os.remove(ruta_local)
    except DefaultCredentialsError:
        print("[ERROR] Credenciales no encontradas.")
    except Exception as e:
        print(f"[ERROR] Fallo al subir a bucket: {e}")

def convertir_parquet_a_json(ruta_archivo_local, nombre_base):
    tabla = pq.read_table(ruta_archivo_local)
    subset = tabla.slice(0, 100000)
    registros = subset.to_pydict()
    json_data = [dict(zip(registros, valores)) for valores in zip(*registros.values())]

    json_filename = f"{nombre_base}.json"
    ruta_json = f"/tmp/{json_filename}"

    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=convertir_a_serializable)

    subir_a_bucket(ruta_json, BUCKET_NAME, CARPETA_BRONCE)

def parquet_a_json_handler(event, context):
    """Punto de entrada para Cloud Function"""
    bucket_name = event['bucket']
    nombre_objeto = event['name']

    if not nombre_objeto.endswith(".parquet"):
        print(f"[INFO] No es un archivo Parquet: {nombre_objeto}")
        return

    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(nombre_objeto)

    ruta_local = f"/tmp/{os.path.basename(nombre_objeto)}"
    blob.download_to_filename(ruta_local)
    print(f"[INFO] Archivo Parquet descargado: {ruta_local}")

    nombre_base = os.path.splitext(os.path.basename(nombre_objeto))[0]
    convertir_parquet_a_json(ruta_local, nombre_base)
