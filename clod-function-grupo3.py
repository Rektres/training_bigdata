import os
import re
import requests
import pyarrow.parquet as pq
import json
from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
from google.cloud import storage

# --- Configuración del bucket ---
# BUCKET_NAME = "bucket_grupo3/2024"
# CARPETA_LOCAL = "2024"
BUCKET_NAME = "bucket_grupo3"
SUBCARPETA_BUCKET = "2024"

# --- Función para hacer JSON serializable ---
def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

# --- Buscar links de archivos parquet con patrón data_2024-MM.parquet ---
def buscar_links_parquet(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links_encontrados = []
        pattern = re.compile(r"data_2024-(0[1-9]|1[0-2])\.parquet$")

        for a in soup.find_all("a", href=True):
            href = a['href']
            if pattern.search(href):
                full_url = href if href.startswith("http") else urljoin(url, href)
                links_encontrados.append(full_url)

        return links_encontrados
    except Exception as e:
        print(f"[ERROR] Al obtener links: {e}")
        return []

# --- Descargar archivo ---
def descargar_archivo(url, carpeta_destino):
    nombre = os.path.basename(url)
    ruta = os.path.join(carpeta_destino, nombre)
    try:
        print(f"[INFO] Descargando {nombre}...")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(ruta, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[OK] Archivo descargado: {ruta}")
        return ruta
    except Exception as e:
        print(f"[ERROR] Descarga fallida: {e}")
        return None

# --- Convertir parquet a JSON ---
def convertir_parquet_a_json(ruta_archivo, max_filas=100000):
    if not os.path.exists(ruta_archivo):
        print(f"[ERROR] Archivo no encontrado: {ruta_archivo}")
        return None

    tabla = pq.read_table(ruta_archivo)
    subset = tabla.slice(0, max_filas)
    registros = subset.to_pydict()
    json_data = [dict(zip(registros, valores)) for valores in zip(*registros.values())]

    ruta_json = os.path.splitext(ruta_archivo)[0] + ".json"
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=convertir_a_serializable)

    print(f"[OK] Archivo JSON generado: {ruta_json}")
    return ruta_json

# --- Subir a bucket ---
def subir_a_bucket(ruta_local, bucket_name):
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    # blob = bucket.blob(os.path.basename(ruta_local))
    blob = bucket.blob(os.path.join(SUBCARPETA_BUCKET, os.path.basename(ruta_local)))
    blob.upload_from_filename(ruta_local)
    print(f"[OK] Archivo subido a bucket: gs://{bucket_name}/{blob.name}")

# --- Flujo principal ---
def ejecutar_flujo(url):
    os.makedirs(SUBCARPETA_BUCKET, exist_ok=True)
    links = buscar_links_parquet(url)

    for link in links:
        ruta_parquet = descargar_archivo(link, SUBCARPETA_BUCKET)
        if not ruta_parquet:
            break

        ruta_json = convertir_parquet_a_json(ruta_parquet)
        if not ruta_json:
            break

        subir_a_bucket(ruta_json, BUCKET_NAME)

# --- Ejecutar ---
if __name__ == "__main__":
    url_usuario = input("Ingresa la URL a inspeccionar: ").strip()
    ejecutar_flujo(url_usuario)