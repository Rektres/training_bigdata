import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

# --- Configuración ---
BUCKET_NAME = "bucket_grupo3"
SUBCARPETA_BUCKET = "2024"
CARPETA_LOCAL = "2024"

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

        # Ordenar por mes ascendente
        def extraer_mes(link):
            match = re.search(r"data_2024-(\d{2})\.parquet", link)
            return int(match.group(1)) if match else 99  # los que no matchean van al final

        links_ordenados = sorted(links_encontrados, key=extraer_mes)
        return links_ordenados

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

# --- Subir a bucket y eliminar archivo local ---
def subir_y_eliminar(ruta_local, bucket_name, subcarpeta):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(os.path.join(subcarpeta, os.path.basename(ruta_local)))
        blob.upload_from_filename(ruta_local)
        print(f"[OK] Archivo subido a bucket: gs://{bucket_name}/{blob.name}")
        os.remove(ruta_local)
        print(f"[INFO] Archivo local eliminado: {ruta_local}")
    except DefaultCredentialsError:
        print("[ERROR] Credenciales no encontradas. Ejecutá 'gcloud auth application-default login'")
    except Exception as e:
        print(f"[ERROR] Fallo al subir o eliminar archivo: {e}")

# --- Flujo principal ---
def ejecutar_flujo(url):
    os.makedirs(CARPETA_LOCAL, exist_ok=True)
    links = buscar_links_parquet(url)

    for link in links:
        ruta_parquet = descargar_archivo(link, CARPETA_LOCAL)
        if ruta_parquet:
            subir_y_eliminar(ruta_parquet, BUCKET_NAME, SUBCARPETA_BUCKET)

# --- Ejecutar ---
if __name__ == "__main__":
    url_usuario = input("Ingresa la URL a inspeccionar: ").strip()
    ejecutar_flujo(url_usuario)
