import os
import re
import json
from datetime import datetime, date
from io import StringIO
import pyarrow.parquet as pq
from google.cloud import storage, bigquery

FILENAME_PATTERN = r"(green|yellow|fhv|fhvhv)_tripdata_2024-(0[1-9]|1[0-2])\.parquet$"
TABLAS_BIGQUERY = {
    "green_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-green",
    "yellow_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-yellow",
    "fhv_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhv",
    "fhvhv_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhvhv",
}

storage_client = storage.Client()
bq_client = bigquery.Client()

def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def _infer_bq_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, (datetime, date)):
        return "TIMESTAMP"
    else:
        return "STRING"

def tabla_existe_o_crear(tabla_id, esquema):
    try:
        bq_client.get_table(tabla_id)
        print(f"[INFO] La tabla {tabla_id} ya existe.")
    except Exception:
        print(f"[WARN] La tabla {tabla_id} no existe. Creando...")
        tabla = bigquery.Table(tabla_id, schema=esquema)
        tabla.time_partitioning = bigquery.TimePartitioning(field="load_pt")
        bq_client.create_table(tabla)
        print(f"[SUCCESS] Tabla {tabla_id} creada correctamente.")

def archivo_ya_cargado(tabla_id, archivo_nombre):
    query = f"""
        SELECT COUNT(1) as count
        FROM `{tabla_id}`
        WHERE source_file_name = @archivo_nombre
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("archivo_nombre", "STRING", archivo_nombre)]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        return row.count > 0
    return False

def procesar_parquet_a_bigquery(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    print(f"[DEBUG] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

    if not re.search(FILENAME_PATTERN, file_name):
        print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
        return

    prefix = file_name.split("-")[0] + "_tripdata_2024"
    tabla_id = TABLAS_BIGQUERY.get(prefix)

    if not tabla_id:
        print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {prefix}")
        return

    if archivo_ya_cargado(tabla_id, file_name):
        print(f"[INFO] El archivo {file_name} ya fue cargado anteriormente. Saltando...")
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    local_path = f"/tmp/{os.path.basename(file_name)}"
    blob.download_to_filename(local_path)
    print(f"[INFO] Archivo descargado a: {local_path}")

    tabla = pq.read_table(local_path)
    subset = tabla.slice(0, 100000)
    registros = subset.to_pydict()

    if not registros:
        print("[WARN] No se encontraron registros en el archivo parquet.")
        return

    fecha_actual = datetime.utcnow().date().isoformat()
    json_data = [
        {**dict(zip(registros.keys(), valores)), "load_pt": fecha_actual, "source_file_name": file_name}
        for valores in zip(*registros.values())
    ]

    print(f"[INFO] {len(json_data)} registros listos para cargar a BigQuery.")

    esquema = [
        bigquery.SchemaField(col, _infer_bq_type(val[0]))
        for col, val in registros.items() if not col.upper().startswith("_FILE_")
    ]
    esquema.append(bigquery.SchemaField("load_pt", "DATE"))
    esquema.append(bigquery.SchemaField("source_file_name", "STRING"))

    tabla_existe_o_crear(tabla_id, esquema)

    json_str = "\n".join([json.dumps(row, default=convertir_a_serializable) for row in json_data])
    json_file = StringIO(json_str)

    job_config = bigquery.LoadJobConfig(
        schema=esquema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(field="load_pt"),
    )

    load_job = bq_client.load_table_from_file(json_file, tabla_id, job_config=job_config)
    load_job.result()

    print(f"[SUCCESS] Archivo {file_name} cargado con éxito. {len(json_data)} filas insertadas.")
