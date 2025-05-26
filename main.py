import os
import re
import json
from datetime import datetime, date
from io import StringIO
import pyarrow.parquet as pq
from google.cloud import storage, bigquery

FILENAME_PATTERN = r"^(green_tripdata|fhv_tripdata|yellow_tripdata|fhvhv_tripdata)_2024-(0[1-9]|1[0-2])\.parquet$"
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
        
def columna_existe_en_tabla(tabla, nombre_columna):
    return any(field.name == nombre_columna for field in tabla.schema)

def archivo_ya_cargado(tabla_id, archivo_nombre):
    tabla = bq_client.get_table(tabla_id)

    if not columna_existe_en_tabla(tabla, "source_file_name"):
        print(f"[INFO] La columna 'source_file_name' no existe en la tabla {tabla_id}, se asume que el archivo no fue cargado.")
        return False

    fecha_actual = datetime.utcnow().date().isoformat()
    query = f"""
        SELECT COUNT(1) AS count
        FROM `{tabla_id}`
        WHERE source_file_name = @archivo_nombre
          AND load_pt = @fecha_actual
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("archivo_nombre", "STRING", archivo_nombre),
            bigquery.ScalarQueryParameter("fecha_actual", "DATE", fecha_actual),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        return row.count > 0
    return False


def agregar_columnas_faltantes(tabla_id, esquema_nuevo):
    """Agrega las columnas que no existan en la tabla BigQuery dada."""
    tabla = bq_client.get_table(tabla_id)
    esquema_existente = {field.name.lower(): field for field in tabla.schema}

    columnas_a_agregar = []
    for campo in esquema_nuevo:
        if campo.name.lower() not in esquema_existente:
            columnas_a_agregar.append(campo)

    if columnas_a_agregar:
        esquema_actualizado = tabla.schema[:] + columnas_a_agregar
        tabla.schema = esquema_actualizado
        tabla = bq_client.update_table(tabla, ["schema"])
        print(f"[INFO] Se agregaron {len(columnas_a_agregar)} columnas a la tabla {tabla_id}")
    else:
        print(f"[INFO] No hay columnas nuevas para agregar a la tabla {tabla_id}")
def procesar_parquet_a_bigquery(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    print(f"[DEBUG] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

    if not re.search(FILENAME_PATTERN, file_name):
        print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
        return

    # file_name = 'fhv_tripdata_2024-02.parquet'
    prefix = file_name.split("-")[0]

    if prefix not in TABLAS_BIGQUERY:
        print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {prefix}")
        return
    
    tabla_id = TABLAS_BIGQUERY[prefix]

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

    tabla_parquet = pq.read_table(local_path)
    subset = tabla_parquet.slice(0, 100000)
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

    esquema_nuevo = [
        bigquery.SchemaField(col, _infer_bq_type(val[0]))
        for col, val in registros.items()
        if not col.upper().startswith("_FILE_")
    ]
    esquema_nuevo.append(bigquery.SchemaField("load_pt", "DATE"))
    esquema_nuevo.append(bigquery.SchemaField("source_file_name", "STRING"))

    agregar_columnas_faltantes(tabla_id, esquema_nuevo)

    json_str = "\n".join([json.dumps(row, default=convertir_a_serializable) for row in json_data])
    json_file = StringIO(json_str)

    job_config = bigquery.LoadJobConfig(
        schema=esquema_nuevo,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(field="load_pt"),
    )

    load_job = bq_client.load_table_from_file(json_file, tabla_id, job_config=job_config)
    load_job.result()

    print(f"[SUCCESS] Archivo {file_name} cargado con éxito. {len(json_data)} filas insertadas.")
