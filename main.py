import os
import re
import json
from datetime import datetime, date
import pyarrow.parquet as pq
from google.cloud import storage, bigquery

TABLE_MAP = {
    "green_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data_trip_green",
    "yellow_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data_trip_yellow",
    "fhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data_trip_fhv",
    "fhvhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data_trip_fhvhv",
}

FILENAME_PATTERN = r"^(green_tripdata|fhv_tripdata|yellow_tripdata|fhvhv_tripdata)_2024-(0[1-9]|1[0-2])\.parquet$"

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
        tabla = bq_client.get_table(tabla_id)
        print(f"[INFO] Tabla existe: {tabla_id}")
        return tabla
    except Exception as e:
        print(f"[INFO] Tabla no existe, creando tabla: {tabla_id}")
        tabla = bigquery.Table(tabla_id, schema=esquema)
        tabla = bq_client.create_table(tabla)
        print(f"[INFO] Tabla creada: {tabla_id}")
        return tabla

def archivo_ya_cargado(tabla_id, archivo_nombre):
    query = f"""
        SELECT COUNT(1) as count
        FROM `{tabla_id}`
        WHERE _FILE_NAME = @archivo_nombre
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

    # Validar patrón archivo
    if not re.match(FILENAME_PATTERN, file_name):
        print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
        return

    prefijo = file_name.split("_2024")[0]
    tabla_id = TABLE_MAP.get(prefijo)

    # Si no hay tabla configurada, crear tabla con esquema base mínimo
    if tabla_id is None:
        print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {prefijo}")
        # Crear tabla con esquema mínimo para evitar error
        esquema_minimo = [
            bigquery.SchemaField("load_pt", "DATE"),
            bigquery.SchemaField("_FILE_NAME", "STRING"),
        ]
        # Usar un id inventado con prefijo para crear tabla temporal o fallback
        tabla_fallback_id = f"bigdata-458022.3_dev_tlc_us_nyc_bronze.data_fallback_{prefijo}"
        tabla_existe_o_crear(tabla_fallback_id, esquema_minimo)
        return

    # Descargar archivo
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    local_path = f"/tmp/{os.path.basename(file_name)}"
    blob.download_to_filename(local_path)
    print(f"[INFO] Archivo descargado a: {local_path}")

    # Leer parquet y tomar primeros 100k registros
    tabla = pq.read_table(local_path)
    subset = tabla.slice(0, 100000)
    registros = subset.to_pydict()

    # Si parquet vacío, crear tabla con esquema mínimo y salir
    if not registros:
        print("[WARN] No se encontraron registros en el archivo parquet.")
        esquema_minimo = [
            bigquery.SchemaField("load_pt", "DATE"),
            bigquery.SchemaField("_FILE_NAME", "STRING"),
        ]
        tabla_existe_o_crear(tabla_id, esquema_minimo)
        return

    # Crear esquema BQ desde los datos
    esquema = [
        bigquery.SchemaField(col, _infer_bq_type(val[0]))
        for col, val in registros.items() if len(val) > 0
    ]
    esquema.append(bigquery.SchemaField("load_pt", "DATE"))
    esquema.append(bigquery.SchemaField("_FILE_NAME", "STRING"))

    # Validar o crear tabla
    tabla_existe_o_crear(tabla_id, esquema)

    # Verificar si el archivo ya fue cargado
    if archivo_ya_cargado(tabla_id, file_name):
        print(f"[INFO] Archivo {file_name} ya fue cargado anteriormente en {tabla_id}. Se omite.")
        return

    # Preparar datos JSON para cargar
    fecha_actual = datetime.utcnow().date().isoformat()
    json_data = [
        {**dict(zip(registros.keys(), valores)), "load_pt": fecha_actual, "_FILE_NAME": file_name}
        for valores in zip(*registros.values())
    ]

    from io import StringIO
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
    print(f"[SUCCESS] Archivo {file_name} procesado y cargado en {tabla_id}. Filas: {len(json_data)}")
