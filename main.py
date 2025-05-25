import os
import re
import json
from datetime import datetime, date
import pyarrow.parquet as pq
from google.cloud import storage, bigquery

# Patrón para filtrar archivos 2024-MM.parquet (puede ser más estricto si quieres)
FILENAME_PATTERN = r"^(green_tripdata|fhv_tripdata|yellow_tripdata|fhvhv_tripdata)_2024-(0[1-9]|1[0-2])\.parquet$"

# Mapear base nombre archivo a tabla BigQuery destino
TABLES_MAP = {
    "yellow_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-yellow",
    "green_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-green",
    "fhv_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhv",
    "fhvhv_tripdata_2024": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhvhv",
}

# Tabla de control para archivos procesados
CONTROL_TABLE_ID = "bigdata-458022.3_dev_tlc_us_nyc_bronze.parquet_files_loaded"

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

def tabla_existe(table_id):
    try:
        bq_client.get_table(table_id)
        return True
    except Exception:
        return False

def crear_tabla_si_no_existe(table_id, schema):
    if tabla_existe(table_id):
        print(f"[INFO] La tabla {table_id} ya existe.")
        return

    dataset_id = table_id.rsplit(".", 1)[0]
    table_name = table_id.rsplit(".", 1)[1]

    table_ref = bq_client.dataset(dataset_id).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = bq_client.create_table(table)
    print(f"[INFO] Tabla creada: {table_id}")

def archivo_ya_cargado(file_name):
    query = f"""
    SELECT COUNT(1) as count FROM `{CONTROL_TABLE_ID}` WHERE file_name = @file_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    result = query_job.result()
    for row in result:
        return row.count > 0
    return False

def registrar_archivo_cargado(file_name):
    rows_to_insert = [
        {"file_name": file_name, "loaded_at": datetime.utcnow()}
    ]
    errors = bq_client.insert_rows_json(CONTROL_TABLE_ID, rows_to_insert)
    if errors:
        print(f"[ERROR] Al insertar registro en tabla de control: {errors}")
    else:
        print(f"[INFO] Archivo registrado como cargado: {file_name}")

def procesar_parquet_a_bigquery(event, context):
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        print(f"[INFO] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

        # Validar patrón del nombre del archivo
        if not re.match(FILENAME_PATTERN, file_name):
            print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
            return

        # Extraer el prefijo para mapear tabla
        base_name = file_name.split("_")[:2]
        base_name_key = "_".join(base_name)
        if base_name_key not in TABLES_MAP:
            print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {base_name_key}")
            return

        table_id = TABLES_MAP[base_name_key]

        # Validar si archivo ya fue cargado
        if archivo_ya_cargado(file_name):
            print(f"[INFO] Archivo ya cargado previamente: {file_name}")
            return

        # Descargar archivo parquet
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        local_path = f"/tmp/{os.path.basename(file_name)}"
        blob.download_to_filename(local_path)
        print(f"[INFO] Archivo descargado: {local_path}")

        # Leer parquet y obtener registros (primeros 100k)
        tabla = pq.read_table(local_path)
        subset = tabla.slice(0, 100000)
        registros = subset.to_pydict()

        if not registros:
            print(f"[WARN] No hay registros en el archivo: {file_name}")
            return

        # Fecha actual para load_pt
        fecha_actual = datetime.utcnow().date().isoformat()

        # Construir JSON con columna load_pt
        json_data = [
            {**dict(zip(registros.keys(), valores)), "load_pt": fecha_actual}
            for valores in zip(*registros.values())
        ]

        print(f"[INFO] {len(json_data)} registros listos para cargar en BigQuery.")

        # Inferir esquema para BigQuery
        schema = [
            bigquery.SchemaField(col, _infer_bq_type(val[0]))
            for col, val in registros.items()
            if len(val) > 0
        ]
        schema.append(bigquery.SchemaField("load_pt", "DATE"))

        # Crear tabla si no existe
        crear_tabla_si_no_existe(table_id, schema)

        # Cargar datos a BigQuery (append)
        from io import StringIO
        json_str = "\n".join([json.dumps(row, default=convertir_a_serializable) for row in json_data])
        json_file = StringIO(json_str)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(field="load_pt"),
        )

        job = bq_client.load_table_from_file(json_file, table_id, job_config=job_config)
        job.result()  # Esperar que termine la carga
        print(f"[SUCCESS] Archivo {file_name} cargado exitosamente a {table_id}")

        # Registrar que el archivo fue cargado
        registrar_archivo_cargado(file_name)

    except Exception as e:
        print(f"[ERROR] Error al procesar archivo: {e}")
        raise
