import os
import re
import json
from datetime import datetime, date
import pyarrow.parquet as pq
from google.cloud import storage, bigquery

FILENAME_PATTERN = r"data_2024-(0[1-9]|1[0-2])\.parquet$"
TABLE_ID = "bigdata-458022.3_dev_tlc_us_nyc_bronze.viajes_test"

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

def procesar_parquet_a_bigquery(event, context):
    """Triggered by a change to a Cloud Storage bucket."""
    try:
        bucket_name = event['bucket']
        file_name = event['name']

        print(f"[DEBUG] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

        if not re.search(FILENAME_PATTERN, file_name):
            print(f"[INFO] Ignorando archivo no válido: {file_name}")
            return

        print(f"[INFO] Archivo válido para procesamiento: {file_name}")

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
            {**dict(zip(registros.keys(), valores)), "load_pt": fecha_actual}
            for valores in zip(*registros.values())
        ]

        print(f"[INFO] {len(json_data)} registros listos para cargar a BigQuery.")

        schema = [
            bigquery.SchemaField(col, _infer_bq_type(val[0]))
            for col, val in registros.items()
            if len(val) > 0
        ]
        schema.append(bigquery.SchemaField("load_pt", "DATE"))

        from io import StringIO
        json_str = "\n".join([json.dumps(row, default=convertir_a_serializable) for row in json_data])
        json_file = StringIO(json_str)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=bigquery.TimePartitioning(field="load_pt"),
        )

        job = bq_client.load_table_from_file(json_file, TABLE_ID, job_config=job_config)
        job.result()

        print(f"[SUCCESS] Archivo {file_name} procesado exitosamente. {len(json_data)} filas cargadas.")

    except Exception as e:
        print(f"[ERROR] Error al procesar archivo: {e}")
        raise
