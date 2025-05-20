import csv
import os
import re
import json
from datetime import datetime
from google.cloud import storage, bigquery
from flask import Request

# Variables de entorno
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
PROJECT_ID = os.environ.get("PROJECT_ID")
FILENAME_PATTERN = r"^data_\d{4}-\d{2}-\d{2}\.csv$"

def cargar_csv_a_bigquery(request: Request):
    try:
        # Parsear el evento CloudEvent desde GCS (vía Eventarc)
        envelope = request.get_json(silent=True)
        if not envelope:
            return "No data received", 400

        bucket_name = envelope.get("bucket")
        file_name = envelope.get("name")

        if not bucket_name or not file_name:
            return "Missing bucket or file name in event", 400

        print(f"Archivo detectado: gs://{bucket_name}/{file_name}")

        if not re.match(FILENAME_PATTERN, file_name):
            print("Nombre de archivo no válido. Se omite la carga.")
            return "Nombre no válido", 204

        # Leer archivo desde Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        contenido = blob.download_as_text().splitlines()

        reader = csv.DictReader(contenido)
        hoy = datetime.utcnow().date().isoformat()
        rows = [dict(row, load_dt=hoy) for row in reader]

        # Cargar en BigQuery
        bq_client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("load_dt", "DATE"),
            ],
            time_partitioning=bigquery.TimePartitioning(field="load_dt"),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        from io import StringIO
        json_data = "\n".join([json.dumps(row) for row in rows])
        json_file = StringIO(json_data)

        job = bq_client.load_table_from_file(json_file, table_id, job_config=job_config)
        job.result()

        print(f"{len(rows)} filas cargadas exitosamente en {table_id}")
        return "OK", 200

    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        return f"Error: {e}", 500
