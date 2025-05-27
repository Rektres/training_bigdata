import re
import json
from datetime import datetime, date
from io import StringIO
import pyarrow.parquet as pq
import gcsfs
from google.cloud import bigquery, storage

# Patrón para validar nombres de archivos aceptados
FILENAME_PATTERN = r"^(green_tripdata|fhv_tripdata|yellow_tripdata|fhvhv_tripdata)_2024-(0[1-9]|1[0-2])\.parquet$"

# Mapeo de prefijos de archivo a tablas de BigQuery
TABLAS_BIGQUERY = {
    "green_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-green",
    "yellow_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-yellow",
    "fhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhv",
    "fhvhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhvhv",
}

bq_client = bigquery.Client()

# Convierte fechas a string ISO y otros objetos a string
def convertir_a_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

# Infieren el tipo de datos de BigQuery basado en el valor del campo
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

# Verifica si una columna existe en una tabla de BigQuery
def columna_existe_en_tabla(tabla, nombre_columna):
    return any(field.name == nombre_columna for field in tabla.schema)

# Agrega columnas que no existen actualmente en la tabla de BigQuery
def agregar_columnas_faltantes(tabla_id, esquema_nuevo):
    try:
        tabla = bq_client.get_table(tabla_id)
        esquema_existente = {field.name.lower(): field for field in tabla.schema}

        columnas_a_agregar = [campo for campo in esquema_nuevo if campo.name.lower() not in esquema_existente]

        if columnas_a_agregar:
            esquema_actualizado = tabla.schema[:] + columnas_a_agregar
            tabla.schema = esquema_actualizado
            bq_client.update_table(tabla, ["schema"])
            print(f"[INFO] Se agregaron {len(columnas_a_agregar)} columnas a la tabla {tabla_id}")
        else:
            print(f"[INFO] No hay columnas nuevas para agregar a la tabla {tabla_id}")
    except Exception as e:
        print(f"[ERROR] Error al agregar columnas a la tabla {tabla_id}: {e}")

# Verifica si el archivo ya ha sido cargado previamente consultando por source_file_name
def archivo_ya_cargado(tabla_id, archivo_nombre):
    try:
        tabla = bq_client.get_table(tabla_id)

        if not columna_existe_en_tabla(tabla, "source_file_name"):
            print(f"[INFO] La columna 'source_file_name' no existe en la tabla {tabla_id}, se asume que el archivo no fue cargado.")
            return False

        query = f"""
            SELECT COUNT(1) as count
            FROM `{tabla_id}`
            WHERE source_file_name = @archivo_nombre
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("archivo_nombre", "STRING", archivo_nombre)
            ]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        for row in results:
            return row.count > 0
        return False
    except Exception as e:
        print(f"[ERROR] Error al verificar si el archivo ya fue cargado: {e}")
        return False

# Función principal que se dispara cuando se sube un archivo al bucket
def procesar_parquet_a_bigquery(event, context):
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        print(f"[DEBUG] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

        if not re.search(FILENAME_PATTERN, file_name):
            print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
            return

        prefix = "_".join(file_name.split("_")[:2])
        if prefix not in TABLAS_BIGQUERY:
            print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {prefix}")
            return

        tabla_id = TABLAS_BIGQUERY[prefix]
        fecha_actual = datetime.utcnow().date()

        if archivo_ya_cargado(tabla_id, file_name):
            print(f"[INFO] El archivo {file_name} ya fue cargado previamente. Saltando...")
            return

        # Descarga el archivo Parquet desde Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_bytes()

        import io
        buffer = io.BytesIO(data)
        tabla_parquet = pq.read_table(buffer)

        # Tomamos solo los primeros 100,000 registros para carga parcial
        subset = tabla_parquet.slice(0, 100000)
        registros = subset.to_pydict()

        if not registros:
            print("[WARN] No se encontraron registros en el archivo parquet.")
            return

        # Convertimos registros a JSON agregando campos auxiliares
        json_data = [
            {**dict(zip(registros.keys(), valores)), "load_pt": fecha_actual.isoformat(), "source_file_name": file_name}
            for valores in zip(*registros.values())
        ]

        print(f"[INFO] {len(json_data)} registros listos para cargar a BigQuery.")

        # Inferimos el esquema de los datos para la tabla BQ
        esquema_nuevo = [
            bigquery.SchemaField(col, _infer_bq_type(val[0]))
            for col, val in registros.items()
            if not col.upper().startswith("_FILE_")
        ]
        esquema_nuevo.append(bigquery.SchemaField("load_pt", "DATE"))
        esquema_nuevo.append(bigquery.SchemaField("source_file_name", "STRING"))

        # Asegura que las columnas estén en la tabla
        agregar_columnas_faltantes(tabla_id, esquema_nuevo)

        # Convertimos los datos a JSON para cargar
        json_str = "\n".join([json.dumps(row, default=convertir_a_serializable) for row in json_data])
        json_file = StringIO(json_str)

        # Configuración de carga a BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=esquema_nuevo,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        load_job = bq_client.load_table_from_file(json_file, tabla_id, job_config=job_config)
        load_job.result()

        print(f"[SUCCESS] Archivo {file_name} cargado con éxito. {len(json_data)} filas insertadas.")
    except Exception as e:
        print(f"[ERROR] Error general en el procesamiento del archivo {file_name}: {e}")
