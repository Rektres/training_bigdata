import re
import json
from datetime import datetime, date
from io import StringIO
import pyarrow.parquet as pq
import gcsfs
from google.cloud import bigquery, storage

FILENAME_PATTERN = r".*/(green_tripdata|fhv_tripdata|yellow_tripdata|fhvhv_tripdata)_2024-(0[1-9]|1[0-2])\.parquet$"

TABLAS_BIGQUERY = {
    "green_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-green",
    "yellow_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-yellow",
    "fhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhv",
    "fhvhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhvhv",
}

bq_client = bigquery.Client()

def columna_existe_en_tabla(tabla, nombre_columna):
    return any(field.name == nombre_columna for field in tabla.schema)

def columnas_iguales(tabla_id, columnas_parquet):
    try:
        tabla = bq_client.get_table(tabla_id)
        columnas_tabla = {field.name.lower() for field in tabla.schema} - {"load_pt", "source_file_name"}
        columnas_parquet = {col.lower() for col in columnas_parquet}
        return columnas_tabla == columnas_parquet
    except Exception as e:
        print(f"[ERROR] No se pudo validar columnas: {e}")
        return False


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
              AND load_pt IS NOT NULL
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

def tipos_datos_coinciden(tabla_id, tabla_parquet):
    try:
        tabla_bq = bq_client.get_table(tabla_id)
        esquema_bq = {
            field.name.lower(): field.field_type.lower()
            for field in tabla_bq.schema
            if field.name.lower() not in {"load_pt", "source_file_name"}
        }

        esquema_parquet = {}
        for field in tabla_parquet.schema:
            nombre = field.name.lower()
            tipo_parquet = str(field.type).lower()

            mapeo_tipos = {
                'int32': 'integer',
                'int64': 'integer',
                'float32': 'float',
                'float64': 'float',
                'double': 'float',
                'string': 'string',
                'bool': 'boolean',
                'timestamp[ms]': 'timestamp',
                'timestamp[us]': 'timestamp',  
                'date32': 'date'
            }

            tipo_bq_equivalente = mapeo_tipos.get(tipo_parquet, tipo_parquet)
            esquema_parquet[nombre] = tipo_bq_equivalente

        for columna, tipo_bq in esquema_bq.items():
            if columna not in esquema_parquet:
                print(f"[WARNING] Columna {columna} existe en BigQuery pero no en Parquet")
                return False

            if esquema_parquet[columna] != tipo_bq:
                print(f"[WARNING] Tipo discrepante para {columna}: Parquet={esquema_parquet[columna]}, BQ={tipo_bq}")
                return False

        return True

    except Exception as e:
        print(f"[ERROR] Error al validar tipos de datos: {e}")
        return False

def cargar_datos_a_bigquery(tabla_id, tabla_parquet, file_name, batch_size=100000):
    try:
        tabla_bq = bq_client.get_table(tabla_id)
        df = tabla_parquet.slice(0, batch_size).to_pandas()

        if len(df) == 0:
            print("[INFO] No hay registros para cargar")
            return True

        current_time = datetime.utcnow()
        df.insert(0, 'load_pt', current_time)
        df['source_file_name'] = file_name

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=tabla_bq.schema,
        )

        job = bq_client.load_table_from_dataframe(
            df, tabla_id, job_config=job_config
        )
        job.result()

        print(f"[SUCCESS] Cargados {len(df)} registros a {tabla_id}")
        return True

    except Exception as e:
        print(f"[ERROR] Error al cargar datos: {e}")
        return False

def procesar_parquet_a_bigquery(event, context):
    tabla_id = None
    try:
        bucket_name = event['bucket']
        file_name = event['name']
        print(f"[DEBUG] Evento recibido para archivo: {file_name} en bucket: {bucket_name}")

        if not file_name.startswith('data-trip-2024/'):
            print(f"[INFO] Archivo ignorado por no estar en la ruta data-trip-2024/: {file_name}")
            return

        if not re.search(FILENAME_PATTERN, file_name):
            print(f"[DEBUG] Validando patrón para: {file_name}")
            print(f"[DEBUG] Patrón usado: {FILENAME_PATTERN}")
            print(f"[DEBUG] Resultado match: {bool(re.search(FILENAME_PATTERN, file_name))}")
            print(f"[INFO] Archivo ignorado por no coincidir con patrón: {file_name}")
            return

        base_name = file_name.split('/')[-1]
        prefix = "_".join(base_name.split("_")[:2])

        print(f"[DEBUG] Nombre base del archivo: {base_name}")
        print(f"[DEBUG] Prefijo extraído: {prefix}")

        if prefix not in TABLAS_BIGQUERY:
            print(f"[WARN] Prefijo del archivo no tiene tabla configurada: {prefix}")
            print(f"[DEBUG] Tablas configuradas: {list(TABLAS_BIGQUERY.keys())}")
            return

        tabla_id = TABLAS_BIGQUERY[prefix]

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_bytes()

        import io
        buffer = io.BytesIO(data)
        tabla_parquet = pq.read_table(buffer)

        if not columnas_iguales(tabla_id, tabla_parquet.schema.names):
            print(f"[ERROR] Las columnas del archivo {file_name} NO coinciden con las de la tabla {tabla_id}.")
            return

        if not tipos_datos_coinciden(tabla_id, tabla_parquet):
            print(f"[ERROR] Los tipos de datos del archivo {file_name} NO coinciden con los de la tabla {tabla_id}.")
            return

        if not cargar_datos_a_bigquery(tabla_id, tabla_parquet, file_name):
            print(f"[ERROR] Falló la carga de datos para {file_name}")
            return

        print(f"[SUCCESS] Procesamiento completado para {file_name}")

    except Exception as e:
        if tabla_id:
            print(f"[ERROR] Error general en el procesamiento del archivo {file_name} para tabla {tabla_id}: {e}")
        else:
            print(f"[ERROR] Error general en el procesamiento del archivo {file_name}: {e}")
