
# 🚖 BigQuery Loader for NYC Tripdata Files (`*.parquet`)

Este proyecto implementa una **Google Cloud Function** en Python que automatiza la validación y carga de archivos `.parquet` del dataset **NYC TLC Trip Data** (amarillo, verde, FHV, FHVHV) desde un bucket de **Google Cloud Storage** a **BigQuery**.

---

## 📂 Estructura de archivos esperada

Los archivos deben almacenarse en el bucket bajo la carpeta:

```
data-trip-2024/
```

Y deben seguir uno de los siguientes formatos de nombre:

- `yellow_tripdata_2024-MM.parquet`
- `green_tripdata_2024-MM.parquet`
- `fhv_tripdata_2024-MM.parquet`
- `fhvhv_tripdata_2024-MM.parquet`

> Ejemplo válido: `data-trip-2024/yellow_tripdata_2024-01.parquet`

---

## 🧠 ¿Qué hace esta función?

Al recibir un evento de Cloud Storage:

1. ✅ **Valida el nombre del archivo** con expresión regular.
2. ✅ **Determina la tabla de destino** en BigQuery según el prefijo.
3. ✅ **Lee el archivo `.parquet`** directamente desde Cloud Storage.
4. ✅ **Verifica coincidencia de columnas** y tipos de datos entre el archivo y la tabla.
5. ✅ **Evita cargas duplicadas** revisando si el archivo ya fue procesado.
6. ✅ **Agrega columnas** `load_pt` (timestamp de carga) y `source_file_name`.
7. ✅ **Carga un batch de hasta 100,000 registros** en modo `WRITE_APPEND` a BigQuery.
8. 📦 **Registra logs detallados** para seguimiento y debugging.

---

## 🗃️ Tablas de destino en BigQuery

El mapeo de prefijos a tablas está definido en el diccionario `TABLAS_BIGQUERY`:

```python
TABLAS_BIGQUERY = {
    "green_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-green",
    "yellow_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-yellow",
    "fhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhv",
    "fhvhv_tripdata": "bigdata-458022.3_dev_tlc_us_nyc_bronze.data-trip-fhvhv",
}
```

> Asegúrate de que estas tablas **existen** en BigQuery y contengan **al menos las columnas esperadas**, incluyendo `source_file_name`.

---

## ✅ Requisitos

- Google Cloud Project con habilitado:
  - BigQuery
  - Cloud Functions
  - Cloud Storage
- Tabla de BigQuery creada por dataset con esquema compatible
- Permisos adecuados (IAM):
  - `roles/bigquery.dataEditor`
  - `roles/storage.objectViewer`

---

## ⚙️ Despliegue

Puedes desplegar esta función con:

```bash
gcloud functions deploy procesar_parquet_a_bigquery \
  --runtime python310 \
  --trigger-resource <NOMBRE_DEL_BUCKET> \
  --trigger-event google.storage.object.finalize \
  --entry-point procesar_parquet_a_bigquery \
  --memory 512MB \
  --timeout 540s \
  --allow-unauthenticated
```

---

## 🧪 Testeo local

Para pruebas locales, puedes simular un evento GCS:

```python
event = {
    "bucket": "mi-bucket",
    "name": "data-trip-2024/yellow_tripdata_2024-01.parquet"
}
context = None

procesar_parquet_a_bigquery(event, context)
```

---

## 🛠️ Dependencias

- `google-cloud-bigquery`
- `google-cloud-storage`
- `pyarrow`
- `pandas`
- `gcsfs`

Instalación local:

```bash
pip install google-cloud-bigquery google-cloud-storage pyarrow pandas gcsfs
```

---

## 📌 Consideraciones

- Solo se procesan los primeros **100,000 registros** por archivo para evitar límites de memoria y tiempo de ejecución.
- La columna `source_file_name` se usa para evitar cargas duplicadas.
- Es obligatorio que las columnas y sus tipos coincidan entre el archivo y la tabla destino.

---

## 📞 Contacto

Este proyecto fue desarrollado para cargas automatizadas de datos de movilidad urbana.  
Para preguntas o mejoras, abre un issue o pull request.
