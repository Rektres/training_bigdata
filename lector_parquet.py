import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from google.cloud import bigquery
from datetime import datetime
import os
import re
import json
from io import StringIO
import pyarrow.parquet as pq
import pandas as pd

# === VARIABLES GLOBALES ===
PROJECT_ID = os.environ.get("PROJECT_ID", "tu-proyecto-id")
BQ_DATASET = os.environ.get("BQ_DATASET", "tu_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "tu_tabla")
FILENAME_PATTERN = r"^data_\d{4}-\d{2}-\d{2}\.parquet$"
ruta_archivo_seleccionado = None

# === FUNCIONES ===

def cargar_parquet_a_bigquery(filepath):
    try:
        df = pq.read_table(filepath).to_pandas()
        hoy = datetime.utcnow().date().isoformat()
        df["load_dt"] = hoy

        rows = df.to_dict(orient="records")

        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
        if "id" in df.columns:
            schema = [bigquery.SchemaField("id", "INTEGER" if df["id"].dtype.kind in "iu" else "STRING")] + \
                     [bigquery.SchemaField(col, "STRING") for col in df.columns if col != "id"]
        if "load_dt" not in df.columns:
            schema.append(bigquery.SchemaField("load_dt", "DATE"))

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(field="load_dt"),
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        json_data = "\n".join([json.dumps(row) for row in rows])
        json_file = StringIO(json_data)

        job = client.load_table_from_file(json_file, table_id, job_config=job_config)
        job.result()

        messagebox.showinfo("Éxito", f"{len(rows)} filas cargadas a {table_id}")
    except Exception as e:
        messagebox.showerror("Error", f"Error al subir a BigQuery:\n{e}")


def seleccionar_archivo():
    global ruta_archivo_seleccionado
    filepath = filedialog.askopenfilename(
        title="Selecciona un archivo PARQUET",
        filetypes=[("Parquet files", "*.parquet")]
    )
    if filepath:
        filename = os.path.basename(filepath)
        if not re.match(FILENAME_PATTERN, filename):
            messagebox.showwarning("Nombre inválido", "El nombre del archivo no cumple el patrón requerido.")
            ruta_archivo_seleccionado = None
            boton_confirmar.config(state="disabled")
            limpiar_preview()
            return

        ruta_archivo_seleccionado = filepath
        label_archivo.config(text=f"Archivo seleccionado:\n{filename}")
        boton_confirmar.config(state="normal")
        mostrar_preview_en_principal(filepath)


def mostrar_preview_en_principal(filepath):
    limpiar_preview()
    try:
        df = pq.read_table(filepath).to_pandas()
        columnas = df.columns.tolist()
        tree_preview["columns"] = columnas
        tree_preview["show"] = "headings"

        for col in columnas:
            tree_preview.heading(col, text=col)
            tree_preview.column(col, width=100, anchor="center")

        for _, row in df.head(100).iterrows():
            tree_preview.insert("", "end", values=[row[col] for col in columnas])
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo leer el archivo Parquet:\n{e}")


def limpiar_preview():
    tree_preview.delete(*tree_preview.get_children())
    for col in tree_preview["columns"]:
        tree_preview.heading(col, text="")
        tree_preview.column(col, width=0)


def confirmar_carga():
    if ruta_archivo_seleccionado:
        cargar_parquet_a_bigquery(ruta_archivo_seleccionado)
        boton_confirmar.config(state="disabled")
        label_archivo.config(text="")
        limpiar_preview()
    else:
        messagebox.showwarning("Archivo no seleccionado", "Debes seleccionar un archivo válido antes.")

# === UI PRINCIPAL ===

def lanzar_app():
    global label_archivo, boton_confirmar, tree_preview

    app = tk.Tk()
    app.title("Cargador Parquet a BigQuery")
    app.geometry("1200x800")
    app.resizable(False, False)

    label = tk.Label(app, text="Subir datos PARQUET a BigQuery", font=("Arial", 16))
    label.pack(pady=10)

    boton_subir = tk.Button(app, text="Seleccionar archivo PARQUET", command=seleccionar_archivo)
    boton_subir.pack(pady=5)

    label_archivo = tk.Label(app, text="", wraplength=500)
    label_archivo.pack(pady=5)

    boton_confirmar = tk.Button(app, text="Confirmar carga", command=confirmar_carga, state="disabled")
    boton_confirmar.pack(pady=5)

    # Vista previa
    frame_preview = ttk.Frame(app)
    frame_preview.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    tree_preview = ttk.Treeview(frame_preview)
    tree_preview.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    scrollbar = ttk.Scrollbar(frame_preview, orient="vertical", command=tree_preview.yview)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    tree_preview.configure(yscroll=scrollbar.set)

    app.mainloop()

# === INICIO ===
if __name__ == "__main__":
    lanzar_app()
