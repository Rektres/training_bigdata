import tkinter as tk

from tkinter import ttk, messagebox, filedialog

from google.cloud import bigquery

from datetime import datetime

import os

import re

import csv

import json

from io import StringIO



# === VARIABLES GLOBALES ===

PROJECT_ID = os.environ.get("PROJECT_ID", "tu-proyecto-id")

BQ_DATASET = os.environ.get("BQ_DATASET", "tu_dataset")

BQ_TABLE = os.environ.get("BQ_TABLE", "tu_tabla")

FILENAME_PATTERN = r"^data_\d{4}-\d{2}-\d{2}\.csv$"

ruta_archivo_seleccionado = None



# === FUNCIONES ===

def cargar_csv_a_bigquery(filepath):

  try:

    with open(filepath, 'r') as f:

      contenido = f.read().splitlines()



    reader = csv.DictReader(contenido)

    hoy = datetime.utcnow().date().isoformat()

    rows = [dict(row, load_dt=hoy) for row in reader]



    client = bigquery.Client()

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



    json_data = "\n".join([json.dumps(row) for row in rows])

    json_file = StringIO(json_data)



    job = client.load_table_from_file(json_file, table_id, job_config=job_config)

    job.result()



    messagebox.showinfo("Éxito", f"{len(rows)} filas cargadas a {table_id}")

  except Exception as e:

    messagebox.showerror("Error", f"Error al subir a BigQuery:\n{e}")

import tkinter as tk

from tkinter import ttk, messagebox, filedialog

from google.cloud import bigquery

from datetime import datetime

import os

import re

import csv

import json

from io import StringIO



# === VARIABLES GLOBALES ===

PROJECT_ID = os.environ.get("PROJECT_ID", "tu-proyecto-id")

BQ_DATASET = os.environ.get("BQ_DATASET", "tu_dataset")

BQ_TABLE = os.environ.get("BQ_TABLE", "tu_tabla")

FILENAME_PATTERN = r"^data_\d{4}-\d{2}-\d{2}\.csv$"

ruta_archivo_seleccionado = None



# === FUNCIONES ===

def cargar_csv_a_bigquery(filepath):

  try:

    with open(filepath, 'r') as f:

      contenido = f.read().splitlines()



    reader = csv.DictReader(contenido)

    hoy = datetime.utcnow().date().isoformat()

    rows = [dict(row, load_dt=hoy) for row in reader]



    client = bigquery.Client()

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

    title="Selecciona un archivo CSV",

    filetypes=[("CSV files", "*.csv")]

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



  with open(filepath, 'r') as f:

    reader = csv.DictReader(f)

    columnas = reader.fieldnames

    tree_preview["columns"] = columnas

    tree_preview["show"] = "headings"



    for col in columnas:

      tree_preview.heading(col, text=col)

      tree_preview.column(col, width=100, anchor="center")



    for i, row in enumerate(reader):

      if i >= 100:

        break

      tree_preview.insert("", "end", values=[row[col] for col in columnas])



def limpiar_preview():

  tree_preview.delete(*tree_preview.get_children())

  for col in tree_preview["columns"]:

    tree_preview.heading(col, text="")

    tree_preview.column(col, width=0)



def confirmar_carga():

  if ruta_archivo_seleccionado:

    cargar_csv_a_bigquery(ruta_archivo_seleccionado)

    boton_confirmar.config(state="disabled")

    label_archivo.config(text="")

    limpiar_preview()

  else:

    messagebox.showwarning("Archivo no seleccionado", "Debes seleccionar un archivo válido antes.")



# === LOGIN ===

def verificar_login():

  usuario = entry_usuario.get()

  clave = entry_clave.get()



  if usuario == "admin" and clave == "1234":

    lanzar_app_principal()

  else:

    messagebox.showerror("Login fallido", "Usuario o clave incorrectos")



def centrar_ventana(ventana):

  ventana.update_idletasks()

  w = 500

  h = 500

  x = (ventana.winfo_screenwidth() // 2) - (w // 2)

  y = (ventana.winfo_screenheight() // 2) - (h // 2)

  ventana.geometry(f'{w}x{h}+{x}+{y}')



def seleccionar_archivos_masivos():

  rutas = filedialog.askopenfilenames(

    title="Selecciona múltiples archivos CSV",

    filetypes=[("CSV files", "*.csv")]

  )

  if not rutas:

    return



  archivos_validos = [ruta for ruta in rutas if re.match(FILENAME_PATTERN, os.path.basename(ruta))]

  archivos_invalidos = [ruta for ruta in rutas if ruta not in archivos_validos]



  if not archivos_validos:

    messagebox.showwarning("Ningún archivo válido", "Ningún archivo cumple con el patrón requerido.")

    return



  total_cargados = 0

  errores = []



  for archivo in archivos_validos:

    try:

      cargar_csv_a_bigquery(archivo)

      total_cargados += 1

    except Exception as e:

      errores.append(f"{os.path.basename(archivo)}: {e}")



  msg = f"{total_cargados} archivos cargados exitosamente."

  if errores:

    msg += f"\n\nErrores:\n" + "\n".join(errores)

    messagebox.showwarning("Carga Masiva con Errores", msg)

  else:

    messagebox.showinfo("Carga Masiva Exitosa", msg)



# === VENTANA PRINCIPAL ===

def lanzar_app_principal():

  login_window.destroy()



  global label_archivo, boton_confirmar, tree_preview



  app = tk.Tk()

  app.title("Cargador BigData GCP")

  app.geometry("500x500")

  app.resizable(False, False)

  centrar_ventana(app)



  label = tk.Label(app, text="Subir datos a BigQuery", font=("Arial", 16))

  label.pack(pady=10)



  boton_subir = tk.Button(app, text="Seleccionar archivo CSV", command=seleccionar_archivo)

  boton_subir.pack(pady=5)



  label_archivo = tk.Label(app, text="", wraplength=400)

  label_archivo.pack(pady=5)



  boton_confirmar = tk.Button(app, text="Confirmar carga", command=confirmar_carga, state="disabled")

  boton_confirmar.pack(pady=5)



  boton_masivo = tk.Button(app, text="Carga Masiva de CSVs", command=seleccionar_archivos_masivos)

  boton_masivo.pack(pady=5)

  # Vista previa (tabla)

  frame_preview = ttk.Frame(app)

  frame_preview.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)



  tree_preview = ttk.Treeview(frame_preview)

  tree_preview.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)



  scrollbar = ttk.Scrollbar(frame_preview, orient="vertical", command=tree_preview.yview)

  scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

  tree_preview.configure(yscroll=scrollbar.set)



  app.mainloop()



# === VENTANA DE LOGIN ===

login_window = tk.Tk()

login_window.title("Login")

login_window.geometry("500x500")

login_window.resizable(False, False)

centrar_ventana(login_window)



tk.Label(login_window, text="Usuario").pack(pady=(100, 5))

entry_usuario = tk.Entry(login_window)

entry_usuario.pack(pady=5)



tk.Label(login_window, text="Contraseña").pack(pady=5)

entry_clave = tk.Entry(login_window, show="*")

entry_clave.pack(pady=5)



boton_login = tk.Button(login_window, text="Iniciar sesión", command=verificar_login)

boton_login.pack(pady=20)



login_window.mainloop()

