import tkinter as tk
from tkinter import ttk, messagebox
import requests
from bs4 import BeautifulSoup
import re

# Función para buscar links de archivos parquet con patrón data_2024-MM.parquet
def buscar_links_parquet(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        links_encontrados = []
        pattern = re.compile(r"data_2024-(0[1-9]|1[0-2])\.parquet$")
        
        for a in soup.find_all("a", href=True):
            href = a['href']
            if pattern.search(href):
                # Construir URL absoluto si href es relativo
                if href.startswith("http"):
                    full_url = href
                else:
                    from urllib.parse import urljoin
                    full_url = urljoin(url, href)
                links_encontrados.append(full_url)
        
        return links_encontrados
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo obtener los links: {e}")
        return []

# Función para copiar el link seleccionado al portapapeles
def copiar_link():
    selected = tree.selection()
    if not selected:
        messagebox.showwarning("Aviso", "Selecciona un link para copiar.")
        return
    link = tree.item(selected[0])['values'][0]
    root.clipboard_clear()
    root.clipboard_append(link)
    messagebox.showinfo("Copiado", "Link copiado al portapapeles.")

# Función para iniciar la búsqueda y mostrar resultados
def iniciar_busqueda():
    url = entry_url.get().strip()
    if not url:
        messagebox.showwarning("Aviso", "Por favor ingresa una URL.")
        return
    boton_buscar.config(state="disabled")
    tree.delete(*tree.get_children())
    links = buscar_links_parquet(url)
    if not links:
        messagebox.showinfo("Resultado", "No se encontraron links con el patrón solicitado.")
    else:
        for link in links:
            tree.insert("", "end", values=[link])
    boton_buscar.config(state="normal")

# --- Interfaz gráfica ---

root = tk.Tk()
root.title("Buscador de archivos parquet 2024")
root.geometry("700x400")
root.resizable(False, False)

frame_top = tk.Frame(root)
frame_top.pack(pady=10, padx=10, fill="x")

tk.Label(frame_top, text="URL de página web:").pack(side="left")
entry_url = tk.Entry(frame_top, width=70)
entry_url.pack(side="left", padx=5)
boton_buscar = tk.Button(frame_top, text="Buscar archivos", command=iniciar_busqueda)
boton_buscar.pack(side="left")

frame_table = tk.Frame(root)
frame_table.pack(pady=10, padx=10, fill="both", expand=True)

tree = ttk.Treeview(frame_table, columns=("link",), show="headings")
tree.heading("link", text="Link al archivo parquet")
tree.column("link", anchor="w", width=680)
tree.pack(side="left", fill="both", expand=True)

scrollbar = ttk.Scrollbar(frame_table, orient="vertical", command=tree.yview)
scrollbar.pack(side="right", fill="y")
tree.configure(yscroll=scrollbar.set)

boton_copiar = tk.Button(root, text="Copiar link seleccionado", command=copiar_link)
boton_copiar.pack(pady=10)

root.mainloop()
