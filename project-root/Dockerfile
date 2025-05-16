# Usa una imagen base con Python y GUI soportada (para tkinter, Debian es recomendable)
FROM python:3.11-slim

# Instala dependencias del sistema necesarias para Tkinter y otras bibliotecas
RUN apt-get update && apt-get install -y \
    tk \
    libgl1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Crea un directorio para el proyecto
WORKDIR /app

# Copia todo el código de las 3 apps
COPY app_ver-parquet/ /app/app_ver-parquet/
COPY app_parquet-json/ /app/app_parquet-json/
COPY app_scraping-links/ /app/app_scraping-links/

# Instala los requisitos de cada app
RUN pip install --upgrade pip

# Instala los requisitos de cada módulo por separado
RUN pip install -r app_ver-parquet/requirements.txt
RUN pip install -r app_parquet-json/requirements.txt
RUN pip install -r app_scraping-links/requirements.txt

# Define un script para elegir qué app lanzar (puede ser override con CMD)
CMD [ "bash" ]
