FROM python:3.12-slim

# Evitar prompts de configuración interactiva al instalar paquetes
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ="America/Santiago"

# Set working directory
WORKDIR /usr/src/app

# Update system packages y dependencias necesarias
RUN apt-get -y update && \
    apt-get install -y build-essential libatlas-base-dev && \
    rm -rf /var/lib/apt/lists/*

# Copiar archivos del proyecto
COPY . .

# Instalar dependencias Python
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Comando para ejecutar el script
CMD ["python", "-u", "./main.py"]
