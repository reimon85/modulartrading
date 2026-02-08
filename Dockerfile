# --- Etapa 1: Builder ---
# Esta etapa instala las dependencias, incluyendo las de compilación.
FROM python:3.11-slim-bullseye as builder

# Instalar dependencias del sistema necesarias para compilar paquetes de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo
WORKDIR /app

# Crear un entorno virtual
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copiar solo el fichero de dependencias e instalarlas
# Usamos el .lock.txt para tener builds reproducibles.
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt

# --- Etapa 2: Runtime ---
# Esta es la imagen final, mucho más ligera y segura.
FROM python:3.11-slim-bullseye as runtime

# Crear un usuario no-root para ejecutar la aplicación
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /home/appuser/app
USER appuser

# Copiar el entorno virtual con las dependencias de la etapa 'builder'
COPY --from=builder /opt/venv /opt/venv

# Copiar el código de la aplicación
COPY --chown=appuser:appuser . .

# Hacer que el entorno virtual sea el intérprete de python por defecto
ENV PATH="/opt/venv/bin:$PATH"

# Exponer el puerto para el dashboard
EXPOSE 8501

# Comando por defecto (será sobreescrito por docker-compose)
CMD ["streamlit", "run", "dashboard/app.py", "--server.port", "8501", "--server.headless", "true"]