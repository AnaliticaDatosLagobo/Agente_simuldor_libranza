# Usa una imagen oficial de Python
FROM python:3.11.9-slim

# Establece directorio de trabajo
WORKDIR /app

# Copia requirements y los instala
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código
COPY . .

# Expone el puerto que usará uvicorn
EXPOSE 10000

# Comando para correr la app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]