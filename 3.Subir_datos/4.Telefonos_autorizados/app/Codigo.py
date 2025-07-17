import pandas as pd
from sqlalchemy import create_engine
import os

# === 1. Leer el archivo Excel desde el volumen ===
archivo_excel = r"D:\Datos\Documents\GitHub\2025\Agente_Libranza\4.SubirTelefono\data\TB_NUMEROS.xlsx"
df = pd.read_excel(archivo_excel)

# === 2. Conexión a Supabase ===
url  = "postgresql://postgres.ixnhworgyhvqsojyocld:Lagobo20255@aws-0-us-east-1.pooler.supabase.com:6543/postgres"

# === 3. Crear motor de conexión ===
engine = create_engine(url, connect_args={"sslmode": "require"})

# === 4. Subir los datos ===
df.to_sql("tb_telefonos", engine, if_exists='replace', index=False)

print("¡Tabla subida correctamente a Supabase!")