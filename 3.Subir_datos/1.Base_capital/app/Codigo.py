import pandas as pd
from sqlalchemy import create_engine

# === 1. Leer el archivo Excel ===
#archivo_excel = "data/TB_BASE.xlsx"
archivo_excel = r"D:\Datos\Documents\GitHub\2025\Agente_Libranza\3.Subir_datos\1.Base_capital\data\Data_final.xlsx" 
df = pd.read_excel(archivo_excel)

# === 2. Datos de conexión a Supabase ===
# Copia la URL del panel de Supabase > Settings > Database > Connection string (URI)
url  = "postgresql://postgres.ixnhworgyhvqsojyocld:Lagobo20255@aws-0-us-east-1.pooler.supabase.com:6543/postgres" 

# === 3. Crear conexión con SSL obligatorio ===
engine = create_engine(url, connect_args={"sslmode": "require"})

# === 4. Subir a la nube (Supabase) ===
#df.to_sql("data_libranza", engine, if_exists='replace', index=False)
df.to_sql("data_final_2", engine, if_exists='replace', index=False)

print(" ¡Tabla subida correctamente a Supabase!")