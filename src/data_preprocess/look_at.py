import pandas as pd
from pathlib import Path

# Configuración
archivo_path = Path("data/processed/2602/Promedio/2602_mean.parquet")
clave_a_buscar = "1393575ENEL_GX" # Cambia esto por la clave que quieras ver

# Carga y filtrado
if archivo_path.exists():
    df = pd.read_parquet(archivo_path)
    
    # Buscamos la clave
    resultado = df[df['clave'] == clave_a_buscar]
    
    if not resultado.empty:
        print(f"--- Datos encontrados para la clave: {clave_a_buscar} ---")
        print(resultado)
    else:
        print(f"❌ La clave '{clave_a_buscar}' no existe en este archivo.")
else:
    print("Archivo no encontrado.")