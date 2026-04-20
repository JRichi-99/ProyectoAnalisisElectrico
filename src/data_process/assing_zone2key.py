from src.utils import *

def assign_zone2key(n_period):
    # 1. Cargar el Maestro de Zonas desde los Excel
    zone_files_path = get_data_raw_path() / "ZonaClaves"
    files = list(zone_files_path.glob("*.xlsx"))
    
    df_key_zone = pd.DataFrame()
    for file in files:
        temp_df = pd.read_excel(file)
        df_key_zone = pd.concat([df_key_zone, temp_df], ignore_index=True)

    # Limpieza del Maestro: Aseguramos que 'clave' sea string y quitamos duplicados 
    # por si una clave aparece en dos Excel distintos por error.
    df_key_zone['clave'] = df_key_zone['clave'].astype(str).str.strip()
    df_key_zone = df_key_zone.drop_duplicates(subset=['clave'])

    # 2. Cargar el DataFrame de datos (Promedio del Periodo)
    period_folder = get_data_processed_path() / f"Period{n_period}"
    data_path = period_folder / f"period{n_period}.parquet"
    
    if not data_path.exists():
        print(f"Error: No se encontró el archivo en {data_path}")
        return None

    data_df = pd.read_parquet(data_path)
    # Aseguramos consistencia en el tipo de dato de la clave
    data_df['clave'] = data_df['clave'].astype(str).str.strip()

    # 3. Realizar el Cruce (Merge)
    # how='inner' hace que si la clave NO está en el maestro de zonas, se elimine del resultado.
    # Esto crea un nuevo DataFrame 'df_final' sin tocar 'data_df'.
    print(f"Cruzando datos del Periodo {n_period} con maestro de zonas...")
    df_final = pd.merge(data_df, df_key_zone[['clave', 'zona']], on='clave', how='inner')

    # 4. Reporte de resultados
    claves_iniciales = data_df['clave'].nunique()
    claves_finales = df_final['clave'].nunique()
    descartadas = claves_iniciales - claves_finales

    if descartadas > 0:
        print(f"⚠️ Se descartaron {descartadas} claves por no tener una zona asignada en los Excel.")
    print(f"✅ Claves resultantes con zona: {claves_finales}")

    # 5. Guardar el nuevo DataFrame consolidado
    output_path = period_folder / f"period{n_period}_with_zones.parquet"
    df_final.to_parquet(output_path, compression='snappy')
    
    print(f"Archivo final guardado en: {output_path.name}")
    
    return df_final

if __name__ == "__main__":
    # Ejemplo para procesar el Period0
    df_zonificado = assign_zone2key(0)