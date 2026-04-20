from src.utils import *

def process_aggregation(df_input):
    """
    Función auxiliar para aplicar la misma lógica de agregación 
    tanto a clientes normales como a prosumidores.
    """
    if df_input.empty:
        return pd.DataFrame()

    # 3. Transformacion de tiempo (Colapsar 744 horas en 24)
    df_input['hora_dia'] = ((df_input['hora'] - 1) % 24) + 1

    # 4. Definicion de grupos de identidad
    group_cols = ['rut', 'clave', 'tension', 'hora_dia']
    available_groups = [c for c in group_cols if c in df_input.columns]

    # 5. Calculo de Media y Desviacion Estandar
    df_result = df_input.groupby(available_groups).agg(
        medida_3_mean=('medida_3', 'mean'),
        medida_3_std=('medida_3', 'std')
    ).reset_index()

    # 6. Calculo del MCV Mensual
    df_result['cv_temp'] = np.abs(df_result['medida_3_std'] / df_result['medida_3_mean'].replace(0, np.nan))
    df_result['cv_temp'] = df_result['cv_temp'].fillna(0)
    df_result['mcv_mensual'] = df_result.groupby('clave')['cv_temp'].transform('mean')

    # 7. Limpieza final
    df_result = df_result.rename(columns={'hora_dia': 'hora'})
    df_result = df_result.drop(columns=['cv_temp'])
    df_result['medida_3_std'] = df_result['medida_3_std'].fillna(0)
    
    return df_result

def generate_mean_profile(path_folder):
    # 1. Configuracion de rutas
    folder_path = Path(path_folder)
    parent_folder = folder_path.parent 
    
    output_dir = parent_folder / "Promedio"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Rutas de salida
    output_path_normal = output_dir / f"{parent_folder.name}_mean.parquet"
    output_path_gen = output_dir / f"{parent_folder.name}_outofmean_bygeneration.parquet"

    if output_path_normal.exists():
        print(f"El archivo {output_path_normal.name} ya existe. Saltando...")
        return

    # 2. Carga de archivos
    all_files = list(folder_path.glob("*.parquet"))
    if not all_files:
        print(f"Error: No hay archivos .parquet en {folder_path.name}")
        return

    print(f"Cargando {len(all_files)} archivos...")
    df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # ---------------------------------------------------------
    # NUEVA LÓGICA: IDENTIFICACIÓN Y SEGMENTACIÓN
    # ---------------------------------------------------------
    # Identificamos claves que devuelven a la red (medida_3 > 0)
    claves_con_generacion = df[df['medida_3'] > 0]['clave'].unique()
    
    print(f"Análisis de Segmentación:")
    print(f" - Claves totales: {len(df['clave'].unique())}")
    print(f" - Claves identificadas con generación: {len(claves_con_generacion)}")

    # Dividimos el DataFrame original en dos
    df_gen_raw = df[df['clave'].isin(claves_con_generacion)].copy()
    df_normal_raw = df[~df['clave'].isin(claves_con_generacion)].copy()

    # Procesamos el grupo NORMAL
    print("Procesando perfiles de consumo puro...")
    df_normal_final = process_aggregation(df_normal_raw)
    
    # Procesamos el grupo GENERACIÓN (Prosumidores)
    print("Procesando perfiles con inyección solar/generación...")
    df_gen_final = process_aggregation(df_gen_raw)

    # 8. Guardado independiente
    if not df_normal_final.empty:
        df_normal_final.to_parquet(output_path_normal, compression='snappy')
        print(f"✅ Guardado consumo normal: {output_path_normal.name}")
    
    if not df_gen_final.empty:
        df_gen_final.to_parquet(output_path_gen, compression='snappy')
        print(f"✅ Guardado prosumidores (apartados): {output_path_gen.name}")
    
    print(f"Proceso {parent_folder.name} terminado exitosamente.\n")

def generate_mean_profile_all():
    avaible_yymmm = get_avaible_yymmm()
    for yymm in avaible_yymmm:
        print(f"--- Procesando Periodo {yymm} ---")
        ruta_insumo = get_data_processed_path() / yymm / "Medidas60"
        generate_mean_profile(ruta_insumo)

if __name__ == "__main__":
    generate_mean_profile_all()