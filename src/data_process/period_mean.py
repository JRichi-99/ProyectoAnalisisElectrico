from src.utils import *

def generate_output_folder():
    """
    Escanea el directorio para crear la carpeta incremental PeriodX.
    """
    data_processed = get_data_processed_path()
    
    existing_periods = []
    for folder in data_processed.iterdir():
        if folder.is_dir() and folder.name.startswith("Period"):
            num_part = folder.name.replace("Period", "")
            if num_part.isdigit():
                existing_periods.append(int(num_part))
    
    next_num = max(existing_periods) + 1 if existing_periods else 0
    output_folder = data_processed / f"Period{next_num}"
    output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder

def generate_meta_csv(period, output_folder):
    csv_path = output_folder / "periodos_incluidos.csv"
    df_meta = pd.DataFrame(period, columns=['periodo_procesado'])
    df_meta.to_csv(csv_path, index=False)

def generate_period_mean(period):
    if not period:
        period = get_avaible_yymmm()
        print(f"No se especificó ningún periodo. Se procesarán todos los disponibles: {period}")
    output_folder = generate_output_folder()
    generate_meta_csv(period, output_folder)
    
    # 2. Carga selectiva y busqueda de claves comunes
    all_dfs = []
    sets_de_claves = []
    
    # Definimos que columnas de identificacion queremos mantener
    id_cols = ['rut', 'clave', 'tension', 'hora']

    for p in period:
        file_path = get_data_processed_path()/ p / "Promedio" / f"{p}_mean.parquet"
        if file_path.exists():
            df_temp = pd.read_parquet(file_path)
            
            # Solo conservamos las columnas de identidad y el promedio de energia
            # Esto descarta automaticamente 'medida_3_std' y 'mcv_mensual' previos
            cols_to_keep = [c for c in id_cols + ['medida_3_mean'] if c in df_temp.columns]
            df_temp = df_temp[cols_to_keep]
            
            all_dfs.append(df_temp)
            sets_de_claves.append(set(df_temp['clave'].unique()))

    if not all_dfs:
        print("No hay archivos para procesar.")
        return

    # 3. Interseccion para mantener solo clientes consistentes
    claves_comunes = set.intersection(*sets_de_claves)
    print(f"Claves totales encontradas: {len(set().union(*sets_de_claves))}")
    print(f"Claves comunes en todos los periodos: {len(claves_comunes)}")
    print(f"Claves descartadas por incompletas: {len(set().union(*sets_de_claves)) - len(claves_comunes)}")

    # 4. Fusion y Recalculo de Estadisticas
    df_total = pd.concat(all_dfs, ignore_index=True)
    df_total = df_total[df_total['clave'].isin(claves_comunes)]

    # Quitar claves con distinto rut o tension entre meses (si es que quedaron)

    print("Calculando nuevas metricas para el periodo...")
    
    # Agrupamos para obtener la nueva media y la nueva desviacion estandar
    available_groups = [c for c in id_cols if c in df_total.columns]
    
    df_final = df_total.groupby(available_groups).agg(
        medida_3_mean=('medida_3_mean', 'mean'),
        medida_3_std=('medida_3_mean', 'std') # Desviacion entre los promedios mensuales
    ).reset_index()

    expected_rows = 24
    counts = df_final.groupby('clave').size()
    
    claves_invalidas = counts[counts != expected_rows].index.tolist()

    if claves_invalidas:
        print(f"⚠️ FILTRO CRÍTICO: Se eliminan {len(claves_invalidas)} claves por inconsistencia de RUT o Tensión.")
        # Quitamos esas claves del dataframe final
        df_final = df_final[~df_final['clave'].isin(claves_invalidas)]
    else:
        print("✅ Integridad confirmada: Todas las claves mantuvieron RUT y Tensión estables.")
        
    # Rellenamos ceros en std (si solo hay un mes o variacion nula)
    df_final['medida_3_std'] = df_final['medida_3_std'].fillna(0)

    # 5. Nuevo MCV del Periodo
    # CV horario = std_periodo / mean_periodo
    df_final['cv_temp'] = np.abs(df_final['medida_3_std'] / df_final['medida_3_mean'].replace(0, np.nan))
    df_final['cv_temp'] = df_final['cv_temp'].fillna(0)

    # MCV del periodo = promedio de los 24 CV horarios
    df_final['mcv_periodo'] = df_final.groupby('clave')['cv_temp'].transform('mean')
    
    # Limpiamos columna auxiliar
    df_final = df_final.drop(columns=['cv_temp'])


    # --- 5. NORMALIZACIÓN DEL PERFIL (Hora / Total) ---
    print("Normalizando perfiles por suma total diaria...")
    
    # Calculamos la energía total diaria real en kWh
    df_final['energia_total_diaria'] = df_final.groupby('clave')['medida_3_mean'].transform('sum')

    # ---------------------------------------------------------
    # NUEVO: Filtro de Energía Total Diaria igual a 0
    # ---------------------------------------------------------
    claves_energia_cero = df_final[df_final['energia_total_diaria'] == 0]['clave'].unique()
    
    if len(claves_energia_cero) > 0:
        print(f"⚠️ FILTRO CONSUMO NULO: Se eliminan {len(claves_energia_cero)} claves por tener energía total diaria igual a 0.")
        df_final = df_final[~df_final['clave'].isin(claves_energia_cero)]
        
    if df_final.empty:
        print("❌ Error: No quedaron datos tras el filtro de energía nula.")
        return

    # --- 5. NORMALIZACIÓN DEL PERFIL (Hora / Total) ---
    print("Normalizando perfiles por suma total diaria...")
    
    # Calculamos la energía total diaria real en kWh
    df_final['energia_total_diaria'] = df_final.groupby('clave')['medida_3_mean'].transform('sum')
    denom = df_final['energia_total_diaria'].replace(0, 1) # Prevenir división por cero

    # Transformamos a porcentaje del total
    df_final['medida_3_mean'] = df_final['medida_3_mean'] / denom
    df_final['medida_3_std'] = df_final['medida_3_std'] / abs(denom)


    # 6. Guardado
    filename = f"{output_folder.stem.lower()}.parquet"
    output_path = output_folder / filename
    df_final.to_parquet(output_path, compression='snappy')

    print(f"Archivo de periodo guardado: {filename}")


if __name__ == "__main__":
    generate_period_mean([])
