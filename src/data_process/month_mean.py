import pandas as pd
import numpy as np
from pathlib import Path
from src.utils import *

def generate_mean_profile(path_folder):
    # 1. Configuracion de rutas
    folder_path = Path(path_folder)
    parent_folder = folder_path.parent 
    
    output_dir = parent_folder / "Promedio"
    output_path = output_dir / f"{parent_folder.name}_mean.parquet"

    if output_path.exists():
        print(f"El archivo {output_path.name} ya existe. Saltando...")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # 2. Carga de archivos
    all_files = list(folder_path.glob("*.parquet"))
    if not all_files:
        print(f"Error: No hay archivos .parquet en {folder_path.name}")
        return

    print(f"Cargando y procesando {len(all_files)} archivos...")
    df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # 3. Transformacion de tiempo (Colapsar 744 horas en 24)
    df['hora_dia'] = ((df['hora'] - 1) % 24) + 1

    # 4. Definicion de grupos de identidad
    group_cols = ['nombre_barra', 'rut', 'clave', 'tension', 'tipo', 'hora_dia']
    available_groups = [c for c in group_cols if c in df.columns]

    # 5. Calculo de Media y Desviacion Estandar (Agregacion inicial)
    print("Calculando estadisticas horarias...")
    df_result = df.groupby(available_groups).agg(
        medida_3_mean=('medida_3', 'mean'),
        medida_3_std=('medida_3', 'std')
    ).reset_index()

    # 6. Calculo del MCV Mensual (Agregado mientras se procesa)
    # Calculamos el CV para cada una de las 24 horas
    df_result['cv_temp'] = np.abs(df_result['medida_3_std'] / df_result['medida_3_mean'].replace(0, np.nan))
    df_result['cv_temp'] = df_result['cv_temp'].fillna(0)

    # Usamos transform para promediar esos 24 CVs y repartir el resultado por clave
    print("Integrando metrica de ruido global (MCV)...")
    df_result['mcv_mensual'] = df_result.groupby('clave')['cv_temp'].transform('mean')

    # 7. Limpieza final y formateo
    df_result = df_result.rename(columns={'hora_dia': 'hora'})
    df_result = df_result.drop(columns=['cv_temp']) # Eliminamos la columna auxiliar
    
    # Rellenamos nulos en std por si hubo algun solo dato
    df_result['medida_3_std'] = df_result['medida_3_std'].fillna(0)

    # 8. Guardado
    df_result.to_parquet(output_path, compression='snappy')
    
    print(f"Proceso terminado exitosamente.")
    print(f"Archivo: {output_path}")
    print(f"Columnas finales: {list(df_result.columns)}")

def generate_mean_profile_all():
    avaible_yymmm = get_avaible_yymmm()
    for yymm in avaible_yymmm:
        print(f"\nProcesando {yymm}...")
        ruta_insumo = get_data_processed_path() / yymm / "Medidas60"
        generate_mean_profile(ruta_insumo)

if __name__ == "__main__":
    generate_mean_profile_all()
