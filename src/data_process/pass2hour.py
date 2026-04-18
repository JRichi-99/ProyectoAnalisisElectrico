import pandas as pd
from pathlib import Path
from src.utils import *

def count_integrity(input_path : Path):    
    # Leemos solo la columna clave para no saturar la RAM
    df = pd.read_parquet(input_path, columns=['clave'])
    
    # Contamos cuántas filas tiene cada clave y vemos si hay variaciones
    resumen = df['clave'].value_counts().value_counts()
    
    print(f"--- Resumen de Filas por Clave ---")
    print(resumen)
    
    if len(resumen) == 1:
        print(f"\nTodas las claves tienen {resumen.index[0]} filas.")
        return True
    else:
        print(f"\nERROR: Hay {len(resumen)} grupos con distinta cantidad de filas.")
        return False

def quarter2hour(input_path : Path):

    output_path = input_path.parent.parent/ "Medidas60" / f"{input_path.stem}_hourly{input_path.suffix}"
    if not count_integrity(input_path):
        print("Integridad de datos comprometida. No se realizará la agregación horaria.")
        return

    if output_path.exists():
        print("Archivo de medidas horarias ya existe. No se procesará nuevamente.")
        return

    if not input_path.exists():
        print("No se encontró el archivo original.")
        return

    print("Cargando datos para agregación horaria...")
    # Leemos solo lo necesario para ahorrar RAM
    df = pd.read_parquet(input_path)

    # 1. Calcular la Hora a partir del Cuarto de Hora (1-96)
    # La lógica es: Quarters 1,2,3,4 -> Hora 1 | Quarters 5,6,7,8 -> Hora 2...
    # Matemáticamente: Hora = ceil(Cuarto / 4)
    df['hora'] = ((df['cuarto de hora'] - 1) // 4) + 1

    # 2. Definir columnas de agrupación (las que identifican el punto de medida)
    group_cols = ['nombre_barra', 'rut', 'clave', 'tension', 'tipo', 'hora']
    
    # Nos aseguramos de agrupar solo por las que realmente existan en tu Parquet
    available_groups = [c for c in group_cols if c in df.columns]

    print(f"Agrupando {len(df):,} filas...")

    # 3. Agrupar y Sumar la medida_3
    # Usamos .reset_index() para que vuelva a ser un DataFrame plano
    df_hourly = df.groupby(available_groups)['medida_3'].sum().reset_index()

    # 5. Guardar el nuevo Parquet
    df_hourly.to_parquet(output_path, compression='snappy')
    
    print(f"--- Proceso Finalizado ---")
    print(f"Filas originales: {len(df):,}")
    print(f"Filas horarias: {len(df_hourly):,}")
    print(f"Archivo guardado en: {output_path.name}")

def quarter2hour_all():
    # Buscamos todos los archivos Parquet en la carpeta de medidas
    avaible_yymmm = get_avaible_yymmm()
    for yymm in avaible_yymmm:
        parquet_files = list(get_data_processed_path().glob(f"{yymm}/Medidas15/*.parquet"))
        for parquet in parquet_files:
            print(f"\nProcesando {parquet.name}...")
            if "_hourly" in parquet.stem:
                print("Archivo ya es horario. Saltando...")
                continue
            quarter2hour(parquet)

if __name__ == "__main__":
    quarter2hour_all()