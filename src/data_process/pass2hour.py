import pandas as pd
from pathlib import Path
from src.utils import *


def quarter2hour(input_path : Path):

    create_output_folder = input_path.parent.parent / "Medidas60"
    if not create_output_folder.exists():
        create_output_folder.mkdir(parents=True, exist_ok=True)
    output_path = input_path.parent.parent / "Medidas60" / f"{input_path.stem}_hourly{input_path.suffix}"

    if output_path.exists():
        print(f"Archivo {output_path.name} ya existe. Saltando...")
        return

    if not input_path.exists():
        print("No se encontró el archivo original.")
        return

    print(f"Cargando {input_path.name} para validación y agregación...")
    df = pd.read_parquet(input_path)

    rut_limpio = df['rut'].astype(str).str.split('-').str[0].str.replace('.', '', regex=False)
    
    # Convertimos a número (los valores nulos o raros se volverán NaN gracias a coerce)
    rut_numerico = pd.to_numeric(rut_limpio, errors='coerce')

    # Identificamos las claves que tienen un RUT menor a 50 millones
    claves_rut_invalido = df[rut_numerico < 50000000]['clave'].unique()
    
    if len(claves_rut_invalido) > 0:
        print(f"⚠️ FILTRO RUT: Se eliminan {len(claves_rut_invalido)} claves por tener RUT menor a 50.000.000.")
        # Filtramos el DataFrame para excluir esas claves
        df = df[~df['clave'].isin(claves_rut_invalido)]
        
    if df.empty:
        print("❌ Error: No quedaron datos tras el filtro de RUT.")
        return

    # 1. Parámetros de tiempo
    yymm = input_path.parent.parent.name  
    mm = yymm[-2:]  
    day_count = days_map.get(mm, 30)  
    
    # El target es: 24 horas * N días * 4 intervalos de 15 min
    expected_rows_per_clave = 24 * day_count * 4 

    # 2. Filtrado de Integridad (Sacar claves incompletas)
    counts = df.groupby('clave').size()
    claves_validas = counts[counts == expected_rows_per_clave].index
    claves_invalidas = counts[counts != expected_rows_per_clave].index

    if not claves_invalidas.empty:
        print(f"⚠️ Calidad: Se eliminaron {len(claves_invalidas)} claves por datos incompletos.")
        # Opcional: print(f"Claves fuera: {list(claves_invalidas)}")
        df = df[df['clave'].isin(claves_validas)]
    else:
        print(f"✅ Todas las claves ({len(claves_validas)}) están completas.")

    if df.empty:
        print("❌ Error: No quedaron datos válidos tras el filtro.")
        return

    # 3. Preparar Agregación Horaria
    # Calculamos la hora (1-24) a partir del cuarto de hora (1-96)
    df['hora'] = ((df['cuarto de hora'] - 1) // 4) + 1

    # 2. Definir columnas de agrupación
    group_cols = ['rut', 'clave', 'tension', 'hora']
    
    # Nos aseguramos de agrupar solo por las que realmente existan en tu Parquet
    available_groups = [c for c in group_cols if c in df.columns]

    print(f"Agrupando {len(df):,} filas...")

    # 3. Agrupar y Sumar la medida_3
    df_hourly = df.groupby(available_groups)['medida_3'].sum().reset_index()

    # 1. Definir el objetivo de filas horarias (ej: 24 horas * 31 días = 744)
    expected_hourly_rows = 24 * day_count
    
    # 2. Contar cuántas horas tiene cada clave en el DF ya agrupado
    # Si una clave cambió de RUT o Tensión, tendrá menos de las filas esperadas
    hourly_counts = df_hourly.groupby('clave').size()
    
    # 3. Identificar las claves que "se rompieron" por inconsistencia de metadatos
    claves_inconsistentes = hourly_counts[hourly_counts != expected_hourly_rows].index
    
    if not claves_inconsistentes.empty:
        print(f"⚠️ FILTRO DE METADATOS: Se eliminan {len(claves_inconsistentes)} claves.")
        print(f"Motivo: Cambio de RUT o Tensión detectado durante el mes.")
        
        # Opcional: Mostrar un ejemplo del error
        for c in claves_inconsistentes[:3]: # Solo los primeros 3 para no saturar
            found = hourly_counts[c]
            print(f"   - Clave: {c} | Filas: {found} vs Esperadas: {expected_hourly_rows}")
            
        # 4. Limpiar el DataFrame horaria
        df_hourly = df_hourly[~df_hourly['clave'].isin(claves_inconsistentes)]
    else:
        print(f"✅ Consistencia total: Todas las claves mantienen su RUT y Tensión los {day_count} días.")

    # 5. Guardar el nuevo Parquet
    df_hourly.to_parquet(output_path, compression='snappy')
    
    print(f"--- Proceso Finalizado ---")
    print(f"Filas originales: {len(df):,}")
    print(f"Filas horarias (resultantes): {len(df_hourly):,}")
    
    # Nota: Si tu agrupación no incluye fecha, expected_rows no coincidirá con tu resultado.
    expected_rows = len(df['clave'].unique()) * 24 * day_count
    
    print(f"Filas esperadas: {expected_rows:,}")
    print(f"Archivo guardado en: {output_path.name}")

def quarter2hour_all():
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