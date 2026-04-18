import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from utils import get_project_root

def save_Cmg():
    root = get_project_root()
    data_raw = root / "data" / "raw"
    data_processed = root / "data" / "processed"

    for db in data_raw.iterdir():
        if db.is_dir() and "Bases de Datos" in db.name:
            try:
                codigo_fecha = db.name.split('_')[1]
            except IndexError:
                print("Esperamos un formato de carpeta como 'Bases de Datos_YYMM_BD01'")
                break

            # 1. Definir rutas de origen y destino primero
            nombre_archivo = f"cmg{codigo_fecha}_15min_formateado.csv"
            csv_path = db / "01 Cmg" / nombre_archivo
            
            output_folder = data_processed / codigo_fecha / "Cmg"
            output_path = output_folder / f"cmg{codigo_fecha}.parquet"

            # 2. Corroborar si el archivo de salida ya existe
            if output_path.exists():
                print(f"Saltando: {output_path.name} ya existe.")
                continue 

            # 3. Solo si no existe, verificamos el origen y procesamos
            if csv_path.exists():
                print(f"Procesando: {nombre_archivo}")
                
                output_folder.mkdir(parents=True, exist_ok=True)

                df = pd.read_csv(
                    csv_path, 
                    sep=None, 
                    engine='python', 
                    encoding='latin-1', 
                    decimal='.'
                )
                
                df.columns = [c.strip().lower() for c in df.columns]
                
                if 'fecha' in df.columns:
                    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y%m%d')

                df.to_parquet(output_path, compression='snappy', index=False)
                print(f"Convertido: {output_path.name}")
            else:
                print(f"Archivo no encontrado: {nombre_archivo}")

def get_short_name(folder_name):
    """Mapea el nombre de la carpeta del CEN a tu nombre preferido."""
    name_low = folder_name.lower()
    if "compraventas" in name_low:
        return "compraventas"
    if "norte" in name_low:
        if "distribución" in name_low or "distribucion" in name_low:
            return "norte_dist"
        return "norte"
    if "sur" in name_low:
        if "distribución" in name_low or "distribucion" in name_low:
            return "sur_dist"
        return "sur"
    return None

def save_Medidas():
    root = get_project_root()
    data_raw = root / "data" / "raw"
    data_processed = root / "data" / "processed"

    for db in data_raw.iterdir():
        if not (db.is_dir() and "Bases de Datos" in db.name):
            continue

        try:
            codigo_fecha = db.name.split('_')[1]
        except IndexError:
            print(f"Formato incorrecto en carpeta: {db.name}")
            continue

        path_medidas_raw = db / "02 Medidas por tipo"
        if not path_medidas_raw.exists():
            continue

        output_folder = data_processed / codigo_fecha / "Medidas"
        
        for subzona in path_medidas_raw.iterdir():
            if not subzona.is_dir():
                continue

            short_name = get_short_name(subzona.name)
            if not short_name:
                continue

            output_path = output_folder / f"{short_name}.parquet"

            if output_path.exists():
                print(f"Saltando: {output_path.name} ya existe.")
                continue

            csv_file = next(subzona.glob("*.csv"), None)
            if csv_file:
                print(f"Procesando archivo masivo: {csv_file.name} -> {short_name}.parquet")
                output_folder.mkdir(parents=True, exist_ok=True)

                # 1. Crear el iterador de lectura
                reader = pd.read_csv(
                    csv_file, 
                    sep=None, 
                    engine='python', 
                    encoding='latin-1', 
                    decimal='.',
                    chunksize=500000
                )

                writer = None

                # 2. Procesar pedazo por pedazo (chunks)
                for chunk in reader:
                    # Limpieza de columnas en el pedazo actual
                    chunk.columns = [c.strip().lower() for c in chunk.columns]

                    # Corrección de fecha
                    if 'fecha' in chunk.columns:
                        chunk['fecha'] = pd.to_datetime(
                            chunk['fecha'].astype(str), 
                            format='%Y%m%d', 
                            errors='coerce'
                        )

                    # 3. Convertir pedazo a tabla de pyarrow para escritura incremental
                    table = pa.Table.from_pandas(chunk)
                    
                    if writer is None:
                        # Inicializar el archivo Parquet con el esquema del primer pedazo
                        writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')
                    
                    writer.write_table(table)

                # 4. Cerrar el archivo al terminar todos los pedazos
                if writer:
                    writer.close()
                
                print(f"Guardado exitoso: {output_path.name}")
        
        # El break se mantiene aquí para procesar solo una carpeta de base de datos durante el test
        break     
    

if __name__ == "__main__":
    save_Cmg()
    save_Medidas()
