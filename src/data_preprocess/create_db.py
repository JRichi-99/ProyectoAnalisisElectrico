from csv2parquet import procesar_csv_a_parquet
from src.utils import *

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


def procesar_base_datos(db_path, data_processed_path, filtros_dict, cols_to_keep, dtypes_dict):
    codigo_fecha = db_path.name.split('_')[1]
    path_medidas_raw = db_path / "02 Medidas por tipo"
    output_folder = data_processed_path / codigo_fecha / "Medidas15"

    if not path_medidas_raw.exists() or not path_medidas_raw.is_dir():
        return

    for subzona in path_medidas_raw.iterdir():
        if not subzona.is_dir(): continue

        short_name = get_short_name(subzona.name)
        if not short_name: continue

        output_path = output_folder / f"{short_name}.parquet"
        if output_path.exists(): continue

        csv_file = next(subzona.glob("*.csv"), None)
        if csv_file:
            print(f"Procesando: {short_name}...")
            
            # Pasamos los diccionarios a la nueva función genérica
            exito = procesar_csv_a_parquet(
                csv_file_path=csv_file, 
                output_path=output_path, 
                cols_to_keep=cols_to_keep,
                filtros=filtros_dict,
                dtypes=dtypes_dict,
                sep=None # pd.read_csv intentará inferir el separador
            )
            
            if exito:
                print(f"Guardado optimizado y filtrado: {output_path.name}")


def save_Medidas():
    data_raw = get_data_raw_path()
    data_processed = get_data_processed_path()

    # 1. Definimos las columnas a conservar
    cols = ['nombre_barra', 'tension', 'clave', 'cuarto de hora', 'medida_3', 'rut', 'tipo']
    
    # 2. Diccionario de FILTROS (¡Puedes poner varias columnas aquí!)
    mis_filtros = {
        'tipo': ["L", "L_D"]
    } 

    # 3. Diccionario de TIPOS DE DATOS (Dtypes)
    mis_dtypes = {
        'RUT': str, 
        'clave': str, 
        'tipo': str
    }

    for db in data_raw.iterdir():
        if not (db.is_dir() and "Bases de Datos" in db.name):
            continue

        procesar_base_datos(db, data_processed, mis_filtros, cols, mis_dtypes)
        
        # OJO: Si quieres probar con una sola carpeta y que se detenga, descomenta el break de abajo.
        # En producción, bórralo para que procese todo.
        break 


if __name__ == "__main__":
    save_Medidas()