from pathlib import Path
import pandas as pd

def get_project_root() -> Path:
    """
    Sube en el árbol de carpetas hasta encontrar la raíz del proyecto.
    Funciona buscando la carpeta 'data' o el archivo '.git'.
    """
    # Empezamos en la ubicación de este script
    current_path = Path(__file__).resolve()
    
    # Subimos por el árbol (parents) hasta encontrar la carpeta raíz
    for parent in [current_path] + list(current_path.parents):
        # Opción A: Buscar por el nombre de la carpeta raíz
        if parent.name == "ProyectoAnalisisElectrico":
            return parent
            
    # Fallback: Si no encuentra nada, sube 2 niveles (src/data_preprocess -> Root)
    return current_path.parents[2]

def get_data_processed_path():
    root = get_project_root()
    data_processed = root / "data" / "processed"
    return data_processed

def get_data_raw_path():
    root = get_project_root()
    data_raw = root / "data" / "raw"
    return data_raw

def get_yymm_procesed_path(yymm, carpet):
    data_processed = get_data_processed_path()
    return data_processed / yymm / carpet

def get_avaible_yymmm():
    # Listamos las carpetas dentro de data/processed
    data_processed_path = get_data_processed_path()
    yymm_folders = [f.name for f in data_processed_path.iterdir() if f.is_dir()]
    return yymm_folders



