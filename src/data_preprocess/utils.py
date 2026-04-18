import zipfile
from pathlib import Path

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
