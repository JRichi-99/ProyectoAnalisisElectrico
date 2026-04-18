import zipfile
from src.utils import get_project_root

def unzip_data_base():
    root = get_project_root()
    data_raw = root / "data" / "raw"

    # Buscamos todos los zips recursivamente
    zip_files = list(data_raw.rglob("*.zip"))

    for zip_path in zip_files:
        # 1. Filtro de ubicación: Solo lo que esté bajo "02 Medidas por tipo"
        if "02 Medidas por tipo" not in zip_path.parts:
            continue

        # 2. Filtro de exclusión: No procesar archivos que contengan "Compraventas"
        if "Compraventas" in zip_path.name:
            print(f"Excluido por regla: {zip_path.name}")
            continue

        # Lógica de chequeo de carpeta existente
        folder_check = zip_path.parent / zip_path.stem
        
        if folder_check.exists() and any(folder_check.iterdir()):
            print(f"Saltando: {zip_path.name} (Ya extraído)")
            continue
        
        # Extracción
        print(f"Extrayendo: {zip_path.name}...")
        try:
            folder_check.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(folder_check)
                print(f"Finalizado.")
        except Exception as e:
            print(f"Error en {zip_path.name}: {e}")

if __name__ == "__main__":
    unzip_data_base()