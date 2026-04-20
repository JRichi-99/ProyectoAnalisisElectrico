import pandas as pd
from src.utils import get_project_root

def preview_dataset():
    root = get_project_root()
    # Apuntamos al archivo que acabas de procesar
    file_path = root / "data" / "processed" / "Period0" / "period0_with_zones.parquet"
    #file_path = root / "data" / "processed" / "2601" / "Promedio"/ "2601_mean.parquet"

    if file_path.exists():
        # Carga instantánea gracias al formato binario
        df = pd.read_parquet(file_path)

        print("Primeras 5 filas del dataset:")
        print(df.head())
        
        print("\nInformación de tipos de datos y memoria:")
        print(df.info())

        # El 1 es para que solo muestre una fila, y .T para que sea vertical
        print(df.sample(5).T)
        
        print(f"\nTotal de registros: {len(df)}")
    else:
        print("El archivo Parquet no fue encontrado. Verifica la ruta.")


if __name__ == "__main__":
    preview_dataset()