import pandas as pd
from utils import get_project_root

def preview_dataset():
    root = get_project_root()
    # Apuntamos al archivo que acabas de procesar
    file_path = root / "data" / "processed" / "2601" / "Medidas" / "norte.parquet"

    if file_path.exists():
        # Carga instantánea gracias al formato binario
        df = pd.read_parquet(file_path)

        print("Primeras 5 filas del dataset:")
        print(df.head())
        
        print("\nInformación de tipos de datos y memoria:")
        print(df.info())

        # El 1 es para que solo muestre una fila, y .T para que sea vertical
        print(df.sample(1).T)
        
        print(f"\nTotal de registros: {len(df)}")
        """
        df_tipo = pd.read_parquet(file_path, columns=['tension'])

        print("--- Entradas únicas en la columna 'tipo' ---")
        print(df_tipo['tension'].unique())

        print("\n--- Cantidad de registros por tipo ---")
        print(df_tipo['tension'].value_counts())
        """
    else:
        print("El archivo Parquet no fue encontrado. Verifica la ruta.")

if __name__ == "__main__":
    preview_dataset()