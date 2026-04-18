import pandas as pd
from utils import get_project_root

def find_common_keys():
    root = get_project_root()
    path_norte = root / "data/processed/2601/Medidas/sur.parquet"
    path_dist = root / "data/processed/2601/Medidas/sur_dist.parquet"

    # 1. Leer SOLO la columna 'clave' de cada archivo
    # Esto ahorra muchísima RAM
    claves_norte = pd.read_parquet(path_norte, columns=['clave'])['clave'].unique()
    claves_dist = pd.read_parquet(path_dist, columns=['clave'])['clave'].unique()

    # 2. Convertir a sets y encontrar la intersección
    set_norte = set(claves_norte)
    set_dist = set(claves_dist)
    
    repetidos = set_norte.intersection(set_dist)

    # 3. Mostrar resultados
    print(f"Claves en Norte: {len(set_norte)}")
    print(f"Claves en Norte_Dist: {len(set_dist)}")
    print(f"Claves que aparecen en AMBOS: {len(repetidos)}")
    
    if len(repetidos) > 0:
        print("\nPrimeras 5 claves repetidas:")
        print(list(repetidos)[:5])

if __name__ == "__main__":
    find_common_keys()