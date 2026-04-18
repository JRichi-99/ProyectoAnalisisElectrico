from src.utils import *

def check_rut(input_path: Path):
    # 1. Leemos solo la columna necesaria para no saturar la RAM
    df = pd.read_parquet(input_path, columns=['rut'])
    
    # 2. Extraemos el cuerpo numérico y lo convertimos a número
    # .str.split('-').str[0] toma el "10234567" del "10234567-8"
    cuerpo_rut = pd.to_numeric(df['rut'].str.split('-').str[0], errors='coerce')

    # 3. Filtramos: mayores a 0 (para ignorar los NAC_N de pérdida) y menores a 50M
    # Usamos .any() para que devuelva True si encuentra AL MENOS UNO
    condicion_bajos = (cuerpo_rut > 0) & (cuerpo_rut < 50000000)
    
    if condicion_bajos.any():
        total_error = condicion_bajos.sum()
        print(f"Alerta: Se encontraron {total_error:,} filas con RUT menor a 50 millones.")
        
        # Opcional: ver qué RUTs son para cachar si es basura o personas
        print("Ejemplos encontrados:", df.loc[condicion_bajos, 'rut'].unique()[:5])
        return False
    
    print(f"ntegridad OK: Todos los RUTs en {input_path.name} son > 50M o registros técnicos (0).")
    return True

if __name__ == "__main__":
    # Ejemplo de uso: revisar un archivo específico
    check_rut(get_data_processed_path() / "2601" / "Promedio" / "2601_mean.parquet")