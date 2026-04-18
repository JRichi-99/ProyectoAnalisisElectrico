import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def procesar_csv_a_parquet(
    csv_file_path, 
    output_path, 
    cols_to_keep=None, 
    filtros=None, 
    dtypes=None, 
    chunksize=500000,
    sep=None,
    encoding='latin-1',
    decimal='.'
):
    """
    Lee un CSV por chunks, aplica filtros dinámicos, selecciona columnas y lo guarda como Parquet.
    
    Parámetros:
    * csv_file_path: Ruta al archivo CSV original.
    * output_path: Ruta donde se guardará el archivo Parquet.
    * cols_to_keep (list): Lista de columnas a mantener. Si es None, se mantienen todas.
    * filtros (dict): Diccionario de listas para filtrar. Ej: {'tipo': ['L', 'L_D'], 'estado': ['OK']}.
    * dtypes (dict): Diccionario con los tipos de datos para pandas. Ej: {'RUT': str}.
    * chunksize (int): Cantidad de filas a leer en memoria a la vez.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Estandarizamos los filtros y columnas requeridas a minúsculas 
    # para evitar errores de mayúsculas/minúsculas en diferentes CSVs
    if filtros:
        filtros = {k.lower(): v for k, v in filtros.items()}
    if cols_to_keep:
        cols_to_keep = [c.lower() for c in cols_to_keep]

    reader = pd.read_csv(
        csv_file_path, 
        sep=sep, engine='python', encoding=encoding, decimal=decimal,
        chunksize=chunksize,
        dtype=dtypes
    )

    writer = None

    for chunk in reader:
        # A. Normalizar nombres de columnas del chunk actual
        chunk.columns = [str(c).strip().lower() for c in chunk.columns]

        # B. FILTRADO DINÁMICO
        # Recorremos el diccionario de filtros y aplicamos cada uno
        if filtros:
            for col, valores_permitidos in filtros.items():
                if col in chunk.columns:
                    chunk = chunk[chunk[col].isin(valores_permitidos)]
        
        # Si tras aplicar todos los filtros el chunk queda vacío, pasamos al siguiente
        if chunk.empty:
            continue

        # C. SELECCIÓN DE COLUMNAS
        if cols_to_keep:
            available_cols = [c for c in cols_to_keep if c in chunk.columns]
            chunk = chunk[available_cols]

        # D. ESCRITURA INCREMENTAL
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')
        writer.write_table(table)

    if writer:
        writer.close()
        return True # Se guardó el archivo con éxito y tiene datos
    
    return False # El CSV estaba vacío o ninguna fila cumplió los filtros