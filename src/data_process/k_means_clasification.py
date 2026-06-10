"""Cluster electrical load profiles by zone using DTW K-medoids.

Replacement for the previous Euclidean KMeans workflow. It keeps the same input
and output files, but the objective is now the sum of DTW distances to cluster
medoids rather than Euclidean inertia to arithmetic centroids.
"""
from __future__ import annotations

import pandas as pd
from kneed import KneeLocator

from src.data_process.k_medoids_dtw import KMedoidsDTW


def find_optimal_k_dtw(
    X,
    *,
    k_max: int = 10,
    window: int | None = 3,
    normalize: bool = False,
    n_init: int = 3,
    max_iter: int = 20,
    random_state: int = 42,
) -> int:
    """Find a candidate K using the elbow method with DTW inertia."""
    inertias: list[float] = []
    k_range = range(2, k_max + 1)

    print(f"Buscando codo DTW en rango 2-{k_max}...")
    for k in k_range:
        model = KMedoidsDTW(
            n_clusters=k,
            window=window,
            normalize=normalize,
            n_init=n_init,
            max_iter=max_iter,
            random_state=random_state,
        ).fit(X)
        if model.inertia_ is None:
            raise RuntimeError("KMedoidsDTW no calculó inertia_.")
        inertias.append(model.inertia_)

    kn = KneeLocator(
        list(k_range),
        inertias,
        curve="convex",
        direction="decreasing",
        interp_method="interp1d",
    )

    k_optimo = kn.knee
    if k_optimo is None:
        print("⚠️ No se detectó un codo claro con DTW. Usando K=3 por defecto.")
        return min(3, k_max)

    print(f"✅ Codo DTW detectado en K={k_optimo}")
    return int(k_optimo)


import pandas as pd
# Asegúrate de tener importado find_optimal_k_dtw y KMedoidsDTW aquí también

def cluster_por_macrozona(
    df: pd.DataFrame,
    *,
    value_col: str = "medida",  # Ajustado al nombre de tu columna de valores
    window: int | None = 3,
    normalize: bool = False,
    k_max_global: int = 10,
    n_init: int = 5,
    max_iter: int = 30,
    random_state: int = 42,
):
    """Cluster clients by macrozona using DTW over their hourly profiles."""
    
    # 1. Validación estricta con TUS nombres de columnas
    required_cols = {"macrozona", "clave", "Hora", value_col}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"❌ Error: Faltan columnas obligatorias en el DataFrame: {sorted(missing)}")

    lista_centroides = []
    lista_datos_clusterizados = []

    # 2. Iteración sobre cada macrozona única
    for mz in df["macrozona"].dropna().unique():
        df_zona = df[df["macrozona"] == mz].copy()
        n_claves = df_zona["clave"].nunique()
        n_filas = len(df_zona)

        print(f"\nMacrozona: {mz}")
        print(f" └─ Claves únicas (clientes/barras): {n_claves:,}")
        print(f" └─ Total de registros: {n_filas:,}")

        # 3. Pivotar usando "Hora" (mayúscula) y "clave"
        df_wide = (
            df_zona.pivot_table(index="clave", columns="Hora", values=value_col, aggfunc="mean")
            .sort_index(axis=1)
            .fillna(0.0)
        )

        if len(df_wide) < 3:
            print(f"⚠️ Macrozona {mz} ignorada: insuficientes datos para clusterizar.")
            continue

        # 4. Preparar matriz para el modelo
        X = df_wide.to_numpy(dtype=float)
        k_maximo_zona = min(k_max_global, len(df_wide) - 1)
        
        # 5. Encontrar el K óptimo
        k_optimo = find_optimal_k_dtw(
            X,
            k_max=k_maximo_zona,
            window=window,
            normalize=normalize,
            n_init=max(1, min(n_init, 3)), 
            max_iter=max_iter,
            random_state=random_state,
        )

        # 6. Ajustar el modelo final
        model = KMedoidsDTW(
            n_clusters=k_optimo,
            window=window,
            normalize=normalize,
            n_init=n_init,
            max_iter=max_iter,
            random_state=random_state,
            verbose=False,
        ).fit(X)

        if model.labels_ is None or model.cluster_centers_ is None or model.medoid_indices_ is None:
            raise RuntimeError(f"El modelo DTW no generó labels/medoids para la macrozona {mz}.")

        # 7. Resultado 1: Centros interpretables (Medoids)
        df_centroides_wide = pd.DataFrame(model.cluster_centers_, columns=df_wide.columns)
        df_centroides_wide["id_cluster"] = range(k_optimo)
        df_centroides_wide["macrozona"] = mz  # Actualizado
        df_centroides_wide["clave_medoid"] = df_wide.index[model.medoid_indices_].to_numpy()
        df_centroides_wide["dtw_inertia"] = model.inertia_

        # Volver al formato "long" usando los nombres correctos
        df_resultado_1 = df_centroides_wide.melt(
            id_vars=["macrozona", "id_cluster", "clave_medoid", "dtw_inertia"],
            var_name="Hora",
            value_name=value_col,
        )

        # 8. Resultado 2: Datos de entrada etiquetados
        df_zona["id_cluster"] = df_zona["clave"].map(dict(zip(df_wide.index, model.labels_)))
        df_zona["metodo_cluster"] = "kmedoids_dtw"
        df_zona["dtw_window"] = -1 if window is None else window
        df_zona["dtw_normalize"] = normalize

        lista_centroides.append(df_resultado_1)
        lista_datos_clusterizados.append(df_zona)

    # 9. Consolidar resultados finales
    if not lista_centroides:
        print("\n❌ Error: No se generaron clusters válidos.")
        return None, None

    df_centroides_final = pd.concat(lista_centroides, ignore_index=True)
    df_clusterizado_final = pd.concat(lista_datos_clusterizados, ignore_index=True)

    print("\n✅ PROCESO COMPLETADO EXITOSAMENTE CON DTW")
    
    return df_centroides_final, df_clusterizado_final


if __name__ == "__main__":
    cluster_por_macrozona(0, window=3, normalize=False, overwrite=False)
