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

def cluster_por_macrozona(
    df: pd.DataFrame,
    *,
    value_col: str = "medida",  
    window: int | None = 3,
    normalize: bool = False,
    k_max_global: int = 10,
    n_init: int = 5,
    max_iter: int = 30,
    random_state: int = 42,
):
    """Cluster clients by macrozona using DTW over their hourly profiles."""
    
    # Asumiendo que la columna se llama "Zona" o "macrozona" para hacer la diferencia
    required_cols = {"macrozona", "clave", "Hora", value_col}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"❌ Error: Faltan columnas obligatorias: {sorted(missing)}")

    df = df.copy()
    df["clave_unica"] = df["clave"].astype(str) + "_" + df["Zona"].astype(str)

    lista_centroides = []
    lista_datos_clusterizados = []

    for mz in df["macrozona"].dropna().unique():
        df_zona = df[df["macrozona"] == mz].copy()
        
        # Ahora contamos claves únicas reales
        n_claves = df_zona["clave_unica"].nunique()
        n_filas = len(df_zona)

        print(f"\nMacrozona: {mz}")
        print(f" └─ Claves únicas (clientes/barras): {n_claves:,}")
        print(f" └─ Total de registros: {n_filas:,}")

        # PIVOTAR usando clave_unica
        df_wide = (
            df_zona.pivot_table(index="clave_unica", columns="Hora", values=value_col, aggfunc="mean")
            .sort_index(axis=1)
            .fillna(0.0)
        )

        if len(df_wide) < 3:
            print(f"⚠️ Macrozona {mz} ignorada: insuficientes datos para clusterizar.")
            continue

        X = df_wide.to_numpy(dtype=float)
        k_maximo_zona = min(k_max_global, len(df_wide) - 1)
        
        k_optimo = find_optimal_k_dtw(
            X,
            k_max=k_maximo_zona,
            window=window,
            normalize=normalize,
            n_init=max(1, min(n_init, 3)), 
            max_iter=max_iter,
            random_state=random_state,
        )

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
            raise RuntimeError(f"El modelo DTW no generó labels para {mz}.")

        # Centros
        df_centroides_wide = pd.DataFrame(model.cluster_centers_, columns=df_wide.columns)
        df_centroides_wide["id_cluster"] = range(k_optimo)
        df_centroides_wide["macrozona"] = mz 
        df_centroides_wide["clave_medoid"] = df_wide.index[model.medoid_indices_].to_numpy()
        df_centroides_wide["dtw_inertia"] = model.inertia_

        df_resultado_1 = df_centroides_wide.melt(
            id_vars=["macrozona", "id_cluster", "clave_medoid", "dtw_inertia"],
            var_name="Hora",
            value_name=value_col,
        )

        # Etiquetar usando clave_unica
        df_zona["id_cluster"] = df_zona["clave_unica"].map(dict(zip(df_wide.index, model.labels_)))
        df_zona["metodo_cluster"] = "kmedoids_dtw"
        df_zona["dtw_window"] = -1 if window is None else window
        df_zona["dtw_normalize"] = normalize

        lista_centroides.append(df_resultado_1)
        lista_datos_clusterizados.append(df_zona)

    if not lista_centroides:
        print("\nError: No se generaron clusters válidos.")
        return None, None

    df_centroides_final = pd.concat(lista_centroides, ignore_index=True)
    df_clusterizado_final = pd.concat(lista_datos_clusterizados, ignore_index=True)

    print("\nPROCESO COMPLETADO EXITOSAMENTE CON DTW")
    return df_centroides_final, df_clusterizado_final


def cluster_por_macrozonaV2(
    df: pd.DataFrame,
    *,
    value_col: str = "medida_mean",  # Actualizado al nuevo nombre por defecto
    window: int | None = 3,
    normalize: bool = False,
    k_max_global: int = 10,
    n_init: int = 5,
    max_iter: int = 30,
    random_state: int = 42,
):
    """Cluster clients by macrozona using DTW over their hourly profiles."""
    
    # 1. Validamos las nuevas columnas obligatorias de la arquitectura
    required_cols = {"macrozona", "clave", "RUT_CLIENTE", "REGION_CLIENTE", "Hora", "Zona", value_col}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"❌ Error: Faltan columnas obligatorias: {sorted(missing)}")

    df = df.copy()
    
    # 2. LA NUEVA LLAVE MAESTRA: Fierro + Dueño + Ubicación + Zona
    df["clave_unica"] = (
        df["clave"].astype(str) + "_" + 
        df["RUT_CLIENTE"].astype(str) + "_" + 
        df["REGION_CLIENTE"].astype(str) + "_"+
        df["Zona"].astype(str)
    )

    lista_centroides = []
    lista_datos_clusterizados = []

    for mz in df["macrozona"].dropna().unique():
        df_zona = df[df["macrozona"] == mz].copy()
        
        # Conteo exacto de prospectos únicos
        n_claves = df_zona["clave_unica"].nunique()
        n_filas = len(df_zona)

        print(f"\nMacrozona: {mz}")
        print(f" └─ Prospectos únicos (Clave+RUT+Región): {n_claves:,}")
        print(f" └─ Total de registros horarios: {n_filas:,}")

        # PIVOTAR usando la nueva clave maestra
        df_wide = (
            df_zona.pivot_table(index="clave_unica", columns="Hora", values=value_col, aggfunc="mean")
            .sort_index(axis=1)
            .fillna(0.0)
        )

        if len(df_wide) < 3:
            print(f"⚠️ Macrozona {mz} ignorada: insuficientes datos para clusterizar.")
            continue

        X = df_wide.to_numpy(dtype=float)
        k_maximo_zona = min(k_max_global, len(df_wide) - 1)
        
        k_optimo = find_optimal_k_dtw(
            X,
            k_max=k_maximo_zona,
            window=window,
            normalize=normalize,
            n_init=max(1, min(n_init, 3)), 
            max_iter=max_iter,
            random_state=random_state,
        )

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
            raise RuntimeError(f"El modelo DTW no generó labels para {mz}.")

        # Centros (Ahora el medoid representará la clave_unica completa)
        df_centroides_wide = pd.DataFrame(model.cluster_centers_, columns=df_wide.columns)
        df_centroides_wide["id_cluster"] = range(k_optimo)
        df_centroides_wide["macrozona"] = mz 
        df_centroides_wide["clave_medoid"] = df_wide.index[model.medoid_indices_].to_numpy()
        df_centroides_wide["dtw_inertia"] = model.inertia_

        df_resultado_1 = df_centroides_wide.melt(
            id_vars=["macrozona", "id_cluster", "clave_medoid", "dtw_inertia"],
            var_name="Hora",
            value_name=value_col,
        )

        # Etiquetar la base usando la nueva clave maestra
        df_zona["id_cluster"] = df_zona["clave_unica"].map(dict(zip(df_wide.index, model.labels_)))
        df_zona["metodo_cluster"] = "kmedoids_dtw"
        df_zona["dtw_window"] = -1 if window is None else window
        df_zona["dtw_normalize"] = normalize

        lista_centroides.append(df_resultado_1)
        lista_datos_clusterizados.append(df_zona)

    if not lista_centroides:
        print("\nError: No se generaron clusters válidos.")
        return None, None

    df_centroides_final = pd.concat(lista_centroides, ignore_index=True)
    df_clusterizado_final = pd.concat(lista_datos_clusterizados, ignore_index=True)

    print("\nPROCESO COMPLETADO EXITOSAMENTE CON DTW")
    return df_centroides_final, df_clusterizado_final

def subcluster_macrozona_cluster(
    df: pd.DataFrame,
    macrozona_target: str,
    cluster_target: int,
    *,
    value_col: str = "medida",
    window: int | None = 3,
    normalize: bool = False,
    k_max_global: int = 8,  
    n_init: int = 5,
    max_iter: int = 30,
    random_state: int = 42,
):
    """
    Realiza una subclusterizacion DTW sobre un cluster especifico de una macrozona.
    Requiere que el DataFrame de entrada ya tenga las columnas 'macrozona' e 'id_cluster'.
    """
    required_cols = {"macrozona", "clave", "Hora", value_col, "id_cluster"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Error: Faltan columnas en el DataFrame: {sorted(missing)}")

    # 1. Filtrar los datos objetivo
    mask = (df["macrozona"] == macrozona_target) & (df["id_cluster"] == cluster_target)
    df_sub = df[mask].copy()

    if df_sub.empty:
        print(f"Atencion: No se encontraron datos para la macrozona '{macrozona_target}' y cluster {cluster_target}.")
        return None, None

    # Crear clave unica para evitar colisiones de dimensiones por duplicados de nombre
    df_sub["clave_unica"] = df_sub["clave"].astype(str) + "_" + df_sub["Zona"].astype(str)

    n_claves = df_sub["clave_unica"].nunique()
    n_filas = len(df_sub)

    print(f"\n--- SUBCLUSTERIZANDO ---")
    print(f"Macrozona: {macrozona_target} | Cluster Padre: {cluster_target}")
    print(f" |- Claves unicas a procesar: {n_claves:,}")
    print(f" |- Total de registros: {n_filas:,}")

    if n_claves < 3:
        print("Atencion: Insuficientes clientes para subclusterizar (se requieren al menos 3).")
        return None, None

    # 2. Pivotar formato Wide usando clave_unica
    df_wide = (
        df_sub.pivot_table(index="clave_unica", columns="Hora", values=value_col, aggfunc="mean")
        .sort_index(axis=1)
        .fillna(0.0)
    )

    # 3. Preparar matriz y buscar K optimo
    X = df_wide.to_numpy(dtype=float)
    k_maximo_zona = min(k_max_global, len(df_wide) - 1)

    k_optimo = find_optimal_k_dtw(
        X,
        k_max=k_maximo_zona,
        window=window,
        normalize=normalize,
        n_init=max(1, min(n_init, 3)),
        max_iter=max_iter,
        random_state=random_state,
    )

    # 4. Ajustar modelo final
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
        raise RuntimeError("El modelo DTW no genero resultados para la subclusterizacion.")

    # 5. Resultado 1: Nuevos Centroides (Medoids)
    df_centroides_wide = pd.DataFrame(model.cluster_centers_, columns=df_wide.columns)
    df_centroides_wide["id_subcluster"] = range(k_optimo)
    df_centroides_wide["id_cluster_padre"] = cluster_target
    df_centroides_wide["macrozona"] = macrozona_target
    df_centroides_wide["clave_medoid"] = df_wide.index[model.medoid_indices_].to_numpy()
    df_centroides_wide["dtw_inertia"] = model.inertia_

    # Volver a formato "long"
    df_centroides_final = df_centroides_wide.melt(
        id_vars=["macrozona", "id_cluster_padre", "id_subcluster", "clave_medoid", "dtw_inertia"],
        var_name="Hora",
        value_name=value_col,
    )

    # 6. Resultado 2: Datos mapeados usando la clave_unica
    map_subclusters = dict(zip(df_wide.index, model.labels_))
    df_sub["id_subcluster"] = df_sub["clave_unica"].map(map_subclusters)

    print("\nSUBCLUSTERIZACION COMPLETADA CON EXITO")
    
    return df_centroides_final, df_sub



if __name__ == "__main__":
    cluster_por_macrozona(0, window=3, normalize=False, overwrite=False)
