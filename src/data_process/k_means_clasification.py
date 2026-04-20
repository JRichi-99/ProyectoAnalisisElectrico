from src.utils import *
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from kneed import KneeLocator

def find_optimal_k(df_scaled, k_max=10):
    inertias = []
    k_range = range(2, k_max + 1)
    
    print(f"Buscando codo (Knee) en rango 2-{k_max}...")
    
    # 1. Calcular la inercia para cada K
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto').fit(df_scaled)
        inertias.append(kmeans.inertia_)

    # 2. Configurar KneeLocator para detectar el punto de inflexión
    # curve='convex': porque la inercia siempre baja formando un hueco (como un tazón)
    # direction='decreasing': porque la inercia baja a medida que K sube
    kn = KneeLocator(
        k_range, 
        inertias, 
        curve='convex', 
        direction='decreasing',
        interp_method='interp1d' # Método de interpolación para mayor precisión
    )

    k_optimo = kn.knee

    # Fallback por si no detecta un codo claro (curva muy plana o muy lineal)
    if k_optimo is None:
        print("⚠️ No se detectó un codo claro. Usando K=3 por defecto.")
        k_optimo = 3
    else:
        print(f"✅ Codo matemático detectado en K={k_optimo}")

    return k_optimo

def cluster_por_zona(n_period):
    period_folder = get_data_processed_path() / f"Period{n_period}"
    data_path = period_folder / f"period{n_period}_with_zones.parquet"
    
    if not data_path.exists():
        print(f"❌ Error: No se encontró el archivo en {data_path}")
        return None, None
        
    df = pd.read_parquet(data_path)

    lista_centroides = []
    lista_datos_clusterizados = []
    conteo_zonas = []
    zonas = df['zona'].unique()
    
    for zona in zonas:
        df_zona = df[df['zona'] == zona].copy()
        
        # Conteo de datos
        n_claves = df_zona['clave'].nunique()
        n_filas = len(df_zona)
        
        print(f"\n📍 Zona: {zona}")
        print(f"   └─ Claves únicas (clientes): {n_claves:,}")
        print(f"   └─ Total de registros: {n_filas:,}")
        
        # Guardamos estadísticas de la zona
        conteo_zonas.append({
            'zona': zona,
            'n_clientes': n_claves,
            'n_registros': n_filas
        })
        
        # 1. Pivotar a formato ancho
        df_wide = df_zona.pivot_table(index='clave', columns='hora', values='medida_3_mean').fillna(0)
        
        if len(df_wide) < 3:
            print(f"⚠️ Zona {zona} ignorada: insuficientes datos para clusterizar.")
            continue

        # 2. Estandarizar
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df_wide)
        
        # 3. K-Means
        k_maximo_zona = min(10, len(df_wide) - 1) 
        k_optimo = find_optimal_k(df_scaled, k_max=k_maximo_zona)
        kmeans = KMeans(n_clusters=k_optimo, random_state=42, n_init='auto').fit(df_scaled)
        
        # ===================================================================
        # RESULTADO 1: CENTROIDES VÍA INVERSE_TRANSFORM
        # ===================================================================
        # Deshacemos el escalado directamente desde los centros matemáticos
        centroides_reales = scaler.inverse_transform(kmeans.cluster_centers_)
        
        # Creamos un DataFrame ancho con esos valores
        df_centroides_wide = pd.DataFrame(centroides_reales, columns=df_wide.columns)
        df_centroides_wide['id_cluster'] = range(k_optimo)
        df_centroides_wide['zona'] = zona
        
        # Lo pasamos a formato largo con .melt()
        df_resultado_1 = df_centroides_wide.melt(
            id_vars=['zona', 'id_cluster'], 
            var_name='hora', 
            value_name='medida'
        )
        
        # ===================================================================
        # RESULTADO 2: DATOS DE ENTRADA ETIQUETADOS
        # ===================================================================
        # Mapeamos rápido los clusters a los datos originales
        df_zona['id_cluster'] = df_zona['clave'].map(dict(zip(df_wide.index, kmeans.labels_)))
        df_resultado_2 = df_zona

        # Guardamos en las listas
        lista_centroides.append(df_resultado_1)
        lista_datos_clusterizados.append(df_resultado_2)

    # 4. CONSOLIDAR Y GUARDAR
    if lista_centroides:
        df_centroides_final = pd.concat(lista_centroides, ignore_index=True)
        df_clusterizado_final = pd.concat(lista_datos_clusterizados, ignore_index=True)
        
        centroides_path = period_folder / f"period{n_period}_centroides.parquet"
        clustered_data_path = period_folder / f"period{n_period}_clustered.parquet"
        if centroides_path.exists() or clustered_data_path.exists():
            print(f"⚠️ Advertencia: Se sobrescribirán los archivos existentes en {period_folder.name}")
            return
        
        df_centroides_final.to_parquet(centroides_path, compression='snappy')
        df_clusterizado_final.to_parquet(clustered_data_path, compression='snappy')
        
        print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
        print(f"📊 1. Centroides limpios guardados en: {centroides_path.name}")
        print(f"🏷️ 2. Datos originales etiquetados guardados en: {clustered_data_path.name}")
    else:
        print("\n❌ Error: No se generaron clusters válidos.")

if __name__ == "__main__":
    cluster_por_zona(0)