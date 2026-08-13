import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils import *
from create_outputs import create_folders

def show_clusters_insight(n_period):
    create_folders(n_period)

    period_folder = get_data_processed_path() / f"Period{n_period}"
    centroids_path = period_folder / f"period{n_period}_centroids.parquet"
    data_path = period_folder / f"period{n_period}_clustered.parquet"

    if not centroids_path.exists() or not data_path.exists():
        print(f"❌ Error: Faltan archivos en {period_folder}")
        return
    
    df_centroides = pd.read_parquet(centroids_path)
    df_clusterizado = pd.read_parquet(data_path)

    sns.set_theme(style="whitegrid")

    for zona in df_centroides['zona'].unique():
        print(f"\n🔹 Generando reportes para Zona: {zona}")
        df_zona_centroides = df_centroides[df_centroides['zona'] == zona]
        df_zona_clusterizado = df_clusterizado[df_clusterizado['zona'] == zona]

        zona_folder = get_output_path() / f"Period{n_period}" / str(zona)
        zona_folder.mkdir(parents=True, exist_ok=True)

        # --- 1. LISTA PARA GUARDAR LOS DATOS DEL CSV TABULAR ---
        datos_reporte_zona = []

        for cluster_id in df_zona_centroides['id_cluster'].unique():
            df_cluster = df_zona_clusterizado[df_zona_clusterizado['id_cluster'] == cluster_id]
            df_unique_clients_cluster = df_cluster.drop_duplicates(subset=['clave']).copy()
            
            n_clientes = len(df_unique_clients_cluster)
            
            if n_clientes > 0:
                datos_energia = df_unique_clients_cluster['energia_total_diaria']
                
                # A. CALCULAR DECILES Y PASAR A POSITIVO (Inyección)
                # quantile(0) sacará el número más negativo. Al hacer .abs(), P0 será el valor más alto positivo.
                percentiles = np.linspace(0, 1, 11)
                valores = datos_energia.quantile(percentiles).abs().values 
                
                mean_val = abs(datos_energia.mean())
                std_val = datos_energia.std() # La desviación estándar ya es positiva
                
                # B. ESTRUCTURAR LA FILA EXACTAMENTE COMO PEDISTE
                fila = {
                    'zona': zona,
                    'id_cluster': cluster_id,
                    'P0': valores[0],
                    'P10': valores[1],
                    'P20': valores[2],
                    'P30': valores[3],
                    'P40': valores[4],
                    'Median': valores[5],
                    'P60': valores[6],
                    'P70': valores[7],
                    'P80': valores[8], # Añadido por continuidad
                    'P90': valores[9],
                    'P100': valores[10],
                    'mean': mean_val,
                    'std': std_val,
                    'n_cliente': n_clientes
                }
                datos_reporte_zona.append(fila)

            # --- 2. GRÁFICO DE TORTA (Se mantiene dentro de su carpeta) ---
            cluster_folder = zona_folder / f"Cluster{cluster_id}"
            cluster_folder.mkdir(parents=True, exist_ok=True)
            
            tension_counts = df_unique_clients_cluster['tension'].value_counts()
            if not tension_counts.empty:
                plt.figure(figsize=(10, 7))
                legend_labels = [f"{t} (n={c})" for t, c in zip(tension_counts.index, tension_counts.values)]
                wedges, _, autotexts = plt.pie(
                    tension_counts, labels=None, autopct='%1.1f%%', 
                    startangle=140, colors=sns.color_palette("viridis", len(tension_counts)),
                    explode=[0.03] * len(tension_counts), pctdistance=0.85
                )
                plt.setp(autotexts, size=9, weight="bold", color="white")
                plt.legend(wedges, legend_labels, title="Niveles de Tensión", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
                plt.title(f"Composición de Tensiones - Zona {zona}\nCluster {cluster_id}", fontsize=12, pad=10)
                plt.savefig(cluster_folder / "torta_tensiones.png", dpi=300, bbox_inches='tight')
                plt.close()
                
        # --- 3. CREAR Y GUARDAR EL CSV DE LA ZONA ---
        if datos_reporte_zona:
            df_reporte_final = pd.DataFrame(datos_reporte_zona)
            csv_path = zona_folder / f"stadistics_clusters_{zona.lower()}.csv"
            df_reporte_final.to_csv(csv_path, index=False)
            print(f"   ✅ Reporte tabular generado: {csv_path.name}")

if __name__ == "__main__":
    show_clusters_insight(0)