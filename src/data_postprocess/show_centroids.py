from src.utils import *
from create_outputs import create_folders


def show_centroids(n_period):
    # 1. Asegurarnos de que la estructura de carpetas de salida exista
    create_folders(n_period)

    period_folder = get_data_processed_path() / f"Period{n_period}"
    centroids_path = period_folder / f"period{n_period}_centroids.parquet"
    
    if not centroids_path.exists():
        print(f"❌ Error: No se encontró el archivo de centroides en {centroids_path}")
        return
        
    df_centroides = pd.read_parquet(centroids_path)
    
    # Configurar el estilo de los gráficos
    sns.set_theme(style="whitegrid")

    for zona in df_centroides['zona'].unique():
        file_name = f"centroides_{zona}.png" 
        zona_folder = get_output_path() / f"Period{n_period}" / str(zona)
        save_path = zona_folder / file_name

        if save_path.exists():
            print(f"El gráfico para Zona {zona} ya existe. Saltando...")
            continue
        print(f"\n🔹 Generando gráfico para Zona: {zona}")
        
        df_zona = df_centroides[df_centroides['zona'] == zona].copy()
        
        # Asegurarse de que los datos estén ordenados para que la línea se dibuje bien
        df_zona = df_zona.sort_values(by=['id_cluster', 'hora'])

        # 2. Crear el gráfico de líneas
        plt.figure(figsize=(10, 6))
        sns.lineplot(
            data=df_zona,
            x='hora',
            y='medida',
            hue='id_cluster',
            palette='tab10', # Usa colores bien diferenciados
            linewidth=2.5,
            marker='o'       # Pone un puntito en cada hora
        )

        # 3. Darle formato
        plt.title(f"Centroides de Consumo - Zona: {zona} (Periodo {n_period})", fontsize=14, pad=15)
        plt.xlabel("Hora del Día", fontsize=11)
        plt.ylabel("Consumo Promedio", fontsize=11)
        plt.xticks(range(1, 25)) 
        plt.ylim(0,0.15)
        plt.legend(title="ID Cluster", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout() # Ajusta los márgenes para que no se corte la leyenda

        # 4. Guardar en la carpeta específica de la zona
      
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close() # Cierra el gráfico para liberar memoria
        
        print(f"Guardado en: {save_path}")

if __name__ == "__main__":
    show_centroids(0)
