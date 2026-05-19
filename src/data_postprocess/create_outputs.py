from src.utils import *

def create_folders(n_period):
    period_data_path = get_data_processed_path() / f"Period{n_period}" / f"period{n_period}_centroids.parquet"
    df_data = pd.read_parquet(period_data_path)
    zonas = df_data['zona'].unique()
    for zona in zonas:
        zona_folder = get_output_path() / f"Period{n_period}" / zona
        if not zona_folder.exists():
            zona_folder.mkdir(parents=True, exist_ok=True)
            cluster_of_zone = df_data[df_data['zona'] == zona]['id_cluster'].unique()
            for cluster in cluster_of_zone:
                cluster_folder = zona_folder / f"Cluster{cluster}"
                if not cluster_folder.exists():
                    cluster_folder.mkdir(parents=True, exist_ok=True)
