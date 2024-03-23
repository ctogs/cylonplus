from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import time

FILES = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]
GROUP_IDS = ["state_id", "state_id", "state_id", "state_id", "0", "0", "0", "0", "0", "0", "0", "0"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        print("Time taken for groupby operation: {:.4f} seconds".format(self.elapsed_time))

if __name__ == "__main__":
    # Initialize a Dask LocalCluster to take full advantage of CPU cores
    cluster = LocalCluster(n_workers=4, threads_per_worker=4)  # Adjust based on system's capabilities
    client = Client(cluster)
    print(f"Dask Dashboard is available at: {client.dashboard_link}")

    for file_name, group_id in zip(FILES, GROUP_IDS):
        # Load CSV files into Dask dataframes
        ddf = dd.read_csv(f'../data/input/{file_name}.csv')
        # Show the first few rows (compute is necessary because Dask is lazy)
        print(ddf.head())

        with Stopwatch() as sw:
            result_ddf = ddf.groupby(group_id).size().compute()
        # print(result_ddf)

    client.close()
