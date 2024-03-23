from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import time

SHUFFLE_FILES = [
    "cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3",
    "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3",
    "csv1_0", "csv1_1", "csv1_2", "csv1_3", "user_device_tm_1"
]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        print("Time taken for shuffle operation: {:.4f} seconds".format(self.elapsed_time))

def perform_shuffle_operations():
    # Initialize a Dask LocalCluster to fully utilize CPU cores
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard is available at: {client.dashboard_link}")

    for file_name in SHUFFLE_FILES:
        # Load CSV files into Dask dataframes
        ddf = dd.read_csv(f'../data/input/{file_name}.csv')

        with Stopwatch() as sw:
            # Perform shuffle operation using Dask
            shuffled_ddf = ddf.sample(frac=1).persist()
            
            # Display the first few rows of the result
            print(shuffled_ddf.head())
            print("Size of File: {:.1f} instances".format(len(shuffled_ddf)))

    # Close the Dask client once done
    client.close()

if __name__ == "__main__":
    perform_shuffle_operations()
