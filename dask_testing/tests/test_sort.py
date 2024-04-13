from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import time

SORT_FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]
SORT_IDS = ["Unit Price", "Unit Price", "Unit Price", "Unit Price"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        print("Time taken for sort operation: {:.4f} seconds".format(self.elapsed_time))

def perform_sort_operations():
    # Initialize a Dask LocalCluster to fully utilize CPU cores
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard is available at: {client.dashboard_link}")

    for file_name, sort_id in zip(SORT_FILES, SORT_IDS):
        # Load CSV files into Dask dataframes
        ddf = dd.read_csv(f'../data/mpiops/{file_name}.csv')

        with Stopwatch() as sw:
            # Perform sort operation using Dask
            result_ddf = ddf.sort_values(by=sort_id)
            # Compute to trigger the sort operation and get results
            result_df = result_ddf.compute()

            # Display the first few rows of the result
            print(result_df.head())
            print("Size of File: {:.1f} instances".format(len(result_df)))

    # Close the Dask client once done
    client.close()

if __name__ == "__main__":
    perform_sort_operations()
