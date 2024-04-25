from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import time

FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        print("Time taken for operation: {:.8f} seconds".format(self.elapsed_time))

def perform_head_tail_operations():
    # Initialize a Dask LocalCluster to fully utilize CPU cores
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard is available at: {client.dashboard_link}")

    for file_name in FILES:
        # Load CSV files into Dask dataframes
        ddf = dd.read_csv(f'../data/mpiops/{file_name}.csv')

        # Perform head operation
        with Stopwatch():
            head_result = ddf.head()
        
        print(head_result)
        
        # Perform tail operation
        with Stopwatch():
            tail_result = ddf.tail()
        
        print(tail_result)

        # Display file size info
        print("Size of File: Approximately {:.1f} instances".format(ddf.shape[0].compute()))

    # Close the Dask client once done
    client.close()

if __name__ == "__main__":
    perform_head_tail_operations()
