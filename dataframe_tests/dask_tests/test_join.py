from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import sys
import time
import numpy as np

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

def perform_join_operations(csv_file1, csv_file2, join_id):
    # Initialize a Dask LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    
    # Load CSV files into Dask dataframes
    ddf1 = dd.read_csv(csv_file1)
    ddf2 = dd.read_csv(csv_file2)

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            _ = ddf1.merge(ddf2, on=join_id, how='inner').compute()

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform join operation using Dask and trigger computation
            result_ddf = ddf1.merge(ddf2, on=join_id, how='inner')
            _ = result_ddf.compute()
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Close the Dask client once done
    client.close()

    return mean_time, std_dev

if __name__ == "__main__":
    csv_file1 = sys.argv[1]
    csv_file2 = sys.argv[2]
    join_id = sys.argv[3]
    
    mean_time, std_dev = perform_join_operations(csv_file1, csv_file2, join_id)
    
    # Print mean time and standard deviation without extra text
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))
