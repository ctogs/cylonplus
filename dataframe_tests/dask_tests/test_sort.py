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

def perform_sort_operations(csv_file, sort_id):
    # Initialize a Dask LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)

    # Load CSV file into a Dask dataframe
    ddf = dd.read_csv(csv_file)

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            _ = ddf.sort_values(by=sort_id).compute()

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform sort operation using Dask
            sorted_ddf = ddf.sort_values(by=sort_id)
            _ = sorted_ddf.compute()  # Trigger computation to measure time
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Close the Dask client once done
    client.close()

    return mean_time, std_dev

if __name__ == "__main__":
    csv_file = sys.argv[1]  # Get file name from command line
    sort_id = sys.argv[2]   # Get sort column from command line
    mean_time, std_dev = perform_sort_operations(csv_file, sort_id)
    
    # Print mean time and standard deviation without extra text
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))
