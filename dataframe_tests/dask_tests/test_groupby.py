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

if __name__ == "__main__":
    # Read command line arguments
    csv_file = sys.argv[1] 
    group_id = sys.argv[2]

    # Initialize a Dask LocalCluster
    cluster = LocalCluster(n_workers=4, threads_per_worker=4)  # Adjust based on system's capabilities
    client = Client(cluster)

    # Load CSV file into a Dask dataframe
    ddf = dd.read_csv(csv_file)

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            _ = ddf.groupby(group_id).size().compute()

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform groupby operation and compute result
            result_ddf = ddf.groupby(group_id).size().compute()
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Print mean time and standard deviation
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))

    client.close()
