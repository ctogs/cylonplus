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

def perform_io_operations(csv_file):
    # Initialize a Dask LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)

    N_RUNS = 10  # Number of times to run the test

    execution_times = []

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            ddf = dd.read_csv(csv_file)
            ddf.compute()  # Force computation to measure I/O time
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Print mean time and standard deviation
    print("{:.8f}".format(mean_time))
    print("{:.8f}".format(std_dev))

    # Close the Dask client once done
    client.close()

if __name__ == "__main__":
    csv_file = sys.argv[1]  # Get file name from command line
    perform_io_operations(csv_file)
