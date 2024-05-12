import cudf as pd
import time
import sys
import numpy as np

N_CPU = 16
SPLIT_FACTOR = 4
ParallelPandas.initialize(n_cpu=N_CPU, split_factor=SPLIT_FACTOR, disable_pr_bar=True)

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

def perform_shuffle(csv_file):
    # Load CSV file into a pandas DataFrame using parallel_pandas
    df = pd.read_csv(f'{csv_file}')

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            _ = df.sample(frac=1)  # Perform shuffle operation

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform shuffle operation
            _ = df.sample(frac=1)
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    return mean_time, std_dev

if __name__ == "__main__":
    csv_file = sys.argv[1]  # Get file name from command line
    mean_time, std_dev = perform_shuffle(csv_file)
    
    # Print mean time and standard deviation without extra text
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))
