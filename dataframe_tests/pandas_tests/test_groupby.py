import pandas as pd
import time
import sys
import numpy as np

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":

    # Get file and group_id input
    csv_file = sys.argv[1]
    group_id = sys.argv[2]

    # Load CSV file into a pandas dataframe
    df1 = pd.read_csv(csv_file)

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            result_df = df1.groupby(group_id)

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform groupby operation
            result_df = df1.groupby(group_id)
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Print mean time and standard deviation
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))