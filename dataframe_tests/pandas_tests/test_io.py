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

    # Get file input
    csv_file = sys.argv[1]

    # List to store elapsed times
    elapsed_times = []

    # Number of runs
    N_RUNS = 10

    # Perform the operation multiple times
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Load CSV files into pandas dataframe
            df1 = pd.read_csv(csv_file)

        # Store elapsed time
        elapsed_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation
    mean_time = np.mean(elapsed_times)
    std_dev = np.std(elapsed_times)

    # Print mean time and standard deviation
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))