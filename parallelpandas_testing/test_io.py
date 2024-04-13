import pandas as pd
import time
from parallel_pandas import ParallelPandas
import sys

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

if __name__ == "__main__":

    #get file and group_id input:
    csv_file = sys.argv[1]

    #Start stopwatch
    with Stopwatch() as sw:
        
        # Load CSV files into pandas dataframes
        df1 = pd.read_csv(f'{csv_file}')

    # Print time taken for reading operation
    print("{:.4f}".format(sw.elapsed_time))