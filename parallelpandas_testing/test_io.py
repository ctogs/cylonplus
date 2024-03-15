import pandas as pd
import time
from parallel_pandas import ParallelPandas

N_CPU = 16
SPLIT_FACTOR = 4
FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]
ParallelPandas.initialize(n_cpu=N_CPU, split_factor=SPLIT_FACTOR, disable_pr_bar=True)

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    # Start stopwatch
    for i in range(len(FILES)):
            # Load CSV files into pandas dataframes
        with Stopwatch() as sw:
            # Perform read input operation
            df = pd.read_csv(f'../data/mpiops/{FILES[i]}.csv')
        
        # Print time taken for operation
        print("Time taken for read input operation: {:.8f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.shape[0]))
