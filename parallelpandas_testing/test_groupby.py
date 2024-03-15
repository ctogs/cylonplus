import pandas as pd
from parallel_pandas import ParallelPandas
import time

FILES = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]

GROUP_IDS = ["state_id","state_id","state_id","state_id","0","0","0","0","0","0","0","0"]
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
    # Start stopwatch
    for i in range(len(FILES)):
            # Load CSV files into pandas dataframes
        df1 = pd.read_csv(f'../data/input/{FILES[i]}.csv')
        print(df1.head())
        with Stopwatch() as sw:
            # Perform groupby operation
            result_df = df1.groupby(GROUP_IDS[i])

        # Print time taken for groupby operation
        print("Time taken for groupby operation: {:.4f} seconds".format(sw.elapsed_time))
