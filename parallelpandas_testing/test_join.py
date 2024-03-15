import pandas as pd
import time
from parallel_pandas import ParallelPandas

JOIN_LIST1 = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]

JOIN_LIST2 = ["cities_b_0", "cities_b_1", "cities_b_2", "cities_b_3", "csv_with_null2_0", "csv_with_null2_1", "csv_with_null2_2", "csv_with_null2_3", "csv2_0", "csv2_1", "csv2_2", "csv2_3"]

JOIN_IDS = ["state_id","state_id","state_id","state_id","0","0","0","0","0","0","0","0"]
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

# Function to perform join operation
def perform_join(df1, df2, common_column):
    return pd.merge(df1, df2, on=common_column)

if __name__ == "__main__":
    # Start stopwatch
    for i in range(len(JOIN_LIST1)):
            # Load CSV files into pandas dataframes
        df1 = pd.read_csv(f'../data/input/{JOIN_LIST1[i]}.csv')
        df2 = pd.read_csv(f'../data/input/{JOIN_LIST2[i]}.csv')
        print(df1.head())
        with Stopwatch() as sw:
            # Perform join operation
            result_df = perform_join(df1, df2, JOIN_IDS[i])

        # Print time taken for join operation
        print("Time taken for join operation: {:.4f} seconds".format(sw.elapsed_time))
