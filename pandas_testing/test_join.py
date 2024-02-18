import pandas as pd
import time

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
    for i in range(4):
            # Load CSV files into pandas dataframes
        df1 = pd.read_csv(f'../data/input/cities_a_{i}.csv')
        df2 = pd.read_csv(f'../data/input/cities_b_{i}.csv')
        print(df1.head())
        with Stopwatch() as sw:
            # Perform join operation
            result_df = perform_join(df1, df2, "state_id")

        # Print time taken for join operation
        print("Time taken for join operation: {:.4f} seconds".format(sw.elapsed_time))