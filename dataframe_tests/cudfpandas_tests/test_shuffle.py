import pandas as pd
import time

SHUFFLE_FILES = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3", "user_device_tm_1"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    # Start stopwatch
    for i in range(len(SHUFFLE_FILES)):
            # Load CSV files into pandas dataframes
        df = pd.read_csv(f'../data/input/{SHUFFLE_FILES[i]}.csv')
        print(df.head())
        with Stopwatch() as sw:
            # Perform shuffle operation
            result_df = df.sample(frac = 1)

        # Print time taken for shuffle operation
        print("Time taken for shuffle operation: {:.4f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.shape[0]))