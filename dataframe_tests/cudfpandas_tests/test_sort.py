import pandas as pd
import time

SORT_FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]

SORT_IDS = ["Unit Price","Unit Price","Unit Price","Unit Price"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    # Start stopwatch
    for i in range(len(SORT_FILES)):
            # Load CSV files into pandas dataframes
        df = pd.read_csv(f'../data/mpiops/{SORT_FILES[i]}.csv')
        print(df.head())
        with Stopwatch() as sw:
            # Perform sort operation
            result_df = df.sort_values(by=SORT_IDS[i])

        # Print time taken for sort operation
        print("Time taken for shuffle operation: {:.4f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.shape[0]))