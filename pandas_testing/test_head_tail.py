import pandas as pd
import time

FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]

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
        df = pd.read_csv(f'../data/mpiops/{FILES[i]}.csv')
        print(df.head())
        with Stopwatch() as sw1:
            # Perform head operation
            result_df = df.head()

        with Stopwatch() as sw2:
            # Perform head operation
            result_df = df.tail()

        # Print time taken for head and tail operations
        print("Time taken for head operation: {:.8f} seconds".format(sw1.elapsed_time))
        print("Time taken for head operation: {:.8f} seconds".format(sw2.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.shape[0]))