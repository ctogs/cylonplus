import pandas as pd
import time
import sys

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

    # Load CSV files into pandas dataframes
    df1 = pd.read_csv(f'{csv_file}')

    #Start stopwatch
    with Stopwatch() as sw:
        
        # Perform groupby operation
        result_df = df1.sample(frac = 1)

    # Print time taken for groupby operation
    print("{:.4f}".format(sw.elapsed_time))
