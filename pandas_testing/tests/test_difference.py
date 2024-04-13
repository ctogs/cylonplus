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

# Function to perform difference operation
def perform_difference(df1, df2):
    return df1[~df1.index.isin(df2.index)]

if __name__ == "__main__":

    #get file and group_id input:
    csv_file1 = sys.argv[1]
    csv_file2 = sys.argv[2]

    # Load CSV files into pandas dataframes
    df1 = pd.read_csv(f'{csv_file1}')
    df2 = pd.read_csv(f'{csv_file2}')

    #Start stopwatch
    with Stopwatch() as sw:
        # Perform difference operation
        diff_df = perform_difference(df1, df2)

    # Print time taken for groupby operation
    print("{:.4f}".format(sw.elapsed_time))