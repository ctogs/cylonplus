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

# Function to perform join operation
def perform_join(df1, df2, common_column):
    return pd.merge(df1, df2, on=common_column)

if __name__ == "__main__":

    #get file and group_id input:
    csv_file1 = sys.argv[1]
    csv_file2 = sys.argv[2]
    join_id = sys.argv[3]

    # Load CSV files into pandas dataframes
    df1 = pd.read_csv(f'{csv_file1}')
    df2 = pd.read_csv(f'{csv_file2}')

    #Start stopwatch
    with Stopwatch() as sw:
        # Perform difference operation
        result_df = perform_join(df1, df2, join_id)

    # Print time taken for groupby operation
    print("{:.4f}".format(sw.elapsed_time))
    
   