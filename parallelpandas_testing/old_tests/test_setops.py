import pandas as pd
import time
from parallel_pandas import ParallelPandas
'''
running test case
we test following operations:
    set_difference
    set_union
    set_intersection
    concat
    drop_duplicates
'''

FILES1 = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]

FILES2 = ["cities_b_0", "cities_b_1", "cities_b_2", "cities_b_3", "csv_with_null2_0", "csv_with_null2_1", "csv_with_null2_2", "csv_with_null2_3", "csv2_0", "csv2_1", "csv2_2", "csv2_3"]

INTERSECT_IDS = [["city", "state_id"], ["city", "state_id"], ["city", "state_id"], ["city", "state_id"], ]
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
def perform_difference(df1, df2):
    return df1[~df1.index.isin(df2.index)]

if __name__ == "__main__":
    # Time operations for all specified files
    for i in range(len(FILES1)):
            # Load CSV files into pandas dataframes
        df1 = pd.read_csv(f'../data/input/{FILES1[i]}.csv')
        df2 = pd.read_csv(f'../data/input/{FILES2[i]}.csv')
        print(df1.head())
        #time difference operations
        with Stopwatch() as sw1:
            diff_df = perform_difference(df1, df2)

        #time union operations
        with Stopwatch() as sw2:
            union_df = pd.concat([df1, df2])
        
        #time intersection operations
        with Stopwatch() as sw3:
            if(i < 4):
                int_df = pd.merge(df1, df2, how='inner', on=INTERSECT_IDS[i]) 

        #time concat operations
        with Stopwatch() as sw4:
            concat_df = pd.concat([df1, df2])

        #time drop duplicate operations
        with Stopwatch() as sw5:
            concat_df = df1.drop_duplicates()

        # Print time taken for all operations
        print("Time taken for difference operation: {:.4f} seconds".format(sw1.elapsed_time))
        print("Time taken for union operation: {:.4f} seconds".format(sw2.elapsed_time))
        print("Time taken for intersection operation: {:.4f} seconds".format(sw3.elapsed_time))
        print("Time taken for concatenation operation: {:.4f} seconds".format(sw4.elapsed_time))
        print("Time taken for drop duplicates operation: {:.4f} seconds".format(sw5.elapsed_time))
        print("Size of File: {:.1f} instances".format(df1.shape[0]))
