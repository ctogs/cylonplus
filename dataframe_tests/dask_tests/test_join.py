from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import time

JOIN_LIST1 = [
    "cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3",
    "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3",
    "csv1_0", "csv1_1", "csv1_2", "csv1_3"
]

JOIN_LIST2 = [
    "cities_b_0", "cities_b_1", "cities_b_2", "cities_b_3",
    "csv_with_null2_0", "csv_with_null2_1", "csv_with_null2_2", "csv_with_null2_3",
    "csv2_0", "csv2_1", "csv2_2", "csv2_3"
]

JOIN_IDS = [
    "state_id", "state_id", "state_id", "state_id",
    "0", "0", "0", "0", "0", "0", "0", "0"
]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        print("Time taken for join operation: {:.4f} seconds".format(self.elapsed_time))

def perform_join_operations():
    # Initialize a Dask LocalCluster to fully utilize CPU cores
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard is available at: {client.dashboard_link}")

    for i in range(len(JOIN_LIST1)):
        # Load CSV files into Dask dataframes
        ddf1 = dd.read_csv(f'../data/input/{JOIN_LIST1[i]}.csv')
        ddf2 = dd.read_csv(f'../data/input/{JOIN_LIST2[i]}.csv')

        with Stopwatch() as sw:
            # Perform join operation using Dask
            result_ddf = ddf1.merge(ddf2, on=JOIN_IDS[i], how='inner')  # Adjust 'how' as needed for your join type
            # Compute to trigger the join operation and get results
            result_df = result_ddf.compute()
            

        print(result_df.head())

    # Close the Dask client once done
    client.close()

if __name__ == "__main__":
    perform_join_operations()
