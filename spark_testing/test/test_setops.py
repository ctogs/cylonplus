from pyspark.sql import SparkSession
import time

FILES1 = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]

FILES2 = ["cities_b_0", "cities_b_1", "cities_b_2", "cities_b_3", "csv_with_null2_0", "csv_with_null2_1", "csv_with_null2_2", "csv_with_null2_3", "csv2_0", "csv2_1", "csv2_2", "csv2_3"]

INTERSECT_IDS = [["city", "state_id"], ["city", "state_id"], ["city", "state_id"], ["city", "state_id"]]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("SetOpsBenchmark").getOrCreate()

    for i in range(len(FILES1)):
        # Load CSV files into Spark dataframes
        df1 = spark.read.csv(f'../data/input/{FILES1[i]}.csv', header=True, inferSchema=True)
        df2 = spark.read.csv(f'../data/input/{FILES2[i]}.csv', header=True, inferSchema=True)
        df1.show()

        # Time difference operation (subtract)
        with Stopwatch() as sw1:
            diff_df = df1.subtract(df2)

        # Time union operation
        with Stopwatch() as sw2:
            union_df = df1.union(df2)

        # Time intersection operation
        with Stopwatch() as sw3:
            if i < 4:
                int_df = df1.join(df2, INTERSECT_IDS[i], 'inner').select(df1["*"])

        # Time concatenation operation (union)
        with Stopwatch() as sw4:
            concat_df = df1.union(df2)

        # Time drop duplicate operation
        with Stopwatch() as sw5:
            dedup_df = df1.dropDuplicates()

        # Print time taken for all operations
        print("Time taken for difference operation: {:.4f} seconds".format(sw1.elapsed_time))
        print("Time taken for union operation: {:.4f} seconds".format(sw2.elapsed_time))
        print("Time taken for intersection operation: {:.4f} seconds".format(sw3.elapsed_time))
        print("Time taken for concatenation operation: {:.4f} seconds".format(sw4.elapsed_time))
        print("Time taken for drop duplicates operation: {:.4f} seconds".format(sw5.elapsed_time))
        print("Size of File: {:.1f} instances".format(df1.count()))

    # Stop Spark session
    spark.stop()
