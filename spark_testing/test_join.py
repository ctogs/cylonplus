from pyspark.sql import SparkSession
import time

JOIN_LIST1 = ["cities_a_0", "cities_a_1", "cities_a_2", "cities_a_3", "csv_with_null1_0", "csv_with_null1_1", "csv_with_null1_2", "csv_with_null1_3", "csv1_0", "csv1_1", "csv1_2", "csv1_3"]

JOIN_LIST2 = ["cities_b_0", "cities_b_1", "cities_b_2", "cities_b_3", "csv_with_null2_0", "csv_with_null2_1", "csv_with_null2_2", "csv_with_null2_3", "csv2_0", "csv2_1", "csv2_2", "csv2_3"]

JOIN_IDS = ["state_id", "state_id", "state_id", "state_id", "0", "0", "0", "0", "0", "0", "0", "0"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("JoinBenchmark").getOrCreate()

    for i in range(len(JOIN_LIST1)):
        df1 = spark.read.csv(f'../data/input/{JOIN_LIST1[i]}.csv', header=True, inferSchema=True)
        df2 = spark.read.csv(f'../data/input/{JOIN_LIST2[i]}.csv', header=True, inferSchema=True)
        df1.show()

        with Stopwatch() as sw:
            result_df = df1.join(df2, df1[JOIN_IDS[i]] == df2[JOIN_IDS[i]])

        print("Time taken for join operation: {:.4f} seconds".format(sw.elapsed_time))

    spark.stop()
