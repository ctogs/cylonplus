import random
from pyspark.sql import SparkSession
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
    spark = SparkSession.builder.appName("ShuffleBenchmark").getOrCreate()

    for i in range(len(SHUFFLE_FILES)):
        df = spark.read.csv(f'../data/input/{SHUFFLE_FILES[i]}.csv', header=True, inferSchema=True)
        df.show()

        with Stopwatch() as sw:
            result_df = df.orderBy(random())

        print("Time taken for shuffle operation: {:.4f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(result_df.count()))

    # Stop Spark session
    spark.stop()
