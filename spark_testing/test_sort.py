from pyspark.sql import SparkSession
import time

SORT_FILES = ["sales_nulls_nunascii_0", "sales_nulls_nunascii_1", "sales_nulls_nunascii_2", "sales_nulls_nunascii_3"]

SORT_IDS = ["Unit Price", "Unit Price", "Unit Price", "Unit Price"]

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SortBenchmark").getOrCreate()

    for i in range(len(SORT_FILES)):
        df = spark.read.csv(f'../data/mpiops/{SORT_FILES[i]}.csv', header=True, inferSchema=True)

        with Stopwatch() as sw:
            sorted_df = df.sort(SORT_IDS[i])

        print("Time taken for sort operation: {:.4f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(sorted_df.count()))

    spark.stop()
