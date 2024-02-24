from pyspark.sql import SparkSession
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
    spark = SparkSession.builder.appName("IOBenchmark").getOrCreate()

    for i in range(len(FILES)):
        with Stopwatch() as sw:
            df = spark.read.csv(f'../data/mpiops/{FILES[i]}.csv', header=True, inferSchema=True)
        
        print("Time taken for read input operation: {:.8f} seconds".format(sw.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.count()))

    spark.stop()
