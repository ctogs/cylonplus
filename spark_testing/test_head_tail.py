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
    spark = SparkSession.builder.appName("HeadTailBenchmark").getOrCreate()

    for i in range(len(FILES)):
        df = spark.read.csv(f'../data/mpiops/{FILES[i]}.csv', header=True, inferSchema=True)
        df.show(5)

        with Stopwatch() as sw1:
            result_df = df.limit(5)
            result_df.show()

        with Stopwatch() as sw2:
            result_df = df.orderBy(df.columns[0], ascending=False).limit(5)
            result_df.show()

        print("Time taken for head operation: {:.8f} seconds".format(sw1.elapsed_time))
        print("Time taken for tail operation: {:.8f} seconds".format(sw2.elapsed_time))
        print("Size of File: {:.1f} instances".format(df.count()))

    spark.stop()
