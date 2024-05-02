from pyspark.sql import SparkSession
import time
import sys


class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    csv_file = sys.argv[1]
    num_cpus = int(sys.argv[2]) 
    num_executors = int(sys.argv[3]) 
    executor_memory = sys.argv[4] if len(sys.argv) > 4 else '4g'

    spark = SparkSession.builder \
        .appName("GroupByBenchmark") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.cores", num_cpus) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

    df = spark.read.csv(f'{csv_file}', header=True, inferSchema=True)

    with Stopwatch() as sw2:
        result_df = df.orderBy(df.columns[0], ascending=False).limit(5)

    print("{:.8f}".format(sw2.elapsed_time))

    spark.stop()
