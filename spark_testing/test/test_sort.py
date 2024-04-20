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
    sort_id = sys.argv[2]
    num_cpus = int(sys.argv[3]) 
    num_executors = int(sys.argv[4]) 
    executor_memory = sys.argv[5] if len(sys.argv) > 5 else '4g'

    spark = SparkSession.builder \
        .appName("SortBenchmark") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.cores", num_cpus) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()


    df = spark.read.csv(f'{csv_file}', header=True, inferSchema=True)

    with Stopwatch() as sw:
        sorted_df = df.sort(sort_id)

    print("Time taken for sort operation: {:.4f} seconds".format(sw.elapsed_time))

    spark.stop()
