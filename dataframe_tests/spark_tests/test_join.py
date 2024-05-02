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
    csv_file1 = sys.argv[1]
    csv_file2 = sys.argv[2]
    join_id = sys.argv[3]
    num_cpus = int(sys.argv[4]) 
    num_executors = int(sys.argv[5]) 
    executor_memory = sys.argv[6] if len(sys.argv) > 6 else '4g'

    spark = SparkSession.builder \
        .appName("GroupByBenchmark") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.cores", num_cpus) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()
    
    spark = SparkSession.builder.appName("JoinBenchmark").getOrCreate()

    df1 = spark.read.csv(f'{csv_file1}', header=True, inferSchema=True)
    df2 = spark.read.csv(f'{csv_file2}', header=True, inferSchema=True)

    with Stopwatch() as sw:
        result_df = df1.join(df2, df1[join_id] == df2[join_id])

    print("{:.4f}".format(sw.elapsed_time))

    spark.stop()
