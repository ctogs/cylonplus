import sys
import time
from pyspark.sql import SparkSession

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

if __name__ == "__main__":
    # Get command line arguments for input files
    csv_file1 = sys.argv[1]
    csv_file2 = sys.argv[2]
    num_cpus = int(sys.argv[3]) 
    num_executors = int(sys.argv[4]) 
    executor_memory = sys.argv[5] if len(sys.argv) > 5 else '4g'

    spark = SparkSession.builder \
        .appName("GroupByBenchmark") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.cores", num_cpus) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

    # Load CSV files into Spark DataFrames
    df1 = spark.read.csv(f'{csv_file1}', header=True, inferSchema=True)
    df2 = spark.read.csv(f'{csv_file2}', header=True, inferSchema=True)

    # Start stopwatch
    with Stopwatch() as sw:
        # Perform union operation
        union_df = df1.union(df2)

    # Print time taken for union operation
    print("{:.4f}".format(sw.elapsed_time))

    # Stop Spark session
    spark.stop()
