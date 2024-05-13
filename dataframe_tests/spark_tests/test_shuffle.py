from pyspark.sql.functions import rand
from pyspark.sql import SparkSession
import sys
import time
import numpy as np

class Stopwatch:
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

def perform_shuffle(csv_file):
    # Initialize Spark session with specific configurations
    spark = SparkSession.builder \
        .appName("ShuffleBenchmark") \
        .config("spark.executor.instances", '4') \
        .config("spa4rk.executor.cores", '4') \
        .config("spark.executor.memory", '4g') \
        .getOrCreate()

    # Load CSV file into DataFrame
    df = spark.read.csv(f'{csv_file}', header=True, inferSchema=True)

    N_RUNS = 10  # Number of times to run the test
    WARMUP_RUNS = 5  # Number of warm-up runs

    execution_times = []

    # Warm-up runs
    for _ in range(WARMUP_RUNS):
        with Stopwatch():
            _ = df.withColumn("rand", rand()).orderBy("rand")

    # Measure execution time for multiple runs
    for _ in range(N_RUNS):
        with Stopwatch() as sw:
            # Perform shuffle operation using Spark
            result_df = df.withColumn("rand", rand()).orderBy("rand")
        execution_times.append(sw.elapsed_time)

    # Calculate mean and standard deviation of execution times
    mean_time = np.mean(execution_times)
    std_dev = np.std(execution_times)

    # Stop Spark session
    spark.stop()

    return mean_time, std_dev

if __name__ == "__main__":
    csv_file = sys.argv[1]  # Get file name from command line
    # num_cpus = int(sys.argv[2]) 
    # num_executors = int(sys.argv[3]) 
    # executor_memory = sys.argv[4] if len(sys.argv) > 4 else '4g'
    mean_time, std_dev = perform_shuffle(csv_file)

    # Print mean time and standard deviation without extra text
    print("{:.4f}".format(mean_time))
    print("{:.4f}".format(std_dev))
