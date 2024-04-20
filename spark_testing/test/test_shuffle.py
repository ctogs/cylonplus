from pyspark.sql.functions import rand
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
        .appName("ShuffleBenchmark") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.cores", num_cpus) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()
    spark = SparkSession.builder.appName("ShuffleBenchmark").getOrCreate()

    df = spark.read.csv(f'{csv_file}', header=True, inferSchema=True)

    with Stopwatch() as sw:
        result_df = df.withColumn("rand", rand()).orderBy("rand")

    print("{:.4f}".format(sw.elapsed_time))

    # Stop Spark session
    spark.stop()
