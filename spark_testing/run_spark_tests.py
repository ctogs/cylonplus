import os
import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import csv

# List of Python files
example_files = [
    "test/test_groupby.py",
    "test/test_head.py",
    "test/test_tail.py",
    "test/test_io.py",
    # "test/test_difference.py",
    # "test/test_drop_duplicates.py",
    # # "test/test_intersection", #takes a very long time
    # "test/test_union.py",
    # "test/test_join.py",
    "test/test_shuffle.py",
    "test/test_sort.py",
]

args = [
    ["Pregnancies", "4", "4"], # 4 executors, 4 CPUs each
    ["4", "4"],
    ["4", "4"],
    ["4", "4"],
    # ["4", "4"],
    # ["4", "4"],
    # # "Pregnancies",  #takes a very long time
    ["4", "4"],
    ["Pregnancies", "4", "4"],
    ["4", "4"],
    ["Pregnancies", "4", "4"],

]

num_files = [
    1,
    1,
    1,
    1,
    # 2,
    # 1,
    # # 2,  #takes a very long time
    # 2,
    # 2,
    1,
    1,
]

# List of CSV files
csv_files = [
    "../data/benchmarking_data_10000.csv",
    "../data/benchmarking_data_1000000.csv",
    "../data/benchmarking_data_2500000.csv",
    "../data/benchmarking_data_5000000.csv",
    "../data/benchmarking_data_5000000.csv",
    "../data/benchmarking_data_10000000.csv",
    "../data/benchmarking_data_25000000.csv",
    "../data/benchmarking_data_50000000.csv",
    "../data/benchmarking_data_75000000.csv",
    "../data/benchmarking_data_100000000.csv",
]

# List of row counts
row_counts = [10000, 1000000, 2500000, 5000000, 10000000, 25000000, 50000000, 75000000, 100000000] 

# Dictionary to store execution times
execution_times = {}

print("Performing tests...")
# Iterate through example files
for file, arg, num_file in zip(example_files, args, num_files):
    execution_times[file] = []

    # Iterate through CSV files
    print(f"Running tests for {file} with argument (if needed): {arg}...")
    for csv_file in csv_files:
        # Run the Python script with subprocess and capture output
        # print(f"\n {['python', file, csv_file] + [arg] * num_file} \n")
        result = subprocess.run(
            ["python", file, csv_file] + arg * num_file,
            capture_output=True,
            text=True
        )

        # Check if the subprocess ran successfully
        if result.returncode == 0:
            output = result.stdout
        else:
            print(f"Error running subprocess for {file} with {csv_file}:")
            print(result.stderr)
            continue

        # Extract execution time from output
        execution_time = float(output)
        execution_times[file].append(execution_time)

    print("Test Finished")
# Notify completion
print("All Tests Finished")

filename = './csv_results/spark_execution_times_16cpu.csv'

# Create the CSV file and write the header and rows
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write a header row
    writer.writerow(['Test File', 'Time 1', 'Time 2', 'Time 3', 'Time 4', 'Time 5', 'Time 6', 'Time 7', 'Time 8', 'Time 9', 'Time 10'])

    # Write data rows
    for file, times in execution_times.items():
        # Prepare the row with the file name and times
        row = [file] + times
        writer.writerow(row)

print(f"Data successfully written to {filename}")