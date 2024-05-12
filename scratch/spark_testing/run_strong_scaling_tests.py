import csv
import os
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Configuration for different CPU counts
cpu_counts = [
    ["1", "1"], # 1 CPU
    ["1", "2"], # 2
    ["1", "4"], # 4
    ["2", "4"], # 8
    ["4", "4"], # 16
    ["8", "4"] # 32
]

example_files = [
    "test/test_groupby.py",
    # "test/test_head.py",
    # "test/test_tail.py",
    # "test/test_io.py",
    # "test/test_difference.py",
    # "test/test_drop_duplicates.py",
    # # "test/test_intersection", #takes a very long time
    # "test/test_union.py",
    # "test/test_join.py",
    # "test/test_shuffle.py",
    # "test/test_sort.py",
]

args = [
    "Pregnancies", # 4 executors, 4 CPUs each
    # [],
    # [],
    # [],
    # # ["4", "4"],
    # # ["4", "4"],
    # # # "Pregnancies",  #takes a very long time
    # [],
    # ["Pregnancies"],
    # [],
    # ["Pregnancies"],
]

num_files = [
    1,
    # 1,
    # 1,
    # 1,
    # # 2,
    # # 1,
    # # # 2,  #takes a very long time
    # # 2,
    # # 2,
    # 1,
    # 1,
]

# List of CSV files
csv_files = [
    # "../data/benchmarking_data_10000.csv",
    # "../data/benchmarking_data_1000000.csv",
    # "../data/benchmarking_data_2500000.csv",
    # "../data/benchmarking_data_5000000.csv",
    # "../data/benchmarking_data_5000000.csv",
    "../data/benchmarking_data_10000000.csv",
    # "../data/benchmarking_data_25000000.csv",
    # "../data/benchmarking_data_50000000.csv",
    # "../data/benchmarking_data_75000000.csv",
    # "../data/benchmarking_data_100000000.csv",
]

# Dictionary to store execution times
execution_times = {f"{file}": [] for file in example_files}

print("Performing tests...")
# Iterate through example files
for file, arg, num_file in zip(example_files, args, num_files):
    # Iterate through CPU counts for scaling tests
    for cpu_count in cpu_counts:
        print(f"Running tests for {file} with {cpu_count} CPUs...")
        # Iterate through CSV files
        for csv_file in csv_files:
            # Run the Python script with subprocess and capture output
            print(["python", file, csv_file] + [arg] + cpu_count)
            result = subprocess.run(
                ["python", file, csv_file] + [arg] + cpu_count,
                capture_output=True,
                text=True
            )
            # Check if the subprocess ran successfully
            if result.returncode == 0:
                output = result.stdout
            else:
                print(f"Error running subprocess for {file} with {csv_file} on {cpu_count} CPUs:")
                print(result.stderr)
                continue
# ['python', 'test/test_groupby.py', '../data/benchmarking_data_10000000.csv', 'Pregnancies', '2', '4']
# ['python', 'test/test_groupby.py', '../data/benchmarking_data_10000.csv', 'Pregnancies', '4', '4'] 
            # Extract execution time from output
            execution_time = float(output)
            execution_times[f"{file}"].append(execution_time)

        print("Test Finished")
# Notify completion
print("All Tests Finished")

filename = './csv_results/spark_execution_times_strong_groupby.csv'

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

# Plotting
# for key, times in execution_times.items():
#     plt.figure(figsize=(10, 6))
    
#     sns.lineplot(x=cpu_counts, y=times, marker='o')
    
#     plt.xlabel("Number of CPUs")
#     plt.ylabel("Execution Time (seconds)")
#     plt.title(f"Scaling Test for {key}")
    
#     plt.xticks(cpu_counts, labels=[f"{cpu} CPUs" for cpu in cpu_counts], rotation=45)
    
#     plt.tight_layout()
#     key = key.replace("scaling_tests/", "")
#     plot_file_name = os.path.join("./plots", f"{key}_scaling_plot.png")
#     plt.savefig(plot_file_name)
#     plt.close()
