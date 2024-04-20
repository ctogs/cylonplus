import os
import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# List of Python files
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
    ["Pregnancies", "4", "4"], # 4 executors, 4 CPUs each
    # "",
    # "",
    # "4 4",
    # "",
    # "",
    # # "Pregnancies",  #takes a very long time
    # "",
    # "Pregnancies",
    # "",
    # "Pregnancies",

]

num_files = [
    1,
    # 1,
    # 1,
    # 1,
    # 2,
    # 1,
    # # 2,  #takes a very long time
    # 2,
    # 2,
    # 1,
    # 1,
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

# Plotting
for file, times in execution_times.items():
    plt.figure(figsize=(10, 6))
    
    # Moving average
    smoothed_times = np.convolve(times, np.ones(3)/3, mode='valid')
    
    sns.lineplot(x=row_counts[:len(smoothed_times)], y=smoothed_times, marker='o')
    
    plt.xlabel("Number of Rows")
    plt.ylabel("Execution Time (seconds)")
    plt.title(f"Apache Spark Execution Time for {file} with Different CSV Row Counts")
    plt.xticks(row_counts[:len(smoothed_times)], labels=[f"{rows/1000:.0f} K" for rows in row_counts[:len(smoothed_times)]], rotation=45)
    
    # Add Error Bars
    plt.errorbar(row_counts[:len(smoothed_times)], smoothed_times, yerr=np.std(times), fmt='-o', capsize=5)
    
    # Annotate significant points
    max_index = np.argmax(smoothed_times)
    plt.annotate(f'Maximum\n({row_counts[max_index]} Rows, {smoothed_times[max_index]:.2f} seconds)',
                 xy=(row_counts[max_index], smoothed_times[max_index]),
                 xytext=(row_counts[max_index]*0.7, smoothed_times[max_index]*0.7),
                 arrowprops=dict(facecolor='black', shrink=0.05))
    
    plt.tight_layout()
    plot_file_name = os.path.join("plots", f"{file[5:]}_plot.png")
    plt.savefig(plot_file_name)
    plt.close()