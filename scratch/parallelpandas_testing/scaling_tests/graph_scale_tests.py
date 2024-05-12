import os
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Configuration for different CPU counts
cpu_counts = [1, 2, 4, 8, 16, 32] 

# List of Python files
example_files = [
    "scaling_tests/strong_groupby.py",
    # "scaling_tests/test_io.py",
]

args = [
    "Pregnancies",
    # "",
]

num_files = [
    1,
    # 1,
]

# List of CSV files (keep this the same for strong scaling; modify for weak scaling if needed)
csv_files = [
    "../data/benchmarking_data_10000.csv", 
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
            result = subprocess.run(
                ["python", file, csv_file] + [arg] * num_file + [str(cpu_count)],
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

            # Extract execution time from output
            execution_time = float(output)
            execution_times[f"{file}"].append(execution_time)

        print("Test Finished")
# Notify completion
print("All Tests Finished")

# Plotting
for key, times in execution_times.items():
    plt.figure(figsize=(10, 6))
    
    sns.lineplot(x=cpu_counts, y=times, marker='o')
    
    plt.xlabel("Number of CPUs")
    plt.ylabel("Execution Time (seconds)")
    plt.title(f"Scaling Test for {key}")
    
    plt.xticks(cpu_counts, labels=[f"{cpu} CPUs" for cpu in cpu_counts], rotation=45)
    
    plt.tight_layout()
    key = key.replace("scaling_tests/", "")
    plot_file_name = os.path.join("./plots", f"{key}_scaling_plot.png")
    plt.savefig(plot_file_name)
    plt.close()
