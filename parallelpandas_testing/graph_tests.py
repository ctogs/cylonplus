import os
import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt

# List of Python files
example_files = [
    "test_groupby.py",
    "test_head.py",
    "test_tail.py",
    "test_io.py",
    "test_difference.py",
    "test_drop_duplicates.py",
    # "test_intersection", #takes a very long time
    "test_union.py",
    "test_join.py",
    "test_shuffle.py",
    "test_sort.py",
]

args = [
    "Pregnancies",
    "",
    "",
    "",
    "",
    "",
    # "Pregnancies",  #takes a very long time
    "",
    "Pregnancies",
    "",
    "Pregnancies",

]

num_files = [
    1,
    1,
    1,
    1,
    2,
    1,
    # 2,  #takes a very long time
    2,
    2,
    1,
    1,
]

# List of CSV files
csv_files = [
    "data/benchmarking_data_76800.csv",
    "data/benchmarking_data_768000.csv",
    "data/benchmarking_data_7680000.csv",
    # "data/benchmarking_data_76800000.csv",
]

# List of Sizes
# sizes = [76800, 768000, 7680000, 76800000]
sizes = [76800, 768000, 7680000]
# Dictionary to store execution times
execution_times = {}

print("Performing tests...")
# Iterate through example files
argCount = 0
for file in example_files:
    execution_times[file] = []

    # Iterate through CSV files
    print(file, "with id of (if necessary): ", args[argCount])
    for csv_file in csv_files:
        # Run the Python script with subprocess and capture output
        if(num_files[argCount] == 1):
            result = subprocess.run(["python", file, csv_file, args[argCount]],
                capture_output=True,
                text=True
            )
        elif(num_files[argCount] == 2):
            result = subprocess.run(["python", file, csv_file, csv_file, args[argCount]],
                capture_output=True,
                text=True
            )
        else:
            print("Not a valid number of files")

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

    argCount += 1

# Print execution times for debugging
print("Execution Times:", execution_times)

# Plotting with Seaborn and save plots in "plots" subfolder
for file, times in execution_times.items():
    plt.figure(figsize=(10, 6))
    sns.lineplot(x=sizes, y=times, marker='o')
    plt.xlabel("File Size")
    plt.ylabel("Execution Time (seconds)")
    plt.title(f"Pandas Execution Time for {file} with Different CSV Sizes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_file_name = os.path.join("plots", f"{file}_plot.png")
    plt.savefig(plot_file_name)
    plt.close()