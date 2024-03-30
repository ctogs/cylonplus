import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt

# List of example Python files
example_files = [
    "test_groupby.py",
    # "test_head_tail.py",
    # "test_io.py",
    # "test_join.py",
    # "test_setops.py",
    # "test_shuffle.py",
    # "test_sort.py",
]

args = [
    "Pregnancies",
]

# List of CSV files
csv_files = [
    "data/benchmarking_data_76800.csv",
    "data/benchmarking_data_768000.csv",
    "data/benchmarking_data_7680000.csv",
    "data/benchmarking_data_76800000.csv",
]

# List of Sizes
sizes = [76800, 768000, 7680000, 76800000]

# Dictionary to store execution times
execution_times = {}

# Iterate through example files
for file in example_files:
    execution_times[file] = []

    # Iterate through CSV files
    argCount = 0
    for csv_file in csv_files:
        # Run the Python script with subprocess and capture output
        result = subprocess.run(
            ["python", file, csv_file, args[argCount]],
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
    argCount += 1

# Print execution times for debugging
print("Execution Times:", execution_times)

# Plotting with Seaborn
plt.figure(figsize=(10, 6))
for file, times in execution_times.items():
    sns.lineplot(x=sizes, y=times, label=file, marker='o')

plt.xlabel("File Size")
plt.ylabel("Execution Time (seconds)")
plt.title("Pandas Execution Time for Different Example Files and CSVs")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()



# Show plot
plt.show()