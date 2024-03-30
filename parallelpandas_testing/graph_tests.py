import pandas as pd
import time
import matplotlib.pyplot as plt

# Define example Python files
example_files = ["test_groupby.py", "test_head_tail.py", "test_io.py", "test_join.py", "test_setops.py", "test_shuffle.py", "test_sort.py"]

# Define CSV files
csv_files = ["benchmarking_data_76800.csv", "benchmarking_data_768000.csv", "benchmarking_data_7680000.csv", "benchmarking_data_76800000.csv"]

# Initialize empty lists to store runtimes
runtimes = []

# Iterate through example files
for file in example_files:
    for csv_file in csv_files:
        start_time = time.time()
        # Execute each example file with pandas
        exec(open(file).read())
        end_time = time.time()
        elapsed_time = end_time - start_time
        runtimes.append(elapsed_time)
        print(f"Execution time for {file} with {csv_file}: {elapsed_time:.2f} seconds")

# Reshape runtimes list for plotting
runtimes_matrix = [runtimes[i:i+len(csv_files)] for i in range(0, len(runtimes), len(csv_files))]

# Plotting
plt.figure(figsize=(10, 6))
for i, file in enumerate(example_files):
    plt.plot(csv_files, runtimes_matrix[i], label=file)
plt.xlabel('CSV Files')
plt.ylabel('Execution Time (seconds)')
plt.title('Pandas Execution Time for Different Example Files and CSVs')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

