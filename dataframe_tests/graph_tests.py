import os
import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# List of dataframe folders and corresponding test files
dataframes = {
    "pandas": "pandas_tests",
    "parallelpandas": "parallelpandas_tests",
    "dask": "dask_tests"
    # Add more dataframes here if needed
}

# List of Python files for each dataframe
example_files = {
    "pandas": [
        # "test_groupby.py",
        "test_io.py",
        # Add more test files specific to pandas if needed
    ],
    "parallelpandas": [
        # "test_groupby.py",
        "test_io.py",
        # Add more test files specific to parallelpandas if needed
    ],
    "dask": [
        # "test_groupby.py",
        "test_io.py",
        # Add more test files specific to parallelpandas if needed
    ],
    # Add more dataframes here with their corresponding test files
    # "spark": [
    #     # "test_groupby.py",
    #     "test_io.py",
    # ],
}

# Dictionary of commands to run for each test file
test_files = {
    # "test_groupby.py": ["Pregnancies"],
    "test_io.py": [""],
    # Add more test files with their commands if needed
}

# List of CSV files
csv_files = [
    "../data/benchmarking_data_10000.csv",
    "../data/benchmarking_data_5000000.csv",
    # "data/benchmarking_data_10000000.csv",
    # "data/benchmarking_data_25000000.csv",
    # "data/benchmarking_data_50000000.csv",
    # "data/benchmarking_data_75000000.csv",
    # "data/benchmarking_data_100000000.csv",
]

# List of row counts
row_counts = [10000, 5000000, 10000000, 25000000, 50000000, 75000000, 100000000]

# Dictionary to store execution times
execution_times = {}

print("Performing tests...")

# Iterate through dataframes
for dataframe, folder in dataframes.items():
    execution_times[dataframe] = {}
    dataframe_files = [os.path.join(folder, file) for file in example_files[dataframe]]

    # Iterate through example files
    for file in dataframe_files:
        execution_times[dataframe][file] = {"mean": [], "std_dev": []}

        # Iterate through CSV files
        print(f"Running tests for {file}...")
        for csv_file in csv_files:
            # Get the commands for the current test file
            commands = test_files.get(os.path.basename(file), [])
            
            # Run the Python script with subprocess and capture output for each command
            for command in commands:
                print("Running test: python " + str(file) + " " + str(csv_file) + " " + str(command))
                result = subprocess.run(
                    ["python", file, csv_file] + [command],
                    capture_output=True,
                    text=True
                )

                # Check if the subprocess ran successfully
                if result.returncode == 0:
                    output = result.stdout
                else:
                    print(f"Error running subprocess for {file} with {csv_file} and command {command}:")
                    print(result.stderr)
                    continue

                # Extract mean and standard deviation from output
                mean, std_dev = map(float, output.strip().split())
                execution_times[dataframe][file]["mean"].append(mean)
                execution_times[dataframe][file]["std_dev"].append(std_dev)

    print(f"All Tests Finished for {dataframe}")

# Notify completion
print("All Tests Finished")

#Debugging Print
print("Execution Times: ", execution_times)

# Plotting
for file in example_files["pandas"]:  # Assuming pandas has the same test files as other dataframes
    plt.figure(figsize=(10, 6))
    
    # Initialize a flag to track if any valid data is found
    valid_data_found = False
    
    for dataframe, files in execution_times.items():
        mean_times = files[os.path.join(dataframes[dataframe], file)]["mean"]
        std_devs = files[os.path.join(dataframes[dataframe], file)]["std_dev"]
        
        # Check if mean_times is not empty
        if mean_times:
            valid_data_found = True
            
            # Pad std_devs array if necessary
            padded_std_devs = np.pad(std_devs, (0, len(mean_times) - len(std_devs)), 'constant')
            
            # Plot with error bars and lines between points
            plt.errorbar(row_counts[:len(mean_times)], mean_times, yerr=padded_std_devs, fmt='-o', label=dataframe.capitalize())
    
    # If no valid data is found, skip plotting
    if not valid_data_found:
        continue
    
    plt.xlabel("Number of Rows")
    plt.ylabel("Mean Execution Time (seconds)")
    plt.title(f"Mean Execution Time for {file} with Different CSV Row Counts")
    plt.xticks(row_counts, labels=[f"{rows/1000:.0f} K" for rows in row_counts], rotation=45)
    
    plt.legend()
    
    plt.tight_layout()
    plot_file_name = os.path.join("plots", f"{file[:-3]}_plot.png")
    plt.savefig(plot_file_name)
    plt.close()