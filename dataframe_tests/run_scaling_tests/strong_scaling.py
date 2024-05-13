import os
import pandas as pd
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import sys
import csv

def update_csv(csv_path, data, num_cores):
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        # Initialize with columns for both mean and std_dev
        df = pd.DataFrame(columns=['Test File', '1', '1_std', '2', '2_std', '4', '4_std', '8', '8_std', '16', '16_std', '32', '32_std', '40', '40_std'])
    
    for framework, tests in data.items():
        for test_file, results in tests.items():
            mean_value = results['mean'][0]  # Assume one mean value per entry
            std_dev_value = results['std_dev'][0]  # Assume one std_dev value per entry
            
            test_label = test_file.split('/')[1].replace('_tests', '')
            
            # Handle updating or creating new rows
            if not df[df['Test File'] == test_label].empty:
                df.loc[df['Test File'] == test_label, str(num_cores)] = mean_value
                df.loc[df['Test File'] == test_label, f"{str(num_cores)}_std"] = std_dev_value
            else:
                new_row = {'Test File': test_label, str(num_cores): mean_value, f"{str(num_cores)}_std": std_dev_value}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    
    df.to_csv(csv_path, index=False)

def main():
    num_tasks = int(sys.argv[1])
    num_cores_per_task = int(sys.argv[2])
    print(f"Number of tasks: {num_tasks}, Cores per task: {num_cores_per_task}")

    # List of dataframe folders and corresponding test files
    dataframes = {
        "pandas": "../pandas_tests",
        "parallelpandas": "../parallelpandas_tests",
        # "dask": "../dask_tests",
        # "spark": "spark_tests"
        # Add more dataframes here if needed
    }

    # List of Python files for each dataframe
    example_files = {
        "pandas": [
            # "test_groupby.py",
            # "test_shuffle.py"
            "test_io.py",
            # Add more test files specific to pandas if needed
        ],
        "parallelpandas": [
            # "test_groupby.py",
            # "test_shuffle.py"
            "test_io.py",
            # Add more test files specific to parallelpandas if needed
        ],
        # "dask": [
        #     # "test_groupby.py",
        #     # "test_shuffle.py"
        #     "test_io.py",
        #     # Add more test files specific to parallelpandas if needed
        # ],
        # Add more dataframes here with their corresponding test files
        # "spark": [
        #     # "test_groupby.py",
        #     # "test_io.py",
        #     "test_shuffle.py",
        # ],
    }

    # Dictionary of commands to run for each test file
    test_files = {
        # "test_groupby.py": ["Pregnancies"],
        "test_io.py": [""],
        # "test_shuffle.py": [""],
        # Add more test files with their commands if needed
    }

    # List of CSV files
    csv_files = [
        # "../../data/benchmarking_data_10000.csv",
        # "../../data/benchmarking_data_5000000.csv",
        # "../../data/benchmarking_data_10000000.csv",
        # "../../data/benchmarking_data_25000000.csv",
        "../../data/benchmarking_data_50000000.csv",
        # "../../data/benchmarking_data_75000000.csv",
        # "../../data/benchmarking_data_100000000.csv",
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
                    print("Running test: python " + str(file) + " " + str(csv_file) + " " + str(command) + " " + str(num_tasks) + " " + str(num_cores_per_task))
                    print(f"\n {['python', file, csv_file] + [command]} \n")
                    result = subprocess.run(
                        ["python", file, csv_file] + [command] + [str(num_tasks), str(num_cores_per_task)],
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

    filename = 'testio_ss_cpu.csv'
    num_cores = num_tasks * num_cores_per_task

    update_csv(filename, execution_times, num_cores)

    print(f"Data successfully` written to {filename}")

if __name__ == "__main__":
    main()