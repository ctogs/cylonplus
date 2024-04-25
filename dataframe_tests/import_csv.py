import pandas as pd

# Define the sizes of the benchmarking files in number of rows
file_sizes = [10000, 500000, 1000000, 2500000, 5000000, 10000000, 25000000, 50000000, 75000000, 100000000] 

# Benchmarking data
data_URL = "https://raw.githubusercontent.com/keitazoumana/Experimentation-Data/main/diabetes.csv"

# Load the original data
original_data = pd.read_csv(data_URL)

# Repeat each row of the original data to create larger datasets
for size in file_sizes:
    # Calculate the number of times to repeat each row
    repeats = size // len(original_data)
    
    benchmarking_df = original_data.loc[original_data.index.repeat(repeats)]
    
    file_name = f"./benchmarking_data_varying_size/benchmarking_data_{size}.csv"
    benchmarking_df.to_csv(file_name, index=False)
    
    print(f"CSV file created: {file_name}")