import pandas as pd

REPEATS = 100

data_URL = "https://raw.githubusercontent.com/keitazoumana/Experimentation-Data/main/diabetes.csv"

original_data = pd.read_csv(data_URL)

# Duplicate each row 100000 times
benchmarking_df = original_data.loc[original_data.index.repeat(REPEATS)]

# Save the result as a CSV file for future use.
file_name = f"data/benchmarking_data_{REPEATS*768}.csv"

benchmarking_df.to_csv(file_name, index=False)