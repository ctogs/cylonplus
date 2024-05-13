import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV data into a DataFrame
df = pd.read_csv('testio_ss_cpu.csv')

# Prepare the plot
fig, ax = plt.subplots(figsize=(10, 6))

# Define the core configurations and their corresponding columns in the DataFrame
core_configs = ['1', '2', '4', '8', '16', '32', '40']
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']  # Different colors for each framework

# Iterate over each row (each framework)
for index, row in df.iterrows():
    means = []
    errors = []
    for core in core_configs:
        means.append(row[f'{core}'])
        errors.append(row[f'{core}_std'])

    # Plot the mean values with error bars
    ax.errorbar(core_configs, means, yerr=errors, fmt='-o', label=row['Test File'], capsize=5, linestyle='-', marker='o', color=colors[index % len(colors)])

# Customize the plot
ax.set_xlabel('Number of Cores')
ax.set_ylabel('Execution Time (s)')
ax.set_title('Strong Scaling I/O Performance for 50M Row CSV')
ax.set_xticks(core_configs)  # Set x-ticks to be the core configurations
ax.legend(title='Frameworks')

# Show the plot
plt.tight_layout()
plt.savefig('testio_ss_cpu_results.png')
plt.close()
