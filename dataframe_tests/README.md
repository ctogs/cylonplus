# Project README

This repository contains instructions for setting up a Conda environment, creating a directory, importing data, and running a Python script using Slurm.

## Setting up Conda Environment

1. Create a Conda environment using the provided `environment.yml` file:

   ```bash
   conda env create -f environment.yml
   ```

2. Activate the Conda environment (Optional, Slurm script should activate it automatically):

   ```bash
   conda activate dataframe_testing
   ```

## Creating Data Directory

Create a directory named "data":

```bash
mkdir data
```

## Importing Data

Use the `import_csv.py` script to import data into the "data" directory:

```bash
python import_csv.py
```

## Running Python Script with Slurm

1. Ensure Slurm is installed and configured on your system. 

2. Use the pre-created slurm script called run_tests.slurm. Or, create a custom slurm script.

3. Submit the Slurm script to the queue:

   ```bash
   sbatch run_tests.slurm
   ```

4. Monitor the progress of your job using Slurm commands like `squeue` or `sacct`.

5. Once the slurm script is done, plots of performance comparison should be seen in the "plots" folder