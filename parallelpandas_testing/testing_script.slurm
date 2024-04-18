#!/bin/bash
#SBATCH --job-name=parp_testing           # Job name
#SBATCH --output=parp_test.log     # Output file
#SBATCH --error=parp_test.log      # Error file
#SBATCH --partition=gpu         # Partition (queue) name
#SBATCH --nodes=1                   # Number of nodes
#SBATCH --ntasks=4                  # Number of tasks (divide your total CPUs)
#SBATCH --cpus-per-task=4           # Number of CPU cores per task (total CPUs divided by tasks)
#SBATCH --mem=4G                    # Memory per node
#SBATCH --time=00:30:00             # Walltime

module purge
module load anaconda3

source activate parallel-pandas-test 
echo "Environment activated: $(conda info --envs | grep '^*' | awk '{print $1}')"

python graph_tests.py
