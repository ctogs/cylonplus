#!/bin/bash
#SBATCH --job-name=my_python_job
#SBATCH --error=my_python_job_%j.err
#SBATCH --time=01:00:00   # Adjust time as necessary
#SBATCH --mem=2G          # Adjust memory as necessary

module load python  # Assuming Python needs to be loaded, adjust according to your setup

# The script assumes $SLURM_NTASKS and $SLURM_CPUS_PER_TASK are set by sbatch
echo "Running srun with NTASKS=$SLURM_NTASKS and CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"
srun python strong_scaling.py $SLURM_NTASKS $SLURM_CPUS_PER_TASK
