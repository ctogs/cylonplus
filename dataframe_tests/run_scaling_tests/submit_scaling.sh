#!/bin/bash

# Define the pairs as an array where each element is "tasks,cores"
declare -a Configurations=("1,1" "1,2" "1,4" "2,4" "4,4" "4,8" "4,10")

# Loop through each configuration and submit a job
for config in "${Configurations[@]}"
do
    IFS=',' read -ra ADDR <<< "$config"  # Split the string into an array based on comma
    TASKS=${ADDR[0]}
    CORES=${ADDR[1]}
    
    # Submit the job with the current number of tasks and cores per task
    sbatch --ntasks=$TASKS --cpus-per-task=$CORES run_scaling_job.sh
done
