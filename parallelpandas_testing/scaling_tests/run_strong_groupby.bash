#!/bin/bash
csv_file1="../data/benchmarking_data_100000000.csv"
id="Pregnancies"  # Replace 'id' with the actual column name used for join

# Test for different CPU counts
for n_cpu in 1 2 4 8 16 32; do
    echo "Running groupby with $n_cpu CPUs..."
    python scaling_tests/strong_groupby.py $csv_file1 $csv_file2 $id $n_cpu
done
