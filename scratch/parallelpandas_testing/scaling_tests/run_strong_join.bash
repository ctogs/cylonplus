#!/bin/bash
csv_file1="../data/benchmarking_data_10000.csv"
csv_file2="../data/benchmarking_data_500000.csv"
join_column="Pregnancies"  # Replace 'id' with the actual column name used for join

# Test for different CPU counts
for n_cpu in 1 2 4 8 16; do
    echo "Running join with $n_cpu CPUs..."
    python scaling_tests/strong_join.py $csv_file1 $csv_file2 $join_column $n_cpu
done
