# Legate Core Installation Guide

This guide will walk you through the installation process for Legate Core.

## Step 1: Create Conda Environment

First, clone the Legate Core repository and create a conda environment using the following commands:

```bash
git clone https://github.com/nv-legate/legate.core <legate.core-dir>
cd <legate.core-dir>
./scripts/generate-conda-envs.py --python 3.10 --ctk 12.0.1 --os linux --ucx
conda env create -n legate -f environment-test-linux-py310-cuda12.0.1-ucx.yaml
```

## Step 2: Build Legate Libraries

After setting up the conda environment, navigate to the legate_core directory and build the Legate libraries:

```bash
cd legate_core
LEGION_DIR=legion quickstart/build.sh
cd cunumeric
quickstart/build.sh
```

## Step 3: Run Legate Tests

Finally, run the Legate tests using the following command, replacing placeholders with appropriate values:

```bash
quickstart/run.sh <num-nodes> <legate-args> <py-program> <program-args>
```

Remember to replace `<legate.core-dir>`, `<num-nodes>`, `<legate-args>`, `<py-program>`, and `<program-args>` with your specific directory and arguments.

This README provides step-by-step instructions for installing Legate Core and running tests. If you encounter any issues, refer to the [Legate Core documentation](https://legatecore.readthedocs.io/en/latest/).