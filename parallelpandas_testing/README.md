# ParallelPandas Test Environment Setup

Follow these steps to set up a Conda environment and install the required libraries for the ParallelPandas test.

## Step 1: Create Conda Environment and Install Libraries

1. Navigate to the parallelpandas-test folder.
2. Create a Conda environment using the following command:

    ```bash
    cd parallelpandas-test
    conda create -n parallelpandas-test python=3.8
    ```

3. Activate the Conda environment:

    ```bash
    conda activate parallelpandas-test
    ```

4. Install the libraries listed in the requirements.txt file:

    ```bash
    pip install -r requirements.txt
    ```

## Step 2: Activate Conda Environment

Activate the Conda environment using the following command:

```bash
conda activate parallelpandas-test