# ParallelPandas Test Environment Setup

Follow these steps to set up a Conda environment and install the required libraries for the ParallelPandas test.

## Step 1: Create Conda Environment and Install Libraries

1. Navigate to the `spark_testing` folder.
2. Create a Conda environment using the following command:

    ```bash
    conda create -n spark-test python=3.10
    ```

3. Activate the Conda environment:

    ```bash
    conda activate spark-test
    ```

4. Install the libraries listed in the `requirements.txt` file:

    ```bash
    pip install -r requirements.txt
    ```

## Import the relevant data for scaling tests
Run the import_csv.py file with the conda environment activated to import the relevant data. To save space later on, you can delete these files after the tests are run because they can always be redownloaded.:
```bash
python import_csv.py #WILL CHANGE THIS
```