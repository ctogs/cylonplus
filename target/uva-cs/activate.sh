# export SCRATCH=/scratch/$USER/workdir
# export PROJECT=/scratch/$USER/workdir/cylonplus
# export PYTHON_DIR=$PROJECT/CYLONPLUS
# export CUDA_HOME=$PYTHON_DIR/bin 
# export PATH=$PYTHON_DIR/bin:$PATH
# export LD_LIBRARY_PATH=$PYTHON_DIR/lib:$LD_LIBRARY_PATH
# export PYTHONPATH=$PYTHON_DIR/lib/python3.11/site-packages 

module purge
module load anaconda3

source activate gcylon_dev