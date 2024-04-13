# cylonplus
High-Performance Distributed Data frames for Machine Learning/Deep Learning Model

## Testing Specific Pandas Performances to the GCylon Dataframe
Each Pandas dataframe-Cudf, Legate, Parallel, and Normal-have their own respective 
folders for graphing comparisons in runtime. 
The tests currently include:
- Strong vs Weak Scaling
- Performance at varying file sizes

Each of the different Pandas folders contain their respective READMEs to run the test files.

## Installation instructions
```
ssh your_computing_id@gpusrv08 -J your_computing_id@portal.cs.virginia.edu
git clone https://github.com/arupcsedu/cylonplus.git
cd cylonplus
module load anaconda3

conda create -n cyp-venv python=3.11
conda activate cyp-venv

conda install pytorch torchvision torchaudio pytorch-cuda=12.1 -c pytorch -c nvidia
DIR=/u/djy8hg/anaconda3/envs/cyp-venv 

export CUDA_HOME=$DIR/bin
export PATH=$DIR/bin:$PATH LD_LIBRARY_PATH=$DIR/lib:$LD_LIBRARY_PATH PYTHONPATH=$DIR/lib/python3.11/site-packages 

pip install petastorm

cd src/model
python multi-gpu-cnn.py
```
