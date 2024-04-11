##
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

'''
# running this example with 2 mpi workers (-n 2) on the local machine:
mpirun -n 2 --mca opal_cuda_support 1 \
    python python/pygcylon/examples/comms.py

# running this example with ucx enabled:
mpirun -n 2 --mca opal_cuda_support 1 \
    --mca pml ucx --mca osc ucx \
    python python/pygcylon/examples/comms.py

# running this example with ucx and infiniband enabled:
mpirun -n 4 --mca opal_cuda_support 1 \
    --mca pml ucx --mca osc ucx \
    --mca btl_openib_allow_ib true \
    python python/pygcylon/examples/comms.py
'''

import cupy as cp
import pycylon as cy
import pygcylon as gcy

env: cy.CylonEnv = cy.CylonEnv(config=cy.MPIConfig(), distributed=True)
print("CylonContext Initialized: My rank: ", env.rank)


def gen_df():
    start = 100 * env.rank
    row_count = 10 + 10 * env.rank
    df = gcy.DataFrame({'first': cp.random.randint(start, start + 10 * row_count, row_count),
                       'second': cp.random.randint(start, start + 10 * row_count, row_count)})
    row_counts = df.row_counts_for_all(env)
    if env.rank == 0:
        print("initial row counts: ", row_counts)
    return df, row_counts


######################
# even repartitioning
def repartition(df):
    repartedDF = df.repartition(env=env)

    print("repartitioned row counts: ", repartedDF.row_counts_for_all(env))
    out_file = "repartitioned" + str(env.rank) + ".csv"
    repartedDF.to_cudf().to_csv(out_file)
    print("has written the repartitioned DataFrame to the file:", out_file)
    return repartedDF


######################
# repartitioning with row_counts per partition
def repartition(df, new_row_counts):
    repartedDF = gcy.comms.repartition(df, env=env, rows_per_partition=new_row_counts)

    print("repartitioned row counts: ", repartedDF.row_counts_for_all(env))
    out_file = "repartitioned" + str(env.rank) + ".csv"
    repartedDF.to_cudf().to_csv(out_file)
    print("has written the repartitioned DataFrame to the file:", out_file)
    return repartedDF


######################
# generate partition sizes randomly
def gen_random_sizes(row_counts):
    avg = int(sum(row_counts) / len(row_counts))
    new_row_counts = [avg] * len(row_counts)

    import random
    from random import randrange
    # we need to generate the same random number in all workers
    random.seed(0)
    mid = int(len(new_row_counts)/2)
    for i in range(mid):
      rnd = randrange(avg)
      new_row_counts[i] += rnd
      new_row_counts[mid + i] -= rnd

    new_row_counts[len(new_row_counts) - 1] += sum(row_counts) - sum(new_row_counts)
    return new_row_counts


######################
def gather(df):
    gatheredDF = gcy.comms.gather(df, env=env, gather_root=1)

    print("gathered row counts: ", gatheredDF.row_counts_for_all(env))
    out_file = "gathered" + str(env.rank) + ".csv"
    gatheredDF.to_cudf().to_csv(out_file)
    print("has written the gathered DataFrame to the file:", out_file)
    return gatheredDF


######################
def allgather(df):
    gatheredDF = df.allgather(env=env)

    print("allgathered row counts: ", gatheredDF.row_counts_for_all(env))
    out_file = "allgathered" + str(env.rank) + ".csv"
    gatheredDF.to_cudf().to_csv(out_file)
    print("has written all gathered DataFrame to the file:", out_file)
    return gatheredDF


######################
def broadcast(df, root):
    bcastDF = gcy.comms.broadcast(df, env=env, root=root)

    if env.rank == root:
        print("broadcasted the partition: ", root, "with row_count: ", len(df.to_cudf()))
    print("broadcasted dataframe row counts: ", bcastDF.row_counts_for_all(env))
    out_file = "broadcasted" + str(env.rank) + ".csv"
    bcastDF.to_cudf().to_csv(out_file)
    print("has written the broadcasted DataFrame to the file:", out_file)
    return bcastDF


######################################################
df, row_counts = gen_df()

# even repartitioning
# repartition(df)

# repartitioning with user given partition sizes
# new_row_counts = gen_random_sizes(row_counts)
# repartedDF = repartition(df, new_row_counts)

# gathering all dataframe to a worker
# gather(df)

# all gathering all dataframes to all workers
allgather(df)

# broadcasting a partition of a distributed dataframe to all workers
# broadcast(df, 0)

env.finalize()
print("after finalize from the rank:", env.rank)
