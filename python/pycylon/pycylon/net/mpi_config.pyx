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

from pycylon.net.comm_config cimport CommConfig
from pycylon.net.mpi_config cimport CMPIConfig
cimport mpi4py.MPI as MPI

from mpi4py.MPI import COMM_NULL

'''
MPIConfig Type mapping from libCylon to PyCylon
'''
cdef class MPIConfig(CommConfig):
    def __cinit__(self, comm = COMM_NULL):
        self.mpi_config_shd_ptr = CMPIConfig.Make((<MPI.Comm> comm).ob_mpi)

    @property
    def comm_type(self):
        return self.mpi_config_shd_ptr.get().Type()
