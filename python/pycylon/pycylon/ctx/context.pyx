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

from libcpp.memory cimport shared_ptr, dynamic_pointer_cast
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp cimport bool
from pycylon.ctx.context cimport CCylonContext
from pycylon.api.lib cimport pycylon_unwrap_mpi_config
IF CYTHON_GLOO:
    from pycylon.api.lib cimport pycylon_unwrap_gloo_config

IF CYTHON_UCX & CYTHON_UCC:
    from pycylon.api.lib cimport pycylon_unwrap_ucx_config, pycylon_unwrap_ucc_config, pycylon_wrap_ucc_ucx_communicator
    from pycylon.net.ucc_ucx_communicator cimport CUCXUCCCommunicator, UCXUCCCommunicator
IF CYTHON_UCX:
    from pycylon.api.lib cimport pycylon_wrap_ucx_communicator
    from pycylon.net.ucx_communicator cimport CUCXCommunicator, UCXCommunicator
from pycylon.net.mpi_communicator cimport CMPICommunicator, MPICommunicator
from pycylon.api.lib cimport pycylon_wrap_mci_communicator
from pycylon.net import CommType
from pycylon.net.mpi_config cimport CMPIConfig
from pycylon.net.mpi_config import MPIConfig
from pycylon.net.comm_config cimport CCommConfig
from pycylon.net.comm_config import CommConfig
from pycylon.net.comm_config cimport CommConfig
from pycylon.net.communicator import Communicator
from pycylon.net.communicator cimport CCommunicator


cdef class CylonContext:
    """
    CylonContext is the container which manages the sequential and distributed runitime information.
    In addition to that CylonContext currently accepts a set of defined configurations which defines the
    compute runtimes associated with Python level computations.

    Configurations:

    Compute Engine
    --------------
    key: compute_engine, value: arrow or numpy
    """

    def __cinit__(self, config=None, distributed=None):
        '''
        Initializing the Cylon Context based on the distributed or non-distributed context
        Args:
            config: an object extended from pycylon.net.CommConfig, pycylon.net.MPIConfig for MPI
            backend
            distributed: bool to set distributed setting True or False
        Returns: None

        Examples

        Sequential Programming

        >>> ctx: CylonContext = CylonContext(config=None, distributed=False)

        Distributed Programming

        >>> from pycylon.net import MPIConfig
        >>> mpi_config = MPIConfig()
        >>> ctx: CylonContext = CylonContext(config=mpi_config, distributed=True)

        '''
        if not distributed:
            self.ctx_shd_ptr = CCylonContext.Init()
        else:
            if config is None:
                raise ValueError("No config passed for a distributed context")

            status = CCylonContext.InitDistributed(self.init_dist(config), &self.ctx_shd_ptr)
            if not status.is_ok():
                raise Exception(f"Ctx initialization failed: {status.get_msg().decode()}")

    cdef void init(self, const shared_ptr[CCylonContext] & ctx):
        self.ctx_shd_ptr = ctx

    cdef shared_ptr[CCommConfig] init_dist(self, config):
        if config.comm_type == CommType.MPI:
            return <shared_ptr[CCommConfig]> pycylon_unwrap_mpi_config(config)
        IF CYTHON_GLOO:
            if config.comm_type == CommType.GLOO:
                return <shared_ptr[CCommConfig]> pycylon_unwrap_gloo_config(config)

        if CYTHON_UCX & CYTHON_UCC:
            if  CYTHON_UCX & CYTHON_UCC:
                if config.comm_type == CommType.UCX:
                    return <shared_ptr[CCommConfig]> pycylon_unwrap_ucx_config(config)
                if config.comm_type == CommType.UCC:
                    return <shared_ptr[CCommConfig]> pycylon_unwrap_ucc_config(config)
        else:
            if  CYTHON_UCX & CYTHON_UCC:
                if config.comm_type == CommType.UCX:
                    return <shared_ptr[CCommConfig]> pycylon_unwrap_ucx_config(config)



        raise ValueError(f"Unsupported distributed comm config {config}")

    def get_rank(self) -> int:
        '''
        This is the process id (unique per process)
        :return: an int as the rank (0 for non distributed mode)

        Examples
        --------

        >>> ctx.get_rank()
            1

        '''
        return self.ctx_shd_ptr.get().GetRank()

    def get_world_size(self) -> int:
        '''
        This is the total number of processes joined for the distributed task
        :return: an int as the world size  (1 for non distributed mode)

        Examples
        --------

        >>> ctx.get_world_size()
            4

        '''
        return self.ctx_shd_ptr.get().GetWorldSize()

    def get_communicator(self):
        IF CYTHON_UCX & CYTHON_UCC:
            return pycylon_wrap_ucc_ucx_communicator(dynamic_pointer_cast[CUCXUCCCommunicator, CCommunicator](self.ctx_shd_ptr.get().GetCommunicator()))
        ELIF CYTHON_UCX:
            return pycylon_wrap_ucx_communicator(
                dynamic_pointer_cast[CUCXCommunicator, CCommunicator](self.ctx_shd_ptr.get().GetCommunicator()))
        return pycylon_wrap_mci_communicator(dynamic_pointer_cast[CMPICommunicator, CCommunicator](self.ctx_shd_ptr.get().GetCommunicator()))

    def finalize(self):
        '''
        Gracefully shuts down the context by closing any distributed processes initialization ,etc
        :return: None

        Examples
        --------

        >>> ctx.finalize()

        '''
        self.ctx_shd_ptr.get().Finalize()

    def barrier(self):
        '''
        Calling barrier to sync workers

        Examples
        --------

        >>> ctx.barrier()
        '''
        self.ctx_shd_ptr.get().Barrier()

    def add_config(self, key: str, value: str):
        """
        Adding a configuration
        @param key: str
        @param value: str

        Example
        -------

        >>> ctx: CylonContext = CylonContext(config=None, distributed=False)
        >>> ctx.add_config("compute_engine", "arrow")
        """
        self.ctx_shd_ptr.get().AddConfig(key.encode(), value.encode())

    def get_config(self, key, default):
        """
        Retrieving a configuration
        @param key: str
        @param default: str (default value if the key is not present)
        @return: str
        """
        return self.ctx_shd_ptr.get().GetConfig(key.encode(), default.encode()).decode()
