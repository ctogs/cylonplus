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

from libc.stdint cimport uint8_t
from libc.stdint cimport int32_t
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from pyarrow.lib cimport CBuffer as ArrowCBuffer

from pycylon.ctx.context cimport CCylonContext
from pycylon.common.status cimport CStatus

cdef extern from "cylon/net/mpi/mpi_operations.hpp" namespace "cylon::mpi":
    CStatus AllGatherArrowBuffer(const shared_ptr[ArrowCBuffer] &buf,
                                 const shared_ptr[CCylonContext] &ctx_srd_ptr,
                                 vector[shared_ptr[ArrowCBuffer]] &buffers);

    CStatus GatherArrowBuffer(const shared_ptr[ArrowCBuffer] &buf,
                              int32_t gather_root,
                              const shared_ptr[CCylonContext] &ctx_srd_ptr,
                              vector[shared_ptr[ArrowCBuffer]] &buffers);

    CStatus BcastArrowBuffer(shared_ptr[ArrowCBuffer] &buf,
                             int32_t bcast_root,
                             const shared_ptr[CCylonContext] &ctx_srd_ptr);

