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

import numpy as np
cimport numpy as c_np
from pycylon.net.txrequest cimport CTxRequest


cdef class TxRequest:
    cdef CTxRequest *thisptr
    cdef public int[:] buf_val_int
    cdef public float[:] buf_val_float
    cdef public char[:] buf_val_char
    cdef public bytes[:] buf_val_bytes
    cdef public double[:] buf_val_double
    cdef public long[:] buf_val_long
    cdef public buf_val_type

    cdef public np_buf_val
    cdef public np_head_val

    def __cinit__(self, int tgt, c_np.ndarray buf, int len,
                  c_np.ndarray[int, ndim=1, mode="c"] head, int hLength):
        '''
        Initialized the PyCylon TxRequest
        :param tgt: passed as an int; the target of communication
        :param buf: passed as an numpy array; the buf that is passed to the communication
        :param len: passed as an int; the length of the target buf
        :param head: passed as an int numpy array; the header of the passed request
        :param hLength: passed as an int; the length of the header
        :return: None
        '''
        if tgt != -1 and buf is None and len == -1 and head is None and hLength == -1:
            self.thisptr = new CTxRequest(tgt)
            self.thisptr.target = tgt
        if tgt != -1 and buf is not None and len != -1 and head is not None and hLength != -1:

            self.np_head_val = head
            self.buf_val_type = buf.dtype

            if self.buf_val_type == np.int16:
                self.buf_val_int = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_int[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_int[0]

            if self.buf_val_type == np.int32:
                self.buf_val_int = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_int[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_int[0]

            if self.buf_val_type == np.int64:
                self.buf_val_long = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_long[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_long[0]

            if self.buf_val_type == np.float32:
                self.buf_val_float = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_float[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_float[0]

            if self.buf_val_type == float:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_double[0]

            if self.buf_val_type == np.float64:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_double[0]

            if self.buf_val_type == np.double:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len, &head[0], hLength)
                self.thisptr.buffer = &self.buf_val_double[0]

            # if self.buf_val_type == np.long:
            #     self.buf_val_long = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new CTxRequest(tgt, &self.buf_val_long[0], len, &head[0], hLength)
            #     self.thisptr.buffer = &self.buf_val_long[0]

            # if self.buf_val_type == np.char:
            #     self.buf_val_char = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new _TxRequest(tgt, &self.buf_val_char[0], len, &head[0], hLength)

            # if self.buf_val_type == np.bytes:
            #     self.buf_val_bytes = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new _TxRequest(tgt, &self.buf_val_bytes[0], len, &head[0], hLength)

            self.np_buf_val = buf

            self.thisptr.target = tgt
            # if self.buf_val_int:
            #     self.thisptr.buffer = &self.buf_val_int[0]
            # if self.buf_val_float:
            #     self.thisptr.buffer = &self.buf_val_float[0]
            self.thisptr.length = len
            self.thisptr.header = &head[0]
            self.thisptr.headerLength = hLength

        if tgt != -1 and buf is not None and len != -1 and head is None and hLength == -1:

            self.np_head_val = head
            self.buf_val_type = buf.dtype
            self.np_buf_val = buf

            if self.buf_val_type == np.int16:
                self.buf_val_int = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_int[0], len)
                self.thisptr.buffer = &self.buf_val_int[0]

            if self.buf_val_type == np.int32:
                self.buf_val_int = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_int[0], len)
                self.thisptr.buffer = &self.buf_val_int[0]

            if self.buf_val_type == np.int64:
                self.buf_val_int = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_int[0], len)
                self.thisptr.buffer = &self.buf_val_int[0]

            if self.buf_val_type == np.float32:
                self.buf_val_float = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_float[0], len)
                self.thisptr.buffer = &self.buf_val_float[0]

            if self.buf_val_type == float:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len)
                self.thisptr.buffer = &self.buf_val_double[0]

            if self.buf_val_type == np.float64:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len)
                self.thisptr.buffer = &self.buf_val_double[0]

            if self.buf_val_type == np.double:
                self.buf_val_double = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
                self.thisptr = new CTxRequest(tgt, &self.buf_val_double[0], len)
                self.thisptr.buffer = &self.buf_val_double[0]

            # if self.buf_val_type == np.long:
            #     self.buf_val_long = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new CTxRequest(tgt, &self.buf_val_long[0], len)
            #     self.thisptr.buffer = &self.buf_val_long[0]

            # if self.buf_val_type == np.char:
            #     self.buf_val_char = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new _TxRequest(tgt, &self.buf_val_char[0], len)

            # if self.buf_val_type == np.bytes_:
            #     self.buf_val_bytes = buf#np.ndarray(buffer=buf.data, dtype=buf.dtype, ndim=ndim)
            #     self.thisptr = new _TxRequest(tgt, &self.buf_val_bytes[0], len)

            self.thisptr.target = tgt
            self.thisptr.length = len
            self.np_buf_val = buf

    def __dealloc__(self):
        del self.thisptr

    @property
    def target(self):
        '''
        provided as a property for user API
        :return: target
        '''
        return self.thisptr.target

    @property
    def length(self):
        '''
        provided as a property for user API
        :return: length of the buffer
        '''
        return self.thisptr.length

    @property
    def buf(self):
        return self.np_buf_val

    @property
    def header(self):
        '''
        provided as a property for user API
        :return: the header
        '''
        return self.np_head_val

    @property
    def headerLength(self):
        '''
        provided as a property for user API
        :return: header length
        '''
        return self.thisptr.headerLength

    def to_string(self, data_type, depth):
        '''
        provided as a property for user API
        :param data_type: data type of the buffer
        :param depth: depth of the buffer
        :return: stringify the TxRequest
        '''
        self.thisptr.to_string(data_type, depth)

