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

from pycylon.data.table import Table
from pycylon.data.column import Column
from pycylon.data.scalar import Scalar
from pycylon.ctx.context import CylonContext
from pycylon.types import (int8, int16, int32, int64,
                           uint8, uint16, uint32, uint64,
                           float, double, string, bool,
                           half_float, binary, fixed_sized_binary,
                           date32, date64, timestamp, time32, time64,
                           interval, decimal, list, fixed_sized_list,
                           extension, duration
                           )
from pycylon.data.data_type import DataType
from pycylon.series import Series
from pycylon.frame import DataFrame
from pycylon.frame import CylonEnv
from pycylon.frame import read_csv
from pycylon.frame import concat
from pycylon.net import MPIConfig
from pycylon.distributed_io import read_csv_dist, write_csv_dist, read_json_dist, write_json_dist, read_parquet_dist, \
    write_parquet_dist
from pycylon.util.logging import log_level, disable_logging

import os

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

lg_level = os.getenv('CYLON_LOG_LEVEL')

if lg_level is None:
    disable_logging()
else:
    log_level(int(lg_level))
