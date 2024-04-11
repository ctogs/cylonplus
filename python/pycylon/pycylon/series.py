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


import pandas as pd
from pycylon.data.column import Column


class Series:
    def __init__(self, series_id: str = None, data=None):
        self._id = series_id
        self._column = Column(data)

    @property
    def id(self):
        return self._id

    @property
    def data(self):
        return self._column.data

    @property
    def dtype(self):
        return self._column.dtype

    @property
    def shape(self):
        return len(self._column.data)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self._column.data[item]
        raise ValueError('Indexing in series is only based on numeric indices and slices')

    def __repr__(self):
        return pd.Series(self.data).__repr__()
