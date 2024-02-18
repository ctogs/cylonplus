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


from __future__ import annotations

from collections.abc import Iterable
from copy import copy
from typing import Hashable, List, Dict, Optional, Sequence, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pycylon as cn
import pycylon.data as pcd
from pandas.core.dtypes.common import infer_dtype_from_object
from pycylon import CylonContext
from pycylon import Series
from pycylon.data.table import SortOptions
from pycylon.index import RangeIndex, CategoricalIndex
from pycylon.io import CSVReadOptions
from pycylon.io import CSVWriteOptions

from pycylon.indexing.cyindex import IndexingType
from pycylon.indexing.cyindex import PyLocIndexer

DEVICE_CPU = "cpu"


# Data loading Functions
def read_csv(filepath: str, use_threads=True, names=None, sep=",", block_size: int = 1 << 20,
             skiprows=0,
             ignore_emptylines=True, na_values=None, slice=False, env=None):
    """
    Read a comma-separated values (csv) file into DataFrame.

    Parameters
    ----------
    filepath : A valid str path to the file
    sep : str, default ,
        Delimiter to use. 
    names : array-like, optional
        List of column names to use. If the file contains a header row,
        then you should explicitly pass ``header=0`` to override the column names.
        Duplicates in this list are not allowed.
    block_size : int, default 1MB
        Arrow block size to be used when chunking the final Cylon table
    skiprows : int, optional, default 0
        Line numbers to skip (0-indexed) or number of lines to skip (int)
        at the start of the file.
    ignore_emptylines: bool, default True
        Whether to keep or ignore empty lines in the csv file
    na_values : list-like, optional
        Additional strings to recognize as NA/NaN.
    """
    read_config = CSVReadOptions().use_threads(
        use_threads).block_size(block_size).with_delimiter(sep).skip_rows(skiprows).slice(slice)

    if ignore_emptylines:
        read_config.ignore_emptylines()

    if na_values is not None:
        read_config.na_values(na_values)

    if names is not None:
        read_config.use_cols(names)

    ctx = CylonContext(
        config=None, distributed=False)

    if env is not None:
        ctx = env.context

    table = pcd.csv.read_csv(ctx, filepath, read_config)

    return DataFrame(table)


class CylonEnv(object):

    def __init__(self, config=None, distributed=True) -> None:
        self._context = CylonContext(config, distributed)
        self._distributed = distributed
        self._finalized = False

    @property
    def context(self) -> CylonContext:
        return self._context

    @property
    def rank(self) -> int:
        return self._context.get_rank()

    @property
    def world_size(self) -> int:
        return self._context.get_world_size()

    @property
    def is_distributed(self) -> bool:
        return self._distributed

    def finalize(self):
        if not self._finalized:
            self._finalized = True
            self._context.finalize()

    def barrier(self):
        self._context.barrier()


class GroupByDataFrame(object):
    def __init__(self, df: DataFrame, by=None, groupby_type: str = 'hash') -> None:
        super().__init__()
        self.df = df
        self.by = by
        self.by_diff = set(df.columns) - set(by)
        self.groupby_type = groupby_type

    def __do_groupby(self, op_dict) -> DataFrame:
        return DataFrame(
            self.df.to_table().groupby(self.by, op_dict, groupby_type=self.groupby_type))

    def __apply_on_remaining_columns(self, op: str) -> DataFrame:
        op_dict = {}
        for c in self.by_diff:
            op_dict[c] = op
        return self.__do_groupby(op_dict)

    def min(self) -> DataFrame:
        """
        Apply min operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("min")

    def max(self) -> DataFrame:
        """
        Apply max operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("max")

    def sum(self) -> DataFrame:
        """
        Apply sum operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("sum")

    def count(self) -> DataFrame:
        """
        Apply count operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("count")

    def mean(self) -> DataFrame:
        """
        Apply mean operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("mean")

    def std(self) -> DataFrame:
        """
        Apply standard deviation operator on each remaining column  which has not been used for grouping
        """
        return self.__apply_on_remaining_columns("std")

    def agg(self, dic: dict) -> DataFrame:
        """
        Apply different aggregation operations on each remainign column
        which has not been used for grouping

        Args:
            dic : A dictionary specifying aggregation operation for each column
        """
        return self.__do_groupby(dic)


class DataFrame(object):

    def __init__(self, data=None, index=None, columns=None, copy=False):
        """
        Construct a Cylon DataFrame

        Parameters
        ----------
        data : Python list, ndarray, Pandas Dataframe, Arrow Table or a Cylon Table
        columns : Optional set of column names
        copy : By default, Cylon will try not to copy data when constructing a datframe from ndarray, 
            Pandas Dataframe, Arrow Table or a Cylon Table. This behavior can be forcefully overridden by setting this flag.

        Returns
        -------
        DataFrame
        """
        self._index = None
        self._columns = []

        self._table = self._initialize_dataframe(
            data=data, index=index, columns=columns, copy=copy)

        # temp workaround for indexing requirement of dataframe api
        self._index_columns = []

        self._device = DEVICE_CPU

    def to_cpu(self):
        """
        Move the dataframe from it's current device to random access memory
        """
        pass

    def to_device(self, device=None):
        """
        Move the dataframe from it's current device to the specified device
        """
        pass

    def is_cpu(self):
        return self._device == DEVICE_CPU

    def is_device(self, device):
        return self._device == device

    def _change_context(self, env: CylonEnv) -> DataFrame:
        """
        This is a temporary function to make the DataFrame backed by a Cylon Table with a different context.
        This should be removed once C++ support Tables which are independent from Contexts
        """
        self._table = self._initialize_dataframe(
            data=self._table.to_arrow(), index=self._index, columns=self._columns, copy=False,
            context=env.context)
        return self

    def _initialize_dataframe(self, data=None, index=None, columns=None, copy=False,
                              context=CylonContext(config=None, distributed=False)):
        rows = 0
        cols = 0
        self._table = None
        if copy:
            data = self._copy(data)

        if isinstance(data, List):
            # load from List or np.ndarray
            if isinstance(data[0], List):
                rows = len(data[0])
                cols = len(data)
                if not columns:
                    columns = self._initialize_columns(
                        cols=cols, columns=columns)
                return cn.Table.from_list(context, columns, data)
            elif isinstance(data[0], np.ndarray):
                # load from List of np.ndarray
                cols = len(data)
                rows = data[0].shape[0]
                if not columns:
                    columns = self._initialize_columns(
                        cols=cols, columns=columns)
                return cn.Table.from_numpy(context, columns, data)
            else:
                # load from List
                rows = len(data)
                cols = 1
                if not columns:
                    columns = self._initialize_columns(
                        cols=cols, columns=columns)
                return cn.Table.from_list(context, columns, data)
        elif isinstance(data, pd.DataFrame):
            # load from pd.DataFrame
            rows, cols = data.shape
            if columns:
                from pycylon.util.pandas.utils import rename_with_new_column_names
                columns = rename_with_new_column_names(data, columns)
                data = data.rename(columns=columns, inplace=True)
            return cn.Table.from_pandas(context, data)
        elif isinstance(data, dict):
            # load from dictionary
            _, data_items = list(data.items())[0]
            rows = len(data_items)
            return cn.Table.from_pydict(context, data)
        elif isinstance(data, pa.Table):
            # load from pa.Table
            rows, cols = data.shape
            return cn.Table.from_arrow(context, data)
        elif isinstance(data, Series):
            # load from PyCylon Series
            # cols, rows = data.shape
            # columns = self._initialize_columns(cols=cols, columns=columns)
            return NotImplemented
        elif isinstance(data, cn.Table):
            if columns:
                from pycylon.util.pandas.utils import rename_with_new_column_names
                columns = rename_with_new_column_names(data, columns)
                data = data.rename(columns=columns)
            return data
        elif not data:
            return cn.Table.from_pydict(context, {})
        else:
            raise ValueError(f"Invalid data structure, {type(data)}")

    def _initialize_dtype(self, dtype):
        raise NotImplemented(
            "Data type forcing is not implemented, only support inferring types")

    def _initialize_columns(self, cols, columns):
        if columns is None:
            return [str(i) for i in range(cols)]
        else:
            if isinstance(columns, Iterable):
                if len(columns) != cols:
                    raise ValueError(f"data columns count: {cols} and column names count "
                                     f"{len(columns)} not equal")
                else:
                    return columns

    def _initialize_index(self, index, rows):
        if index is None:
            self._index = RangeIndex(start=0, stop=rows)
        else:
            if isinstance(index, CategoricalIndex):
                # check the validity of provided Index
                pass
            elif isinstance(index, RangeIndex):
                # check the validity of provided Index
                pass

    def _copy(self, obj):
        return copy(obj)

    @property
    def shape(self):
        return self._table.shape

    @property
    def columns(self) -> List[str]:
        return self._table.column_names

    def to_pandas(self) -> pd.DataFrame:
        return self._table.to_pandas()

    def to_numpy(self, order: str = 'F', zero_copy_only: bool = True, writable: bool = False) -> \
            np.ndarray:
        return self._table.to_numpy(order=order, zero_copy_only=zero_copy_only,
                                    writable=writable)

    def to_arrow(self) -> pa.Table:
        return self._table.to_arrow()

    def to_dict(self) -> Dict:
        return self._table.to_pydict()

    def to_table(self) -> cn.Table:
        return self._table

    def to_csv(self, path, csv_write_options: CSVWriteOptions):
        self._table.to_csv(path=path, csv_write_options=csv_write_options)

    def equals(self, df: DataFrame, ordered=True, env: CylonEnv = None):
        '''
        Determine whether two tables are equal.
        Args:
            table: the other table
            ordered: whether we have to maintain the original order when 
            comparing two tables

        Returns: boolean

        Examples
        ----------
        >>> tb1
            1,2
            1,2
            3,4
            5,6

        >>> tb2
            1,2
            1,2
            5,6
            3,4

        >>> tb1.equals(tb2)
            False
        
        >>> tb1.equals(tb2, false)
            True
        '''
        if env is None:
            return self._table.equals(df._table, ordered)
        else:
            self._change_context(env)
            df._change_context(env)
            return self._table.distributed_equals(df._table, ordered)

    def repartition(self, rows_per_partition, receive_build_rank_order=None, env: CylonEnv = None):
        if env != None:
            self._change_context(env)
        df = DataFrame(self._table.repartition(rows_per_partition, receive_build_rank_order))
        return df

    def evenly_partition(self, env: CylonEnv = None):
        if env != None:
            self._change_context(env)
        return self._table.evenly_partition()

    @property
    def iloc(self) -> PyLocIndexer:
        """
        This operator finds value by integer-location based indexing.
        Allowed inputs are:

        - An integer, e.g. ``5``.
        - A list or array of integers, e.g. ``[2, 3, 0]``.
        - A slice object with ints, e.g. ``0:2``.

        Examples
        --------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> df1.iloc[1]
               col-0  col-1
            0      2      5

        >>> df1.iloc[[1,2]]
               col-0  col-1
            0      2      5
            1      3      6

        >>> df1.iloc[1:3]
               col-0  col-1
            0      2      5
            1      3      6
        """
        return self._table.iloc

    def __len__(self):
        """
        This operator returns number of rows in the df.

        Example
        --------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> len(df1)
            3

        """
        return self._table.row_count

    def set_index(self, key, indexing_type: IndexingType = IndexingType.LINEAR, drop: bool = False):
        """
        Set index of the pycylon DataFrame
        Allowed inputs are:

        - A list e.g. ``[i for i in range(len(df))]``
        - A list e.g. ``[0,1,len(df)-1]``

        Examples
        --------

        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6
        >>> df1.set_index([i for i in range(len(df1))])

        or

        >>> df1.set_index([0,1,len(df1)-1])

        """
        return self._table.set_index(key, indexing_type, drop)

    @property
    def index(self):
        """
        Returns the index of the df.

        Example
        --------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> df1.index
            <pycylon.indexing.cyindex.BaseArrowIndex object at 0x7f6cf948ca00>

        """
        return self._table.get_index()

    def get_hash_object(self, index=True, encoding="utf8", hash_key=None, categorize=True):
        """
        Returns a data hash of the df.

        Example
        --------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> df1.get_hash_object()
                                 0
            0   580038878669277522
            1  2529894495349307502
            2  4389717532997776129

        """

        pdf = self.to_pandas()
        hashed_series = pd.util.hash_pandas_object(pdf, index=index, encoding=encoding,
                                                   hash_key=hash_key, categorize=categorize)
        hashed_pdf = hashed_series.to_frame()
        context = CylonContext(config=None, distributed=False)
        return DataFrame(cn.Table.from_pandas(context, hashed_pdf))

    @property
    def values(self) -> np.ndarray:  # pandas series.values and here should be same
        """
        Returns Numpy ndarray object representation of the df.

        Example
        -------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> df1.values
            array([[1, 4],
                   [2, 5],
                   [3, 6]])

        """

        if len(self.columns) >= 2:
            return self.to_numpy()
        else:
            if len(self.columns) == 0:
                return np.empty(self.shape, dtype=float)

            dict = self.to_dict()
            col = self.columns[0]
            return np.array(list(dict[col]))

    @property
    def dtypes(self):
        """
        Return the dtypes in the df as a Dictionary object.

        Example
        -------
        >>> df1
               col-0  col-1
            0      1      4
            1      2      5
            2      3      6

        >>> df1.dtypes
            {'col-0': <class 'numpy.int64'>, 'col-1': <class 'numpy.int64'>}
        """

        schema = self._table.to_arrow().schema
        dict = {}
        types = schema.types
        i = 0
        for value in schema.names:
            dict[value] = pa.DataType.to_pandas_dtype(types[i])
            i += 1

        return dict

    def select_dtypes(self, include=None, exclude=None) -> DataFrame:
        """
        Return a subset of the df's columns based on the column dtypes.

        Parameters
        ----------
        include, exclude : scalar or list-like
            A selection of dtypes or strings to be included/excluded.
            At least one of these parameters must be supplied.

        Returns
        -------
        DataFrame: subset of the frame including the dtypes in ``include`` and
            excluding the dtypes in ``exclude``.

        Raises
        ------
        ValueError
            * If both of ``include`` and ``exclude`` are null
            * If ``include`` and ``exclude`` have overlapping elements
            * If any kind of string dtype is passed in.

        Examples
        --------
        >>> df1
               col-1 col-2
            0   True     b
            1  False     a
            2  False     c

        >>> df1.select_dtypes(include=['int64', 'object'], exclude=['bool'])
                col-2
            0     b
            1     a
            2     c
        >>> df1.select_dtypes(include=['object'])
                col-2
            0     b
            1     a
            2     c
        >>> df1.select_dtypes(exclude=['bool'])
                col-2
            0     b
            1     a
            2     c
        >>> df1.select_dtypes(exclude=['int64'])
                col-1 col-2
            0   True     b
            1  False     a
            2  False     c
        >>> df1.select_dtypes(include=['int64'])
            Empty DataFrame
            Columns: []
            Index: []

        """

        if not isinstance(include, (list, tuple)):
            include = (include,) if include is not None else ()
        if not isinstance(exclude, (list, tuple)):
            exclude = (exclude,) if exclude is not None else ()

        selection = (frozenset(include), frozenset(exclude))

        if not any(selection):
            raise ValueError("at least one of include or exclude must be nonempty")

        include = frozenset(infer_dtype_from_object(x) for x in include)
        exclude = frozenset(infer_dtype_from_object(x) for x in exclude)

        if not include.isdisjoint(exclude):
            raise ValueError(
                f"include and exclude overlap on {(include & exclude)}"
            )

        def extract_unique_dtypes_from_dtypes_set(dtypes_set: FrozenSet[np.generic],
                                                  unique_dtypes: np.ndarray) -> List[
            np.generic]:
            extracted_dtypes = [
                unique_dtype
                for unique_dtype in unique_dtypes
                # error: Argument 1 to "tuple" has incompatible type
                # "FrozenSet[Union[ExtensionDtype, str, Any, Type[str],
                # Type[float], Type[int], Type[complex], Type[bool]]]";
                # expected "Iterable[Union[type, Tuple[Any, ...]]]"
                if issubclass(
                    # here no need to put unique_dtypes.type -> already gets type
                    unique_dtype, tuple(dtypes_set)  # type: ignore[arg-type]
                )
            ]
            return extracted_dtypes

        # get unique dtypes from cylon df - set operation
        unique_set = set(self.dtypes.values())
        unique_dtypes = np.array(list(unique_set), dtype=object)

        if include:
            included_dtypes = extract_unique_dtypes_from_dtypes_set(include, unique_dtypes)

            extracted_columns = [
                column
                for column, dtype in self.dtypes.items()
                if dtype in included_dtypes
            ]

        if exclude:
            if include:
                # include given priority always, so need to execute exclude
                pass
            else:
                excluded_dtypes = extract_unique_dtypes_from_dtypes_set(exclude, unique_dtypes)

                extracted_columns = [
                    column
                    for column, dtype in self.dtypes.items()
                    if dtype not in excluded_dtypes
                ]

        if extracted_columns:
            return DataFrame(self.iloc[:, extracted_columns].to_arrow())
        else:
            return DataFrame()

    def __getitem__(self, item) -> DataFrame:
        """
            This method allows to retrieve a subset of a DataFrane by means of a key
            Args:
                key: a key can be the following
                     1. slice i.e dataframe[1:5], rows 1:5
                     2. int i.e a row index
                     3. str i.e extract the data column-wise by column-name
                     4. List of columns are extracted
                     5. PyCylon DataFrame
            Returns: PyCylon DataFrame

            Examples
            --------
            >>> data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
            >>> df: DataFrame = DataFrame(data)

            >>> df1 = df[1:3]
                col-1  col-2  col-3
                    0      2      6     10
                    1      3      7     11
                    2      4      8     12

            >>> df2 = df['col-1']
                   col-1
                0      1
                1      2
                2      3
                3      4

            >>> df3 = df[['col-1', 'col-2']]
                   col-1  col-2
                0      1      5
                1      2      6
                2      3      7
                3      4      8

            >>> df4 = df > 3
                     col-1  col-2  col-3
                0    False   True   True
                1    False   True   True
                2    False   True   True
                3     True   True   True

            >>> df5 = df[tb4]
                    col-1  col-2  col-3
                0    NaN      5      9
                1    NaN      6     10
                2    NaN      7     11
                3    4.0      8     12

            >>> df8 = df['col-1'] > 2
                   col-1  col-2  col-3
                0      3      7     11
                1      4      8     12

        """
        if isinstance(item, slice) or isinstance(item, int) or isinstance(item, str) or \
                isinstance(item, List):
            return DataFrame(self._table.__getitem__(item))
        elif isinstance(item, tuple):
            key, obj = item
            self._change_context(env=obj)
            return DataFrame(self._table.__getitem__(item))
        elif isinstance(item, DataFrame):
            return DataFrame(self._table.__getitem__(item.to_table()))

    def __setitem__(self, key, value):
        '''
            Sets values for a existing dataframe by means of a column
            Args:
                key: (str) column-name
                value: (DataFrame) data as a single column table or a scalar

            Returns: PyCylon DataFrame

            Examples
            --------
            >>> df
                   col-1  col-2  col-3
                0      1      5      9
                1      2      6     10
                2      3      7     11
                3      4      8     12


            >>> df['col-3'] = DataFrame([[90, 100, 110, 120]])
                   col-1  col-2  col-3
                0      1      5     90
                1      2      6    100
                2      3      7    110
                3      4      8    120

            >>> df['col-4'] = DataFrame([190, 1100, 1110, 1120]])
                    col-1  col-2  col-3  col-4
                0      1      5     90    190
                1      2      6    100   1100
                2      3      7    110   1110
                3      4      8    120   1120
        '''

        if isinstance(key, str):
            if isinstance(value, DataFrame):
                self._table.__setitem__(key, value.to_table())
            elif np.isscalar(value):
                self._table.__setitem__(key, value)
        else:
            raise ValueError(f"Not Implemented __setitem__ option for key Type {type(key)} and "
                             f"value type {type(value)}")

    def __repr__(self):
        return self._table.__repr__()

    def __len__(self) -> int:
        return self._table.row_count

    def __eq__(self, other) -> DataFrame:
        '''
        Equal operator for DataFrame
        Args:
            other: can be a numeric scalar or a DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------

        >>> df
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df['col-1'] == 2
               col-1
            0  False
            1   True
            2  False
            3  False

        >>> df == 2
               col-1  col-2  col-3
            0  False  False  False
            1   True  False  False
            2  False  False  False
            3  False  False  False

        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__eq__(other))

    def __ne__(self, other) -> DataFrame:
        '''
        Not equal operator for DataFrame
        Args:
            other: can be a numeric scalar or DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df3 = df['col-1'] != 2
               col-1
            0   True
            1  False
            2   True
            3   True

        >>> df4 = df != 2
               col-1  col-2  col-3
            0   True   True   True
            1  False   True   True
            2   True   True   True
            3   True   True   True
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__ne__(other))

    def __lt__(self, other) -> DataFrame:
        '''
        Lesser than operator for DataFrame
        Args:
            other: can be a numeric scalar or DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> tb
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> tb3 = tb['col-1'] < 2
               col-1
            0   True
            1  False
            2  False
            3  False

        >>> tb4 = tb < 2
               col-1  col-2  col-3
            0   True  False  False
            1  False  False  False
            2  False  False  False
            3  False  False  False
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__lt__(other))

    def __gt__(self, other) -> DataFrame:
        '''
        Greater than operator for DataFrame
        Args:
            other: can be a numeric scalar or DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df3 = df['col-1'] > 2
                col-1
            0  False
            1  False
            2   True
            3   True

        >>> df4 = df > 2
               col-1  col-2  col-3
            0  False   True   True
            1  False   True   True
            2   True   True   True
            3   True   True   True
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__gt__(other))

    def __le__(self, other) -> DataFrame:
        '''
        Lesser than or equal operator for DataFrame
        Args:
            other: can be a numeric scalar or DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> tb
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df3 = df['col-1'] <= 2
                col-1
            0   True
            1   True
            2  False
            3  False

        >>> df4 = df <= 2
               col-1  col-2  col-3
            0   True  False  False
            1   True  False  False
            2  False  False  False
            3  False  False  False
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__le__(other))

    def __ge__(self, other) -> DataFrame:
        '''
        Greater than or equal operator for DataFrame
        Args:
            other: can be a numeric scalar or DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12


        >>> df3 = df['col-1'] >= 2
               col-1
            0  False
            1   True
            2   True
            3   True

        >>> df4 = df >= 2
               col-1  col-2  col-3
            0  False   True   True
            1   True   True   True
            2   True   True   True
            3   True   True   True
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__ge__(other))

    def __or__(self, other) -> DataFrame:
        '''
        Or operator for DataFrame
        Args:
            other: PyCylon DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df1
               col-1  col-2
            0  False   True
            1   True   True
            2  False  False
            3   True  False

        >>> df2
                col-1  col-2
            0   True  False
            1   True   True
            2  False  False
            3  False   True

        >>> df_or = df1 | df2
               col-1  col-2
            0   True   True
            1   True   True
            2  False  False
            3   True   True
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__or__(other))

    def __and__(self, other) -> DataFrame:
        '''
        And operator for DataFrame
        Args:
            other: PyCylon DataFrame

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df1
               col-1  col-2
            0  False   True
            1   True   True
            2  False  False
            3   True  False

        >>> df2
                col-1  col-2
            0   True  False
            1   True   True
            2  False  False
            3  False   True

        >>> df_or = df1 & df2
               col-1  col-2
            0  False  False
            1   True   True
            2  False  False
            3  False  False
        '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__and__(other))

    def __invert__(self) -> DataFrame:
        '''
         Invert operator for DataFrame

         Returns: PyCylon DataFrame

         Examples
         --------
         >>> df
                col-1  col-2
            0  False   True
            1   True   True
            2  False  False
            3   True  False

        >>> ~df
               col-1  col-2
            0   True  False
            1  False  False
            2   True   True
            3  False   True
         '''

        return DataFrame(self._table.__invert__())

    def __neg__(self) -> DataFrame:
        '''
         Negation operator for DataFrame

         Returns: PyCylon DataFrame

         Examples
         --------
         >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

         >>> -df
               col-1  col-2  col-3
            0     -1     -5     -9
            1     -2     -6    -10
            2     -3     -7    -11
            3     -4     -8    -12
         '''

        return DataFrame(self._table.__neg__())

    def __add__(self, other) -> DataFrame:
        '''
         Add operator for DataFrame
         Args:
             other: scalar numeric

         Returns: PyCylon DataFrame

         Examples
         --------
        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df + 2
               col-1  col-2  col-3
            0      3      7     11
            1      4      8     12
            2      5      9     13
            3      6     10     14
         '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__add__(other))

    def __sub__(self, other) -> DataFrame:
        '''
         Subtract operator for DataFrame
         Args:
             other: scalar numeric

         Returns: PyCylon DataFrame

         Examples
         --------
        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df - 2
               col-1  col-2  col-3
            0     -1      3      7
            1      0      4      8
            2      1      5      9
            3      2      6     10
         '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__sub__(other))

    def __mul__(self, other) -> DataFrame:
        '''
         Multiply operator for DataFrame
         Args:
             other: scalar numeric

         Returns: PyCylon DataFrame

         Examples
         --------
        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df * 2
               col-1  col-2  col-3
            0      2     10     18
            1      4     12     20
            2      6     14     22
            3      8     16     24
         '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__mul__(other))

    def __truediv__(self, other) -> DataFrame:
        '''
         Element-wise division operator for DataFrame
         Args:
             other: scalar numeric

         Returns: PyCylon DataFrame

         Examples
         --------
        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df / 2
               col-1  col-2  col-3
            0    0.5    2.5    4.5
            1    1.0    3.0    5.0
            2    1.5    3.5    5.5
            3    2.0    4.0    6.0
         '''
        other = other.to_table() if (isinstance(other, DataFrame)) else other
        return DataFrame(self._table.__truediv__(other))

    def drop(self, column_names: List[str]) -> DataFrame:
        '''
        drop a column or list of columns from a DataFrame
        Args:
            column_names: List[str]

        Returns: PyCylon DataFrame

        Examples
        --------

        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df.drop(['col-1'])
               col-2  col-3
            0      5      9
            1      6     10
            2      7     11
            3      8     12
        '''

        return DataFrame(self._table.drop(column_names))

    def fillna(self, fill_value) -> DataFrame:
        '''
        Fill not applicable values with a given value
        Args:
            fill_value: scalar

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0    1.0    5.0    9.0
            1    NaN    6.0   10.0
            2    3.0    NaN   11.0
            3    4.0    8.0    NaN

        >>> df.fillna(0)
               col-1  col-2  col-3
            0      1      5      9
            1      0      6     10
            2      3      0     11
            3      4      8      0
        '''
        # Note: Supports numeric types only
        return DataFrame(self._table.fillna(fill_value))

    def where(self, condition: DataFrame = None, other=None) -> DataFrame:
        '''
        Experimental version of Where operation.
        Replace values where condition is False
        Args:
            condition: bool DataFrame
            other: Scalar

        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df.where(df > 2)
                col-1  col-2  col-3
            0    NaN      5      9
            1    NaN      6     10
            2    3.0      7     11
            3    4.0      8     12

        >>> df.where(df > 2, 10)
               col-1  col-2  col-3
            0     10      5      9
            1     10      6     10
            2      3      7     11
            3      4      8     12
        '''
        if condition is None:
            raise ValueError("Condition must be provided")
        return DataFrame(self._table.where(condition, other))

    def isnull(self) -> DataFrame:
        '''
        Checks for null elements and returns a bool DataFrame
        Returns: PyCylon DataFrame

        Examples
        --------

        >>> df
               col-1  col-2  col-3
            0    1.0    5.0    9.0
            1    NaN    6.0   10.0
            2    3.0    NaN   11.0
            3    4.0    8.0    NaN

        >>> df.isnull()
                col-1  col-2  col-3
            0  False  False  False
            1   True  False  False
            2  False   True  False
            3  False  False   True

        '''
        return DataFrame(self._table.isnull())

    def isna(self) -> DataFrame:
        '''
        Check for not applicable values and returns a bool DataFrame
        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0    1.0    5.0    9.0
            1    NaN    6.0   10.0
            2    3.0    NaN   11.0
            3    4.0    8.0    NaN

        >>> df.isna()
                col-1  col-2  col-3
            0  False  False  False
            1   True  False  False
            2  False   True  False
            3  False  False   True
        '''
        return DataFrame(self._table.isnull())

    def notnull(self) -> DataFrame:
        '''
        Check the not null values and returns a bool DataFrame
        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0    1.0    5.0    9.0
            1    NaN    6.0   10.0
            2    3.0    NaN   11.0
            3    4.0    8.0    NaN

        >>> df.notnull()
               col-1  col-2  col-3
            0   True   True   True
            1  False   True   True
            2   True  False   True
            3   True   True  False
        '''

        return ~self.isnull()

    def notna(self) -> DataFrame:
        '''
        Checks for not NA values and returns a bool DataFrame
        Returns: PyCylon DataFrame

        Examples
        --------
        >>> df
               col-1  col-2  col-3
            0    1.0    5.0    9.0
            1    NaN    6.0   10.0
            2    3.0    NaN   11.0
            3    4.0    8.0    NaN

        >>> df.notna()
               col-1  col-2  col-3
            0   True   True   True
            1  False   True   True
            2   True  False   True
            3   True   True  False
        '''

        return ~self.isnull()

    def rename(self, column_names):
        '''
        Rename a DataFrame with a column name or column names
        Args:
            column_names: dictionary or full list of new column names

        Returns: None

        Examples
        --------
        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df.rename({'col-1': 'col_1'})
               col_1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df.rename(['c1', 'c2', 'c3'])
               c1  c2  c3
            0   1   5   9
            1   2   6  10
            2   3   7  11
            3   4   8  12
        '''
        self._table.rename(column_names)

    def add_prefix(self, prefix: str) -> DataFrame:
        '''
        Adding a prefix to column names
        Args:
            prefix: str

        Returns: PyCylon DataFrame with prefix updated

        Examples
        --------

        >>> df
                col-1  col-2  col-3
            0      1      5      9
            1      2      6     10
            2      3      7     11
            3      4      8     12

        >>> df.add_prefix('old_')
               old_c1  old_c2  old_c3
            0       1       5       9
            1       2       6      10
            2       3       7      11
            3       4       8      12

        '''

        return DataFrame(self._table.add_prefix(prefix))

    # Indexing

    def set_index(
            self, keys, drop=True, append=False, inplace=False, verify_integrity=False
    ) -> DataFrame:
        """
        Set the DataFrame index using existing columns.
        Set the DataFrame index (row labels) using one or more existing
        columns or arrays (of the correct length). The index can replace the
        existing index or expand on it.
        Parameters
        ----------
        keys : label or array-like or list of labels/arrays
            This parameter can be either a single column key, a single array of
            the same length as the calling DataFrame, or a list containing an
            arbitrary combination of column keys and arrays. Here, "array"
            encompasses :class:`Series`, :class:`Index`, ``np.ndarray``, and
            instances of :class:`~collections.abc.Iterator`.
        drop : bool, default True
            Delete columns to be used as the new index.
        append : bool, default False
            Whether to append columns to existing index.
        inplace : bool, default False
            If True, modifies the DataFrame in place (do not create a new object).
        verify_integrity : bool, default False
            Check the new index for duplicates. Otherwise defer the check until
            necessary. Setting to False will improve the performance of this
            method.
        Returns
        -------
        DataFrame or None
            Changed row labels or None if ``inplace=True``.
        See Also
        --------
        DataFrame.reset_index : Opposite of set_index.
        DataFrame.reindex : Change to new indices or expand indices.
        DataFrame.reindex_like : Change to same indices as other DataFrame.
        Examples
        --------
        >>> df = pd.DataFrame({'month': [1, 4, 7, 10],
        ...                    'year': [2012, 2014, 2013, 2014],
        ...                    'sale': [55, 40, 84, 31]})
        >>> df
           month  year  sale
        0      1  2012    55
        1      4  2014    40
        2      7  2013    84
        3     10  2014    31
        Set the index to become the 'month' column:
        >>> df.set_index('month')
               year  sale
        month
        1      2012    55
        4      2014    40
        7      2013    84
        10     2014    31
        Create a MultiIndex using columns 'year' and 'month':
        >>> df.set_index(['year', 'month'])
                    sale
        year  month
        2012  1     55
        2014  4     40
        2013  7     84
        2014  10    31
        Create a MultiIndex using an Index and a column:
        >>> df.set_index([pd.Index([1, 2, 3, 4]), 'year'])
                 month  sale
           year
        1  2012  1      55
        2  2014  4      40
        3  2013  7      84
        4  2014  10     31
        Create a MultiIndex using two Series:
        >>> s = pd.Series([1, 2, 3, 4])
        >>> df.set_index([s, s**2])
              month  year  sale
        1 1       1  2012    55
        2 4       4  2014    40
        3 9       7  2013    84
        4 16     10  2014    31
        """
        # todo this is not a final implementation
        index_keys = []
        index_keys.extend(keys)

        if append:
            for c in self._index_columns:
                if not c in index_keys:
                    index_keys.append(c)

        if inplace:
            self._index_columns = index_keys
            self._table.set_index(index_keys, drop=drop)
            return None
        else:
            new_df = DataFrame(self._table)
            new_df._table.set_index(index_keys, drop=drop)
            new_df._index_columns = index_keys
            return new_df

    def reset_index(  # type: ignore[misc]
            self,
            level: Optional[Union[Hashable, Sequence[Hashable]]] = ...,
            drop: bool = ...,
            inplace: False = ...,
            col_level: Hashable = ...,
            col_fill=...,
    ) -> DataFrame:
        # todo this is not a final implementation
        self._index_columns = []
        self._table.reset_index(drop=drop)
        return self

    # Combining / joining / merging

    def sort_values(
            self,
            by,
            ascending=True,
            env: CylonEnv = None,
            **sort_options) -> DataFrame:
        """
        Sort by the values along columns.
        Parameters
        ----------
        by : str or list of str
            Name or list of names to sort by.
                if axis is 0 or ‘index’ then by may contain index levels and/or column labels.
                if axis is 1 or ‘columns’ then by may contain column levels and/or index labels.
        ascending : bool or list of bool, default True
             Sort ascending vs. descending. Specify list for multiple sort
             orders.  If this is a list of bools, must match the length of
             the by.
        env: CylonEnv, default (None)
            Execution environment used to distinguish between distributed and local operations.
            default None (local env)
        sort_options:
            Options to be passed to SortOptions class
            ex:
            sampling: str
                Sampling algorithm. Choices {'initial', 'regular'} default 'initial'
            num_samples: int, default 0
                Number of samples to determine key distribution. Only used in a distributed env.
                Need to pass a deterministic value common to every process. If num_samples == 0,
                the value would be handled internally.
            num_bins: int, default 0
                Number of bins in the histogram of the key distribution. Only used in a distributed
                env. Need to pass a deterministic value common to every process. If num_bins == 0,
                the value would be handled internally.

        Returns
        -------
        DataFrame
            DataFrame with sorted values

        Examples
        --------
        >>> df = DataFrame({
        ...     'col1': ['A', 'A', 'B', np.nan, 'D', 'C'],
        ...     'col2': [2, 1, 9, 8, 7, 4],
        ...     'col3': [0, 1, 9, 4, 2, 3],
        ...     'col4': ['a', 'B', 'c', 'D', 'e', 'F']
        ... })
        >>> df
          col1  col2  col3 col4
        0    A     2     0    a
        1    A     1     1    B
        2    B     9     9    c
        3  NaN     8     4    D
        4    D     7     2    e
        5    C     4     3    F
        Sort by col1
        >>> df.sort_values(by=['col1'])
          col1  col2  col3 col4
        0    A     2     0    a
        1    A     1     1    B
        2    B     9     9    c
        5    C     4     3    F
        4    D     7     2    e
        3  NaN     8     4    D
        Sort by multiple columns
        >>> df.sort_values(by=['col1', 'col2'])
          col1  col2  col3 col4
        1    A     1     1    B
        0    A     2     0    a
        2    B     9     9    c
        5    C     4     3    F
        4    D     7     2    e
        3  NaN     8     4    D
        Sort Descending
        >>> df.sort_values(by='col1', ascending=False)
          col1  col2  col3 col4
        4    D     7     2    e
        5    C     4     3    F
        2    B     9     9    c
        0    A     2     0    a
        1    A     1     1    B
        3  NaN     8     4    D
        """
        if env is None:
            return DataFrame(self._table.sort(order_by=by, ascending=ascending))
        else:
            sort_opts = SortOptions(**sort_options)
            return DataFrame(
                self._change_context(env)._table.distributed_sort(order_by=by, ascending=ascending,
                                                                  sort_options=sort_opts))

    def join(self, other: DataFrame, on=None, how='left', lsuffix='l', rsuffix='r',
             sort=False, algorithm="sort", env: CylonEnv = None) -> DataFrame:
        """
        Join columns with other DataFrame either on index or on a key
        column. Efficiently Join multiple DataFrame objects by index at once by
        passing a list.

        Parameters
        ----------
        other : DataFrame, Series with name field set, or list of DataFrame
            Index should be similar to one of the columns in this one. If a
            Series is passed, its name attribute must be set, and that will be
            used as the column name in the resulting joined DataFrame
        on : column name, tuple/list of column names, or array-like
            Column(s) in the caller to join on the index in other,
            otherwise joins index-on-index. If multiples
            columns given, the passed DataFrame must have a MultiIndex. Can
            pass an array as the join key if not already contained in the
            calling DataFrame. Like an Excel VLOOKUP operation
        how : {'left', 'right', 'outer', 'inner'}, default: 'left'
            How to handle the operation of the two objects.
            * left: use calling frame's index (or column if on is specified)
            * right: use other frame's index
            * outer: form union of calling frame's index (or column if on is
              specified) with other frame's index, and sort it
              lexicographically
            * inner: form intersection of calling frame's index (or column if
              on is specified) with other frame's index, preserving the order
              of the calling's one
        lsuffix : string
            Suffix to use from left frame's overlapping columns
        rsuffix : string
            Suffix to use from right frame's overlapping columns
        sort : boolean, default False
            Order result DataFrame lexicographically by the join key. If False,
            the order of the join key depends on the join type (how keyword)
        algorithm: {'sort', 'hash'}, default: 'sort'
            The algorithm that should be used to perform the join between two tables.
        Notes
        -----
        on, lsuffix, and rsuffix options are not supported when passing a list
        of DataFrame objects
        Examples
        --------
        >>> caller
            A key
        0  A0  K0
        1  A1  K1
        2  A2  K2
        3  A3  K3
        4  A4  K4
        5  A5  K5

        >>> other
            B key
        0  B0  K0
        1  B1  K1
        2  B2  K2
        Join DataFrames using their indexes.
        >>> caller.join(other, lsuffix='_caller', rsuffix='_other')
        >>>     A key_caller    B key_other
            0  A0         K0   B0        K0
            1  A1         K1   B1        K1
            2  A2         K2   B2        K2
            3  A3         K3  NaN       NaN
            4  A4         K4  NaN       NaN
            5  A5         K5  NaN       NaN
        If we want to join using the key columns, we need to set key to be
        the index in both caller and other. The joined DataFrame will have
        key as its index.
        >>> caller.set_index('key').join(other.set_index('key'))
        >>>      A    B
            key
            K0   A0   B0
            K1   A1   B1
            K2   A2   B2
            K3   A3  NaN
            K4   A4  NaN
            K5   A5  NaN
        Another option to join using the key columns is to use the on
        parameter. DataFrame.join always uses other's index but we can use any
        column in the caller. This method preserves the original caller's
        index in the result.
        >>> caller.join(other.set_index('key'), on='key')
        >>>     A key    B
            0  A0  K0   B0
            1  A1  K1   B1
            2  A2  K2   B2
            3  A3  K3  NaN
            4  A4  K4  NaN
            5  A5  K5  NaN
        See also
        --------
        DataFrame.merge : For column(s)-on-columns(s) operations
        Returns
        -------
        joined : DataFrame
        """
        left_on = on
        if left_on is None:
            left_on = self._index_columns

        right_on = other._index_columns

        if left_on is None or len(left_on) == 0:
            raise ValueError(
                "The column to join from left relation is no specified. Either provide 'on' or set indexing")

        if right_on is None or len(right_on) == 0:
            raise ValueError(
                "The 'other' relation doesn't have index columns specified.")

        if env is None:
            joined_table = self._table.join(table=other._table, join_type=how,
                                            algorithm=algorithm,
                                            left_on=left_on, right_on=right_on,
                                            left_prefix=lsuffix, right_prefix=rsuffix)
            return DataFrame(joined_table)
        else:
            # attach context
            self._change_context(env=env)
            other._change_context(env=env)

            joined_table = self._table.distributed_join(table=other._table, join_type=how,
                                                        algorithm=algorithm,
                                                        left_on=left_on, right_on=right_on,
                                                        left_prefix=lsuffix, right_prefix=rsuffix)
            return DataFrame(joined_table)

    def merge(self,
              right: DataFrame,
              how="inner",
              algorithm="sort",
              on=None,
              left_on=None,
              right_on=None,
              left_index=False,
              right_index=False,
              sort=False,
              suffixes=("_x", "_y"),
              copy=True,
              indicator=False,
              validate=None,
              env: CylonEnv = None) -> DataFrame:
        """
        Merge DataFrame with a database-style join.
        The join is done on columns or indexes. If joining columns on
        columns, the DataFrame indexes *will be ignored*. Otherwise if joining indexes
        on indexes or indexes on a column or columns, the index will be passed on.
        When performing a cross merge, no column specifications to merge on are
        allowed.

        Parameters
        ----------
        right : DataFrame or named Series
            Object to merge with.
        how : {'left', 'right', 'outer', 'inner', 'cross(Unsupported)'}, default 'inner'
            Type of merge to be performed.
            * left: use only keys from left frame, similar to a SQL left outer join;
            preserve key order.
            * right: use only keys from right frame, similar to a SQL right outer join;
            preserve key order.
            * outer: use union of keys from both frames, similar to a SQL full outer
            join; sort keys lexicographically.
            * inner: use intersection of keys from both frames, similar to a SQL inner
            join; preserve the order of the left keys.
            * cross: creates the cartesian product from both frames, preserves the order
            of the left keys.
            .. versionadded:: 1.2.0
        on : label or list
            Column or index level names to join on. These must be found in both
            DataFrames. If `on` is None and not merging on indexes then this defaults
            to the intersection of the columns in both DataFrames.
        left_on : label or list, or array-like
            Column or index level names to join on in the left DataFrame. Can also
            be an array or list of arrays of the length of the left DataFrame.
            These arrays are treated as if they are columns.
        right_on : label or list, or array-like
            Column or index level names to join on in the right DataFrame. Can also
            be an array or list of arrays of the length of the right DataFrame.
            These arrays are treated as if they are columns.
        left_index : bool, default False
            Use the index from the left DataFrame as the join key(s). If it is a
            MultiIndex, the number of keys in the other DataFrame (either the index
            or a number of columns) must match the number of levels.
        right_index : bool, default False
            Use the index from the right DataFrame as the join key. Same caveats as
            left_index.
        sort(Unsupported) : bool, default False
            Sort the join keys lexicographically in the result DataFrame. If False,
            the order of the join keys depends on the join type (how keyword).
        suffixes : list-like, default is ("_x", "_y")
            A length-2 sequence where each element is optionally a string
            indicating the suffix to add to overlapping column names in
            `left` and `right` respectively. Pass a value of `None` instead
            of a string to indicate that the column name from `left` or
            `right` should be left as-is, with no suffix. At least one of the
            values must not be None.
        copy(Unsupported) : bool, default True
            If False, avoid copy if possible.
        indicator(Unsupported) : bool or str, default False
            If True, adds a column to the output DataFrame called "_merge" with
            information on the source of each row. The column can be given a different
            name by providing a string argument. The column will have a Categorical
            type with the value of "left_only" for observations whose merge key only
            appears in the left DataFrame, "right_only" for observations
            whose merge key only appears in the right DataFrame, and "both"
            if the observation's merge key is found in both DataFrames.
        validate(Unsupported) : str, optional
            If specified, checks if merge is of specified type.
            * "one_to_one" or "1:1": check if merge keys are unique in both
            left and right datasets.
            * "one_to_many" or "1:m": check if merge keys are unique in left
            dataset.
            * "many_to_one" or "m:1": check if merge keys are unique in right
            dataset.
            * "many_to_many" or "m:m": allowed, but does not result in checks.
        Returns
        -------
        DataFrame
            A DataFrame of the two merged objects.
        See Also
        --------
        merge_ordered : Merge with optional filling/interpolation.
        merge_asof : Merge on nearest keys.
        DataFrame.join : Similar method using indices.
        Notes
        -----
        Support for specifying index levels as the `on`, `left_on`, and
        `right_on` parameters was added in version 0.23.0
        Support for merging named Series objects was added in version 0.24.0
        Examples
        --------
        >>> df1 = DataFrame({'lkey': ['foo', 'bar', 'baz', 'foo'],
        ...                     'value': [1, 2, 3, 5]})
        >>> df2 = DataFrame({'rkey': ['foo', 'bar', 'baz', 'foo'],
        ...                     'value': [5, 6, 7, 8]})
        >>> df1
            lkey value
        0   foo      1
        1   bar      2
        2   baz      3
        3   foo      5
        >>> df2
            rkey value
        0   foo      5
        1   bar      6
        2   baz      7
        3   foo      8

        Merge df1 and df2 on the lkey and rkey columns. The value columns have
        the default suffixes, _x and _y, appended.

        >>> df1.merge(df2, left_on='lkey', right_on='rkey')
        lkey  value_x rkey  value_y
        0  foo        1  foo        5
        1  foo        1  foo        8
        2  foo        5  foo        5
        3  foo        5  foo        8
        4  bar        2  bar        6
        5  baz        3  baz        7

        Merge DataFrames df1 and df2 with specified left and right suffixes
        appended to any overlapping columns.

        >>> df1.merge(df2, left_on='lkey', right_on='rkey',
        ...           suffixes=('_left', '_right'))
        lkey  value_left rkey  value_right
        0  foo           1  foo            5
        1  foo           1  foo            8
        2  foo           5  foo            5
        3  foo           5  foo            8
        4  bar           2  bar            6
        5  baz           3  baz            7

        Merge DataFrames df1 and df2, but raise an exception if the DataFrames have
        any overlapping columns.

        >>> df1.merge(df2, left_on='lkey', right_on='rkey', suffixes=(False, False))
        Traceback (most recent call last):
        ...
        ValueError: columns overlap but no suffix specified:
            Index(['value'], dtype='object')

        >>> df1 = DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
        >>> df2 = DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})
        >>> df1
            a  b
        0   foo  1
        1   bar  2

        >>> df2
            a  c
        0   foo  3
        1   baz  4

        >>> df1.merge(df2, how='inner', on='a')
            a  b  c
        0   foo  1  3

        >>> df1.merge(df2, how='left', on='a')
            a  b  c
        0   foo  1  3.0
        1   bar  2  NaN

        >>> df1 = DataFrame({'left': ['foo', 'bar']})
        >>> df2 = DataFrame({'right': [7, 8]})

        >>> df1
            left
        0   foo
        1   bar

        >>> df2
            right
        0   7
        1   8

        >>> df1.merge(df2, how='cross')
        left  right
        0   foo      7
        1   foo      8
        2   bar      7
        3   bar      8
        """
        if not on is None:
            left_on = on
            right_on = on

        if left_index:
            left_on = self._index_columns

        if right_index:
            right_on = right._index_columns

        if left_on is None or right_on is None:
            raise ValueError(
                "Columns to merge is not specified. Expected on or left_index/right_index."
                "Make sure dataframes has specified index columns if using left_index/right_index")

        if env is None:
            joined_table = self._table.join(table=right._table, join_type=how,
                                            algorithm=algorithm,
                                            left_on=left_on, right_on=right_on,
                                            left_prefix=suffixes[0], right_prefix=suffixes[1])
            return DataFrame(joined_table)
        else:
            self._change_context(env)
            right._change_context(env)
            joined_table = self._table.distributed_join(table=right._table, join_type=how,
                                                        algorithm=algorithm,
                                                        left_on=left_on, right_on=right_on,
                                                        left_prefix=suffixes[0],
                                                        right_prefix=suffixes[1])
            return DataFrame(joined_table)

    def drop_duplicates(self, subset: Optional[Union[Hashable, Sequence[Hashable]]] = None,
                        keep: Union[str, bool] = "first", inplace: bool = False,
                        ignore_index: bool = False,
                        env: CylonEnv = None) -> DataFrame:
        """
        Return DataFrame with duplicate rows removed.
        Considering certain columns is optional. Indexes, including time indexes
        are ignored.
        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by
            default use all of the columns.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to keep.
            - ``first`` : Drop duplicates except for the first occurrence.
            - ``last`` : Drop duplicates except for the last occurrence.
            - False (Unsupported): Drop all duplicates.
        inplace : bool, default False
            Whether to drop duplicates in place or to return a copy.
        ignore_index (Unsupported) : bool, default False
            If True, the resulting axis will be labeled 0, 1, …, n - 1.
        env: Execution environment used to distinguish between distributed and local operations. default None (local env)
        Returns
        -------
        DataFrame or None
            DataFrame with duplicates removed or None if ``inplace=True``(Unsupported).
        See Also
        --------
        DataFrame.value_counts: Count unique combinations of columns.
        Examples
        --------
        Consider dataset containing ramen rating.
        >>> df = DataFrame({
        ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        ...     'rating': [4, 4, 3.5, 15, 5]
        ... })
        >>> df
            brand style  rating
        0  Yum Yum   cup     4.0
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0
        By default, it removes duplicate rows based on all columns.
        >>> df.drop_duplicates()
            brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0
        To remove duplicates on specific column(s), use ``subset``.
        >>> df.drop_duplicates(subset=['brand'])
            brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        To remove duplicates and keep last occurrences, use ``keep``.
        >>> df.drop_duplicates(subset=['brand', 'style'], keep='last')
            brand style  rating
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        4  Indomie  pack     5.0
        """
        if env is None:
            return DataFrame(self._table.unique(columns=subset, keep=keep, inplace=inplace))
        else:
            return DataFrame(self._change_context(env)._table.distributed_unique(columns=subset,
                                                                                 inplace=inplace))

    def groupby(self, by: Union[int, str, List], groupby_type="hash",
                env: CylonEnv = None) -> GroupByDataFrame:
        """
        A groupby operation involves some combination of splitting the object, applying a function, and combining the results. 
        This can be used to group large amounts of data and compute operations on these groups.

        Parameters
        ----------

        by : str, int or a list of str, int.  
            List of column(s) used for grouping.
        groupby_type: str,
            Groupby Type - [hash, mapred_hash] default: hash

        Returns
        -------
        GroupByDataFrame

        Examples
        -------

        >>> df1 = DataFrame([[0, 0, 1, 1], [1, 10, 1, 5], [10, 20, 30, 40]])


        >>> df3 = df1.groupby(by=0).agg({"1": "sum", "2": "min"})
        >>> df3
        0  sum_1  min_2
        0  0     11     10
        1  1      6     30

        >>> df4 = df1.groupby(by=0).min()
        >>> df4
        0  min_2  min_1
        0  0     10      1
        1  1     30      1

        >>> df5 = df1.groupby(by=[0, 1]).max()
        >>> df5
        0   1  max_2
        0  0   1     10
        1  0  10     20
        2  1   1     30
        3  1   5     40
        """
        by_list = []
        if isinstance(by, int):
            by_list.append(self.columns[by])
        elif isinstance(by, str):
            if by not in self.columns:
                raise ValueError(
                    str + " is not a column of this table. Expected one of " + str(by))
            by_list.append(by)
        elif isinstance(by, list):
            if len(by) == 0:
                raise ValueError("Group by columns should be specified.")

            for b in by:
                if isinstance(b, str):
                    by_list.append(b)
                elif isinstance(b, int):
                    by_list.append(self.columns[b])
                else:
                    raise ValueError(
                        "Unsupported column specification. Expected column index or name")
        else:
            raise ValueError("Unknown value for by")
        if env is None:
            return GroupByDataFrame(self, by_list, groupby_type=groupby_type)
        else:
            return GroupByDataFrame(self._change_context(env), by_list, groupby_type=groupby_type)

    def isin(self, values: Union[List, Dict, cn.Table], skip_null: bool = True) -> DataFrame:
        """
        Whether each element in the DataFrame is contained in values

        Parameters
        ----------
        values: list, dict or cylon
        skip_null:

        Returns
        ----------
        DataFrame

        """
        if isinstance(values, (List, Dict)):
            return DataFrame(self._table.isin(values, skip_null=skip_null))
        elif isinstance(values, DataFrame):
            return DataFrame(self._table.isin(values._table, skip_null=skip_null))
        else:
            raise ValueError("Unsupported type for values" + type(values))

    def applymap(self, func, na_action=None) -> DataFrame:
        """
        Apply a function to a Dataframe elementwise.
        This method applies a function that accepts and returns a scalar
        to every element of a DataFrame.
        Parameters
        ----------
        func : callable
            Python function, returns a single value from a single value.
        na_action :  (Unsupported) {None, 'ignore'}, default None
            If ‘ignore’, propagate NaN values, without passing them to func.

        Returns
        -------
        DataFrame
            Transformed DataFrame.
        See Also
        --------
        DataFrame.apply : Apply a function along input axis of DataFrame.
        Examples
        --------
        >>> df = pd.DataFrame([[1, 2.12], [3.356, 4.567]])
        >>> df
               0      1
        0  1.000  2.120
        1  3.356  4.567
        >>> df.applymap(lambda x: len(str(x)))
           0  1
        0  3  4
        1  5  5
        (Unsupported) Like Series.map, NA values can be ignored:
        >>> df_copy = df.copy()
        >>> df_copy.iloc[0, 0] = pd.NA
        >>> df_copy.applymap(lambda x: len(str(x)), na_action='ignore')
              0  1
        0  <NA>  4
        1     5  5
        Note that a vectorized version of `func` often exists, which will
        be much faster. You could square each number elementwise.
        >>> df.applymap(lambda x: x**2)
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489
        (Unsupported) But it's better to avoid applymap in that case.
        >>> df ** 2
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489
        """
        return DataFrame(self._table.applymap(func))


# -------------------- staticmethods ---------------------------

def concat(objs: Union[Iterable["DataFrame"]], axis=0, join="outer",
           env: CylonEnv = None) -> DataFrame:
    """
    Concatenate DataFrames along a particular axis with optional set logic
    along the other axes.
    Can also add a layer of hierarchical indexing on the concatenation axis,
    which may be useful if the labels are the same (or overlapping) on
    the passed axis number.

    Cylon currently support concat along axis=0, for DataFrames having the same schema(Union).

    Parameters
    ----------
    objs : a sequence or mapping of Series or DataFrame objects
        If a mapping is passed, the sorted keys will be used as the `keys`
        argument, unless it is passed, in which case the values will be
        selected (see below). Any None objects will be dropped silently unless
        they are all None in which case a ValueError will be raised.
    axis : {0/'index', 1/'columns' (Unsupported)}, default 0
        The axis to concatenate along.
    join : {'inner', 'outer'}, default 'outer'
        How to handle indexes on other axis (or axes).
    env: Execution environment used to distinguish between distributed and local operations. default None (local env)
    Returns
    -------
    object, type of objs
        When concatenating along
        the columns (axis=1) or rows (axis=0), a ``DataFrame`` is returned.

    Examples
    --------

    Combine two ``DataFrame`` objects with identical columns.

    >>> df1 = DataFrame([['a', 1], ['b', 2]],
    ...                    columns=['letter', 'number'])
    >>> df1
    letter  number
    0      a       1
    1      b       2
    >>> df2 = DataFrame([['c', 3], ['d', 4]],
    ...                    columns=['letter', 'number'])
    >>> df2
    letter  number
    0      c       3
    1      d       4
    >>> DataFrame.concat([df1, df2])
    letter  number
    0      a       1
    1      b       2
    0      c       3
    1      d       4

    (Unsupported) Combine ``DataFrame`` objects with overlapping columns
    and return everything. Columns outside the intersection will
    be filled with ``NaN`` values.

    >>> df3 = DataFrame([['c', 3, 'cat'], ['d', 4, 'dog']],
    ...                    columns=['letter', 'number', 'animal'])
    >>> df3
    letter  number animal
    0      c       3    cat
    1      d       4    dog
    >>> DataFrame.concat([df1, df3])
    letter  number animal
    0      a       1    NaN
    1      b       2    NaN
    0      c       3    cat
    1      d       4    dog

    (Unsupported) Combine ``DataFrame`` objects with overlapping columns
    and return only those that are shared by passing ``inner`` to
    the ``join`` keyword argument.

    >>> DataFrame.concat([df1, df3],join="inner")
    letter  number
    0      a       1
    1      b       2
    0      c       3
    1      d       4

    (Unsupported) Combine ``DataFrame`` objects horizontally along the x axis by
    passing in ``axis=1``.

    >>> df4 = DataFrame([['bird', 'polly'], ['monkey', 'george']],
    ...                    columns=['animal', 'name'])
    >>> DataFrame.concat([df1, df4],axis=1)

    letter  number  animal    name
    0      a       1    bird   polly
    1      b       2  monkey  george

    (Unsupported) Prevent the result from including duplicate index values with the
    ``verify_integrity`` option.

    >>> df5 = DataFrame([1], index=['a'])
    >>> df5
    0
    a  1
    >>> df6 = DataFrame([2], index=['a'])
    >>> df6
    0
    a  2
    >>> DataFrame.concat([df5, df6])
    Traceback (most recent call last):
        ...
    ValueError: Indexes have overlapping values: ['a']
    """
    # ignore_index: bool = False,
    # keys=None,
    # levels=None,
    # names=None,
    # verify_integrity: bool = False,
    # sort: bool = False,
    # copy: bool = True,

    if len(objs) == 0:
        raise ValueError("objs can't be empty")

    if env is None:
        res_table = cn.Table.concat(
            tables=[df.to_table() for df in objs], axis=axis, join=join)
    else:
        res_table = cn.Table.distributed_concat(
            tables=[df._change_context(env).to_table() for df in objs],
            axis=axis, join=join)
    return DataFrame(res_table)
