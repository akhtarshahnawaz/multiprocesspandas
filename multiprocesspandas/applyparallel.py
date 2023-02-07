"""ADDS FUNCTIONALITY TO APPLY FUNCTION ON PANDAS OBJECTS IN PARALLEL 

This script add functionality to Pandas so that you can do parallel processing in multiple cores when you use apply method on 
dataframes, series or groupby objects.

This file must be imported as a module and it attached following functions to pandas:
    * group_apply_parallel - adds apply_parallel method to groupby objects
    * series_apply_parallel - adds apply_parallel method to series objects
    * df_apply_parallel - adds apply_parallel method to dataframe objects
"""

import pandas as pd
from multiprocess import Pool
import functools
from os import cpu_count
from tqdm import tqdm
import numpy as np

def attachpandas():
    pd.core.groupby.generic.DataFrameGroupBy.apply_parallel = group_apply_parallel
    pd.core.series.Series.apply_parallel = series_apply_parallel
    pd.core.frame.DataFrame.apply_parallel = df_apply_parallel

def group_apply_parallel(self, func, num_processes=cpu_count(), n_chunks = None, *args, **kwargs):
    """
    Add functionality to pandas so that you can do processing on groups on multiple cores at same time.
    - This method will pass each group dataframe to the passed func (including key columns on which the group is formed).
    """
    if n_chunks is not None:
        assert n_chunks>=num_processes, "Number of chunks must be greater than number of processes"
    func = functools.partial(func, *args, **kwargs)
    chunk_size = (len(self)//(n_chunks if n_chunks is not None else num_processes))

    with Pool(num_processes) as p:
        ret_list = p.map(func, tqdm([df.copy() for idx, df in self]), chunksize=max(1,chunk_size))

    keys = self.keys if isinstance(self.keys, list) else [self.keys]
    if isinstance(ret_list[0], pd.DataFrame):
        return pd.concat(ret_list, keys=[idx for idx, df in self],names=keys, axis=0)
    
    if isinstance(ret_list[0], pd.Series):
        return pd.concat(ret_list, keys=[idx for idx, df in self],names=keys, axis=1).T
    
    out = pd.DataFrame([idx for idx, df in self], columns=keys)
    out[func.func.__name__] = ret_list
    return out.set_index(keys)

pd.core.groupby.generic.DataFrameGroupBy.apply_parallel = group_apply_parallel


def series_apply_parallel(self, func, num_processes=cpu_count(), n_chunks = None, *args, **kwargs):
    """
    Add functionality to pandas so that you can do processing on series on multiple cores at same time.
    - This method will pass individual items from series to the func.
    """
    if n_chunks is not None:
        assert n_chunks>=num_processes, "Number of chunks must be greater than number of processes"
    func = functools.partial(func, *args, **kwargs)
    chunk_size = (self.shape[0]//(n_chunks if n_chunks is not None else num_processes))

    with Pool(num_processes) as p:
        ret_list = p.map(func, tqdm(self.values.tolist()), chunksize = max(1,chunk_size))
    ret_list = np.array(ret_list).flatten()

    # Handle if aggregation function ('func') returns dataframes 
    if isinstance(ret_list[0], pd.DataFrame):
        return pd.concat(ret_list, keys=self.index,names=self.index.name, axis=0)
    
    # Handle if aggregation function ('func') returns any other iterable
    return pd.Series(ret_list, index=self.index)

pd.core.series.Series.apply_parallel = series_apply_parallel


def df_apply_parallel(self, func, num_processes=cpu_count(), n_chunks = None, axis=0, *args, **kwargs):
    """
    Add functionality to pandas so that you can do processing on dataframes on multiple cores at same time.
    - This method will pass individual rows/columns from dataframe to the func.
    - Pass axis=1 if you want to process on columns. By default, axis=0 i.e. each rows
    """       
    if n_chunks is not None:
        assert n_chunks>=num_processes, "Number of chunks must be greater than number of processes"   
    func = functools.partial(func, *args, **kwargs)
    chunk_size = (self.shape[0]//(n_chunks if n_chunks is not None else num_processes))

    with Pool(num_processes) as p:
        if axis==0:
            ret_list = p.map(func, tqdm([row for _, row in self.iterrows()]), chunksize = max(1,chunk_size))
        elif axis==1:
            ret_list = p.map(func, tqdm([col for _, col in self.items()]), chunksize = max(1,chunk_size))

    # Handle if aggregation function ('func') returns dataframes
    if isinstance(ret_list[0], pd.DataFrame):
        if axis==0:
            return pd.concat(ret_list, keys=self.index,names=self.index.name, axis=0)
        elif axis==1:
            return pd.concat(ret_list, keys=self.columns, axis=0)

    # Handle if aggregation function ('func') returns series
    if isinstance(ret_list[0], pd.Series):
        if axis==0:
            return pd.concat(ret_list, keys=self.index,names=self.index.name, axis=1).T
        elif axis==1:
            return pd.concat(ret_list, keys=self.columns, axis=1)
    
    # Handle if aggregation function ('func') returns any other iterable
    return pd.Series(ret_list, index=self.index) if axis==0 else pd.Series(ret_list, index=self.columns)

pd.core.frame.DataFrame.apply_parallel = df_apply_parallel

