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

def attachpandas():
    pd.core.groupby.generic.DataFrameGroupBy.apply_parallel = group_apply_parallel
    pd.core.series.Series.apply_parallel = series_apply_parallel
    pd.core.frame.DataFrame.apply_parallel = df_apply_parallel

def group_apply_parallel(self, func, static_data=None, num_processes = cpu_count()):
    """
    Add functionality to pandas so that you can do processing on groups on multiple cores at same time.
    - This method will pass each group dataframe to the passed func (including key columns on which the group is formed).
    - If there is some external data that needs to be used by the function, pass it as a list in static_data, and then accept that list in your func.
        You must have a named argument with name 'static_data' if you need to accept static data.
        Individual items in static_data list needs to be accessed using indexing.
    """
    func = func if static_data is None else functools.partial(func, static_data=static_data)
    with Pool(num_processes) as p:
        ret_list = p.map(func, [df.copy() for idx, df in self])

    if isinstance(ret_list[0], pd.DataFrame) or isinstance(ret_list[0], pd.DataFrame):
        return pd.concat(ret_list, keys=[idx for idx, df in self],names=self.keys, axis=0)
    
    out = pd.DataFrame([idx for idx, df in self], columns=self.keys)
    out[''] = ret_list
    return out.set_index(self.keys)

pd.core.groupby.generic.DataFrameGroupBy.apply_parallel = group_apply_parallel



def series_apply_parallel(self, func, static_data=None, num_processes = cpu_count()):
    """
    Add functionality to pandas so that you can do processing on series on multiple cores at same time.
    - This method will pass individual items from series to the func.
    - If there is some external data that needs to be used by the function, pass it as a list in static_data, and then accept that list in your func.
        You must have a named argument with name 'static_data' if you need to accept static data.
        Individual items in static_data list needs to be accessed using indexing.
    """        
    func = func if static_data is None else functools.partial(func, static_data=static_data)
    with Pool(num_processes) as p:
        ret_list = p.map(func, self.values.tolist())

    if isinstance(ret_list[0], pd.DataFrame) or isinstance(ret_list[0], pd.DataFrame):
        return pd.concat(ret_list, keys=self.index,names=self.index.name, axis=0)
    
    return pd.Series(ret_list, index=self.index)

pd.core.series.Series.apply_parallel = series_apply_parallel


def df_apply_parallel(self, func, static_data=None, num_processes = cpu_count(), axis=0):
    """
    Add functionality to pandas so that you can do processing on dataframes on multiple cores at same time.
    - This method will pass individual rows/columns from dataframe to the func.
    - If there is some external data that needs to be used by the function, pass it as a list in static_data, and then accept that list in your func.
        You must have a named argument with name 'static_data' if you need to accept static data.
        Individual items in static_data list needs to be accessed using indexing.
    - Pass axis=1 if you want to process on columns. By default, axis=0 i.e. each rows
    """          
    func = func if static_data is None else functools.partial(func, static_data=static_data)
    with Pool(num_processes) as p:
        if axis==0:
            ret_list = p.map(func, [row for _, row in self.iterrows()])
        elif axis==1:
            ret_list = p.map(func, [col for _, col in self.items()])
    if isinstance(ret_list[0], pd.DataFrame) or isinstance(ret_list[0], pd.DataFrame):
        if axis==0:
            return pd.concat(ret_list, keys=self.index,names=self.index.name, axis=0)
        elif axis==1:
            return pd.concat(ret_list, keys=self.columns, axis=0)
        
    
    return pd.Series(ret_list, index=self.index) if axis==0 else pd.Series(ret_list, index=self.columns)

pd.core.frame.DataFrame.apply_parallel = df_apply_parallel

