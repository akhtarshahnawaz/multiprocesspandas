# MultiprocessPandas

MultiprocessPandas package extends functionality of Pandas to easily run operations on multiple cores i.e. parallelize the operations. The current version of the package provides capability to parallelize ***apply()*** methods on DataFrames, Series and DataFrameGroupBy .

Importing the applyparallel module will add ***apply_parallel()*** method to DataFrame, Series and DataFrameGroupBy, which will allow you to run operation on multiple cores.

## Installation
The source code is currently hosted on GitHub at: [https://github.com/akhtarshahnawaz/multiprocesspandas](https://github.com/akhtarshahnawaz/multiprocesspandas). The package can be pulled from GitHub or can be installed from PyPi directly. 

To install using pip
```python
    pip install multiprocesspandas
```
## Setting up the Library
To use the library, you have to import applyparallel module. Import will attach required methods to pandas, and you can call them directly on Pandas data objects. 
```python
    from multiprocesspandas import applyparallel
```
## Usage
Once imported, the library adds functionality to call ***apply_parallel()*** method on your DataFrame, Series or DataFrameGroupBy . The methods accepts a function that has to be applied, and two named arguments:

 - ***static_data*** (External Data required by passed function, defaults to None)
 - ***num_processes*** (Defaults to maximum available cores on your CPU)
 - ***axis*** (Only for DataFrames, defaults to 0 i.e. rows. For columns, set axis=1.

***Note:** Any extra module required by the passed function must be re-imported again inside the function.*

### Usage with DataFrameGroupBy 
```python
    def func(x):
        import pandas as pd
        return pd.Series([x['C'].mean()])

    df.groupby(["A","B"]).apply_parallel(func, num_processes=30)
```
If you need some external data inside **func()**, it has to be passed and received as position argumnets or keyword arguments.
```python
    data1 = pd.Series([1,2,3])
    data2 = 20
    
    def func(x, static_data):
        import pandas as pd
        output = static_data[0] - x['C'].mean()
        return output * static_data[1]
	
    df.groupby(["A","B"]).apply_parallel(func, static_data, num_processes=30)
```
### Usage with DataFrame
Usage with DataFrames is very similar to the one with DataFrameGroupBy, however you have to pass an extra argument 'axis' which tells whether to apply function on  the rows or the columns.
```python
    def func(x):
        return x.mean()

    df.apply_parallel(func, num_processes=30, axis=1)
```
External data can be passed in same way as we did in DataFrameGroupBy
```python
    data = pd.Series([1,2,3])
    
    def func(x, static_data):
        return static_data.sum() + x.mean()
	
    df.apply_parallel(func, static_data, num_processes=30)
```
### Usage with Series
Usage with Series is very similar to the usage with DataFrames and DataFrameGroupBy.
```python
    data = pd.Series([1,2,3])
    
    def func(x, static_data):
	    return static_data-x
    
    series.apply_parallel(func, static_data, num_processes=30)
```
