
# coding: utf-8

# <img src="images/dask_horizontal.svg" align="right" width="30%">

# # Table of Contents
# * [Data Storage](#Data-Storage)
# 	* &nbsp;
# 		* [Setup](#Setup)
# 		* [Read CSV](#Read-CSV)
# 		* [Write to HDF5](#Write-to-HDF5)
# 		* [Compare CSV to HDF5 speeds](#Compare-CSV-to-HDF5-speeds)
# 		* [1.  Store text efficiently with categoricals](#1.--Store-text-efficiently-with-categoricals)
# 		* [Exercise](#Exercise)
# 	* [Remote files](#Remote-files)
# 	* [Parquet](#Parquet)
# 

# # Data Storage

# <img src="images/hdd.jpg" width="20%" align="right">
# Efficient storage can dramatically improve performance, particularly when operating repeatedly from disk.
# 
# Decompressing text and parsing CSV files is expensive.  One of the most effective strategies with medium data is to use a binary storage format like HDF5.  Often the performance gains from doing this is sufficient so that you can switch back to using Pandas again instead of using `dask.dataframe`.
# 
# In this section we'll learn how to efficiently arrange and store your datasets in on-disk binary formats.  We'll use the following:
# 
# 1.  [Pandas `HDFStore`](http://pandas.pydata.org/pandas-docs/stable/io.html#io-hdf5) format on top of `HDF5`
# 2.  Categoricals for storing text data numerically
# 
# **Main Take-aways**
# 
# 1.  Storage formats affect performance by an order of magnitude
# 2.  Text data will keep even a fast format like HDF5 slow
# 3.  A combination of binary formats, column storage, and partitioned data turns one second wait times into 80ms wait times.

# In[1]:


# be sure to shut down other kernels running distributed clients
from dask.distributed import Client
client = Client()


# ## Setup

# Create data if we don't have any

# In[2]:


from prep import accounts_csvs
accounts_csvs(3, 1000000, 500)


# ## Read CSV

# First we read our csv data as before.
# 
# CSV and other text-based file formats are the most common storage for data from many sources, because they require minimal pre-processing, can be written line-by-line and are human-readable. Since Pandas' `read_csv` is well-optimized, CSVs are a reasonable input, but far from optimized, since reading required extensive text parsing.

# In[3]:


import os
filename = os.path.join('data', 'accounts.*.csv')
filename


# In[4]:


import dask.dataframe as dd
df_csv = dd.read_csv(filename)
df_csv.head()


# ### Write to HDF5

# HDF5 and netCDF are binary array formats very commonly used in the scientific realm.
# 
# Pandas contains a specialized HDF5 format, `HDFStore`.  The ``dd.DataFrame.to_hdf`` method works exactly like the ``pd.DataFrame.to_hdf`` method.

# In[5]:


target = os.path.join('data', 'accounts.h5')
target


# In[6]:


get_ipython().magic("time df_csv.to_hdf(target, '/data')")


# In[7]:


df_hdf = dd.read_hdf(target, '/data')
df_hdf.head()


# ### Compare CSV to HDF5 speeds

# We do a simple computation that requires reading a column of our dataset and compare performance between CSV files and our newly created HDF5 file.  Which do you expect to be faster?

# In[8]:


get_ipython().magic('time df_csv.amount.sum().compute()')


# In[9]:


get_ipython().magic('time df_hdf.amount.sum().compute()')


# Sadly they are about the same, or perhaps even slower. 
# 
# The culprit here is `names` column, which is of `object` dtype and thus hard to store efficiently.  There are two problems here:
# 
# 1.  How do we store text data like `names` efficiently on disk?
# 2.  Why did we have to read the `names` column when all we wanted was `amount`

# ### 1.  Store text efficiently with categoricals

# We can use Pandas categoricals to replace our object dtypes with a numerical representation.  This takes a bit more time up front, but results in better performance.
# 
# More on categoricals at the [pandas docs](http://pandas.pydata.org/pandas-docs/stable/categorical.html) and [this blogpost](http://matthewrocklin.com/blog/work/2015/06/18/Categoricals).

# In[10]:


# Categorize data, then store in HDFStore
get_ipython().magic("time df_hdf.categorize(columns=['names']).to_hdf(target, '/data2')")


# In[11]:


# It looks the same
df_hdf = dd.read_hdf(target, '/data2')
df_hdf.head()


# In[12]:


# But loads more quickly
get_ipython().magic('time df_hdf.amount.sum().compute()')


# This is significantly faster.  This tells us that it's not only the file type that we use but also how we represent our variables that influences storage performance. 
# 
# However this can still be better.  We had to read all of the columns (`names` and `amount`) in order to compute the sum of one (`amount`).  We'll improve further on this with `parquet`, an on-disk column-store.  First though we learn about how to set an index in a dask.dataframe.

# ### Exercise

# `fastparquet` is a library for interacting with parquet-format files, which are a very common format in the Big Data ecosystem, and used by tools such as Hadoop, Spark and Impala.

# In[13]:


target = os.path.join('data', 'accounts.parquet')
df_csv.categorize(columns=['names']).to_parquet(target, has_nulls=False)


# Investigate the file structure in the resultant new directory - what do you suppose those files are for?
# 
# `to_parquet` comes with many options, such as compression, whether to explicitly write NULLs information (not necessary in this case), and how to encode strings. You can experiment with these, to see what effect they have on the file size and the processing times, below.

# In[14]:


ls -l data/accounts.parquet/


# In[15]:


df_p = dd.read_parquet(target)
# note that column names shows the type of the values - we could
# choose to load as a categorical column or not.
df_p.dtypes


# Rerun the sum computation above for this version of the data, and time how long it takes.

# When archiving data, it is common to sort and partition by a column with unique identifiers, to facilitate fast look-ups later. For this data, that column is `id`. Time how long it takes to retrieve the rows corresponding to `id==100` from the raw CSV, from HDF5 and parquet versions, and finally from a new parquet version written after applying `set_index('id')`.

# In[16]:


# df_p.set_index('id').to_parquet(...)


# ## Remote files

# Dask can access Amazon S3 buckets or on HDFS
# 
# Advantages:
# * scalable, secure storage
# 
# Disadvantages:
# * network speed becomes bottleneck
# * only utilize local compute resources
#     * See dask.distributed
# 
# 

# For this we'll need s3fs.
# 
# ```
# conda install s3fs
# ```

# ```python
# taxi = dd.read_csv('s3://nyc-tlc/trip data/yellow_tripdata_2015-*.csv')
# ```

# **Warning**: operations over the Internet can take a long time to run. Such operations work really well in a cloud clustered set-up, e.g., amazon EC2 machines reading from S3 or Google compute machines reading from GCS.
