
# coding: utf-8

# <img src="images/dask_horizontal.svg" align="right" width="30%">

# # Table of Contents
# * [Bag: Parallel Lists for semi-structured data](#Bag:-Parallel-Lists-for-semi-structured-data)
# 	* [Creation](#Creation)
# 	* [Manipulation](#Manipulation)
# 		* [Exercise: Accounts JSON data](#Exercise:-Accounts-JSON-data)
# 		* [Basic Queries](#Basic-Queries)
# 		* [Use `flatten` to de-nest](#Use-flatten-to-de-nest)
# 		* [Groupby and Foldby](#Groupby-and-Foldby)
# 		* [`groupby`](#groupby)
# 		* [`foldby`](#foldby)
# 		* [Example with account data](#Example-with-account-data)
# 		* [Exercise: compute total amount per name](#Exercise:-compute-total-amount-per-name)
# 	* [DataFrames](#DataFrames)
# 		* [Denormalization](#Denormalization)
# 	* [Limitations](#Limitations)
# 

# # Bag: Parallel Lists for semi-structured data

# Dask-bag excels in processing data that can be represented as a sequence of arbitrary inputs. We'll refer to this as "messy" data, because it can contain complex nested structures, missing fields, mixtures of data types, etc. The *functional* programming style fits very nicely with standard Python iteration, such as can be found in the `itertools` module.
# 
# Messy data is often encountered at the beginning of data processing pipelines when large volumes of raw data are first consumed. The initial set of data might be JSON, CSV, XML, or any other format that does not enforce strict structure and datatypes.
# For this reason, the initial data massaging and processing is often done with Python `list`s, `dict`s, and `set`s.
# 
# These core data structures are optimized for general-purpose storage and processing.  Adding streaming computation with iterators/generator expressions or libraries like `itertools` or [`toolz`](https://toolz.readthedocs.io/en/latest/) let us process large volumes in a small space.  If we combine this with parallel processing then we can churn through a fair amount of data.
# 
# Dask.bag is a high level Dask collection to automate common workloads of this form.  In a nutshell
# 
#     dask.bag = map, filter, toolz + parallel execution
#     
# **Related Documentation**
# 
# *  [Bag Documenation](http://dask.pydata.org/en/latest/bag.html)
# *  [Bag API](http://dask.pydata.org/en/latest/bag-api.html)

# ## Creation

# You can create a `Bag` from a Python sequence, from files, from data on S3, etc..

# In[94]:


# each element is an integer
import dask.bag as db
b = db.from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])


# In[95]:


# each element is a text file of JSON lines
import os
b = db.read_text(os.path.join('data', 'accounts.*.json.gz'))


# In[96]:


# Requires `s3fs` library
# each element is a remote CSV text file
b = db.read_text('s3://dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv')


# ## Manipulation

# `Bag` objects hold the standard functional API found in projects like the Python standard library, `toolz`, or `pyspark`, including `map`, `filter`, `groupby`, etc..
# 
# As with `Array` and `DataFrame` objects, operations on `Bag` objects create new bags.  Call the `.compute()` method to trigger execution.  
# 
# Dask.bag uses `dask.multiprocessing.get` by default.  For examples using dask.bag with a distributed cluster see 
# 
# *  [Dask.distributed docs](http://distributed.readthedocs.io/en/latest/)
# *  [Blogpost analyzing github data on EC2](https://www.continuum.io/content/dask-distributed-and-anaconda-cluster)

# In[7]:


def is_even(n):
    return n % 2 == 0

b = db.from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
c = b.filter(is_even).map(lambda x: x ** 2)
c


# In[8]:


# blocking form: wait for completion (which is very fast in this case)
c.compute()


# ### Exercise: Accounts JSON data

# We've created a fake dataset of gzipped JSON data in your data directory.  This is like the example used in the `DataFrame` example we will see later, except that it has bundled up all of the entires for each individual `id` into a single record.  This is similar to data that you might collect off of a document store database or a web API.
# 
# Each line is a JSON encoded dictionary with the following keys
# 
# *  id: Unique identifier of the customer
# *  name: Name of the customer
# *  transactions: List of `transaction-id`, `amount` pairs, one for each transaction for the customer in that file

# In[9]:


filename = os.path.join('data', 'accounts.*.json.gz')
lines = db.read_text(filename)
lines.take(3)


# Our data comes out of the file as lines of text. Notice that file decompression happened automatically. We can make this data look more reasonable by mapping the `json.loads` function onto our bag.

# In[10]:


import json
js = lines.map(json.loads)
# take: inspect first few elements
js.take(3)


# ### Basic Queries

# Once we parse our JSON data into proper Python objects (`dict`s, `list`s, etc.) we can perform more interesting queries by creating small Python functions to run on our data.

# In[11]:


# filter: keep only some elements of the sequence
js.filter(lambda record: record['name'] == 'Alice').take(5)


# In[12]:


def count_transactions(d):
    return {'name': d['name'], 'count': len(d['transactions'])}

# map: apply a function to each element
(js.filter(lambda record: record['name'] == 'Alice')
   .map(count_transactions)
   .take(5))


# In[13]:


# pluck: select a field, as from a dictionary, element[field]
(js.filter(lambda record: record['name'] == 'Alice')
   .map(count_transactions)
   .pluck('count')
   .take(5))


# In[14]:


# Average number of transactions for all of the Alice's
(js.filter(lambda record: record['name'] == 'Alice')
   .map(count_transactions)
   .pluck('count')
   .mean()
   .compute())


# ### Use `flatten` to de-nest

# In the example below we see the use of `.flatten()` to flatten results.  We compute the average amount for all transactions for all Alices.

# In[15]:


js.filter(lambda record: record['name'] == 'Alice').pluck('transactions').take(3)


# In[16]:


(js.filter(lambda record: record['name'] == 'Alice')
   .pluck('transactions')
   .flatten()
   .take(3))


# In[17]:


(js.filter(lambda record: record['name'] == 'Alice')
   .pluck('transactions')
   .flatten()
   .pluck('amount')
   .take(3))


# In[18]:


(js.filter(lambda record: record['name'] == 'Alice')
   .pluck('transactions')
   .flatten()
   .pluck('amount')
   .mean()
   .compute())


# ### Groupby and Foldby

# Often we want to group data by some function or key.  We can do this either with the `.groupby` method, which is straightforward but forces a full shuffle of the data (expensive) or with the harder-to-use but faster `.foldby` method, which does a streaming combined groupby and reduction.
# 
# *  `groupby`:  Shuffles data so that all items with the same key are in the same key-value pair
# *  `foldby`:  Walks through the data accumulating a result per key
# 
# *Note: the full groupby is particularly bad. In actual workloads you would do well to use `foldby` or switch to `DataFrame`s if possible.*

# ### `groupby`

# Groupby collects items in your collection so that all items with the same value under some function are collected together into a key-value pair.

# In[19]:


b = db.from_sequence(['Alice', 'Bob', 'Charlie', 'Dan', 'Edith', 'Frank'])
b.groupby(len).compute()  # names grouped by length


# In[20]:


b = db.from_sequence(list(range(10)))
b.groupby(lambda x: x % 2).compute()


# In[21]:


b.groupby(lambda x: x % 2).starmap(lambda k, v: (k, max(v))).compute()


# ### `foldby`

# Foldby can be quite odd at first.  It is similar to the following functions from other libraries:
# 
# *  [`toolz.reduceby`](http://toolz.readthedocs.io/en/latest/streaming-analytics.html#streaming-split-apply-combine)
# *  [`pyspark.RDD.combineByKey`](http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/)
# 
# When using `foldby` you provide 
# 
# 1.  A key function on which to group elements
# 2.  A binary operator such as you would pass to `reduce` that you use to perform reduction per each group
# 3.  A combine binary operator that can combine the results of two `reduce` calls on different parts of your dataset.
# 
# Your reduction must be associative.  It will happen in parallel in each of the partitions of your dataset.  Then all of these intermediate results will be combined by the `combine` binary operator.

# In[22]:


is_even = lambda x: x % 2
b.foldby(is_even, binop=max, combine=max).compute()


# ### Example with account data

# We find the number of people with the same name.

# In[23]:


get_ipython().run_cell_magic('time', '', "# Warning, this one takes a while...\nresult = js.groupby(lambda item: item['name']).starmap(lambda k, v: (k, len(v))).compute()\nprint(sorted(result))")


# In[81]:


get_ipython().run_cell_magic('time', '', "# This one is comparatively fast and produces the same result.\nfrom operator import add\ndef incr(tot, smt):\n    return tot+1\n\nresult = js.foldby(key='name', \n                   binop=incr, \n                   initial=0, \n                   combine=add, \n                   combine_initial=0).compute()\nprint(sorted(result))")


# ### Exercise: compute total amount per name

# We want to groupby (or foldby) the `name` key, then add up the all of the amounts for each name.
# 
# Steps
# 
# 1.  Create a small function that, given a dictionary like 
# 
#         {'name': 'Alice', 'transactions': [{'amount': 1, 'id': 123}, {'amount': 2, 'id': 456}]}
#         
#     produces the sum of the amounts, e.g. `3`
#     
# 2.  Slightly change the binary operator of the `foldby` example above so that the binary operator doesn't count the number of entries, but instead accumulates the sum of the amounts.

# In[104]:


from operator import add

def sum_amounts(i, z):
    return i + sum([y["amount"] for y in z["transactions"]])

result = js.foldby(key='name', 
                   binop=sum_amounts, 
                   initial=0, 
                   combine=add, 
                   combine_initial=0).compute()

print(sorted(result))


# ## DataFrames

# For the same reasons that Pandas is often faster than pure Python, `dask.dataframe` can be faster than `dask.bag`.  We will work more with DataFrames later, but from for the bag point of view, they are frequently the end-point of the "messy" part of data ingestion—once the data can be made into a data-frame, then complex split-apply-combine logic will become much more straight-forward and efficient.
# 
# You can transform a bag with a simple tuple or flat dictionary structure into a `dask.dataframe` with the `to_dataframe` method.

# In[83]:


df1 = js.to_dataframe()
df1.head()


# This now looks like a well-defined DataFrame, and we can apply Pandas-like computations to it efficiently.

# Using a Dask DataFrame, how long does it take to do our prior computation of numbers of people with the same name?  It turns out that `dask.dataframe.groupby()` beats `dask.bag.groupby()` more than an order of magnitude; but it still cannot match `dask.bag.foldby()` for this case.

# In[84]:


get_ipython().magic("time df1.groupby('name').id.count().compute().head()")


# ### Denormalization

# This DataFrame format is less-than-optimal because the `transactions` column is filled with nested data so Pandas has to revert to `object` dtype, which is quite slow in Pandas.  Ideally we want to transform to a dataframe only after we have flattened our data so that each record is a single `int`, `string`, `float`, etc..

# In[85]:


def denormalize(record):
    # returns a list for every nested item, each transaction of each person
    return [{'id': record['id'], 
             'name': record['name'], 
             'amount': transaction['amount'], 
             'transaction-id': transaction['transaction-id']}
            for transaction in record['transactions']]

transactions = js.map(denormalize).flatten()
transactions.take(3)


# In[86]:


df = transactions.to_dataframe()
df.head()


# In[87]:


get_ipython().run_cell_magic('time', '', "# number of transactions per name\n# note that the time here includes the data load and ingestion\ndf.groupby('name')['transaction-id'].count().compute()")


# ## Limitations

# Bags provide very general computation (any Python function.)  This generality
# comes at cost.  Bags have the following known limitations
# 
# 1.  Bag operations tend to be slower than array/dataframe computations in the
#     same way that Python tends to be slower than NumPy/Pandas
# 2.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
#     Using ``Bag.foldby`` requires more thought. Even better, consider creating
#     a normalised dataframe.
