
# coding: utf-8

# <img src="images/dask_horizontal.svg" align="right" width="30%">

# # Table of Contents
# * [Grounding](#Grounding)
# 	* [Prelude](#Prelude)
# 	* [Dask is a graph execution engine](#Dask-is-a-graph-execution-engine)
# 		* [Exercise](#Exercise)
# 	* [Appendix: Further detail and examples](#Appendix:-Further-detail-and-examples)
# 		* [Example 1: simple word count](#Example-1:-simple-word-count)
# 		* [Example 2: background execution](#Example-2:-background-execution)
# 		* [Example 3: delayed execution](#Example-3:-delayed-execution)
# 		* [Dask graphs](#Dask-graphs)
# 

# # Grounding

# In this section, we will take you through some basic understanding of what Dask is for, how it works, and some situations that it will be ideal for.

# ## Prelude

# As Python programmers, you probably already perform certain *tricks* to enable computation of larger-than-memory datasets, parallel execution or delayed/background execution. Perhaps with this phrasing, it is not clear what we mean, but a few examples should make things clearer. The point of Dask is to make simple things easy and complex things possible!
# 
# Aside from the [detailed introduction](http://dask.pydata.org/en/latest/), we can summarize the basics of Dask as follows:
# - process data that doesn't fit into memory by breaking it into blocks and specifying task chains
# - parallelize execution of tasks across cores and even nodes of a cluster
# - move computation to the data rather than the other way around, to minimize communication overheads
# 
# All of this allows you to get the most out of your computation resources, but program in a way that is very familiar: for-loops to build basic tasks, Python iterators, and the Numpy (array) and Pandas (dataframe) functions for multi-dimensional or tabular data, respectively.
# 
# The remainder of this notebook will take you through the first of these programming paradigms. This is more detail than some users will want, who can skip ahead to the iterator, array and dataframe sections; but there will be some data processing tasks that don't easily fit into those abstractions and need to fall back to the methods here.
# 
# We include a few examples at the end of the notebooks showing that the ideas behind how Dask is built are not actually that novel, and experienced programmers will have met parts of the design in other situations before. Those examples are left for the interested.

# ## Dask is a graph execution engine

# Dask allows you to construct a prescription for the calculation you want to carry out. That may sound strange, but a simple example will demonstrate that you can achieve this while programming with perfectly ordinary Python functions and for-loops.

# In[ ]:

from dask import delayed

@delayed
def inc(x):
    return x + 1

@delayed
def add(x, y):
    return x + y


# Here we have used the delayed annotation to show that we want these functions to operate lazily - to save the set of inputs and execute only on demand. `dask.delayed` is also a function which can do this, without the annotation, leaving the original function unchanged, e.g., 
# ```python
#     delayed_inc = delayed(inc)
# ```

# In[ ]:

# this looks like ordinary code
x = inc(15)
y = inc(30)
total = add(x, y)
# incx, incy and total are all delayed objects. 
# They contain a prescription of how to execute


# Calling a delayed function created a delayed object (`incx, incy, total`) - examine these interactively. Making these objects is somewhat equivalent to constructs like the `lambda` or function wrappers (see below). Each holds a simple dictionary describing the task graph, a full specification of how to carry out the computation.
# 
# We can visualize the chain of calculations that the object `total` corresponds to as follows; the circles are functions, rectangles are data/results.

# In[ ]:

total.visualize()


# But so far, no functions have actually been executed. To run the "graph" in the visualization, and actually get a result, do:

# In[ ]:

# execute all tasks
total.compute()


# **Why should you care about this?**
# 
# By building a specification of the calculation we want to carry out before executing anything, we can pass the specification to an *execution engine* for evaluation. In the case of Dask, this execution engine could be running on many nodes of a cluster, so you have access to the full number of CPU cores and memory across all the machines. Dask will intelligently execute your calculation with care for minimizing the amount of data held in memory, while parallelizing over the tasks that make up a graph. Notice that in the animated diagram below, where four workers are processing the (simple) graph, execution progresses vertically up the branches first, so that intermediate results can be expunged before moving onto a new branch.
# 
# With `delayed` and normal pythonic looped code, very complex graphs can be built up and passed on to Dask for execution. See a nice example of [simulated complex ETL](http://matthewrocklin.com/blog/work/2017/01/24/dask-custom) work flow.
# 
# <img src="images/grid_search_schedule.gif">

# ### Exercise

# Consider reading three CSV files with `pd.read_csv` and then measuring their total length. We will consider how you would do this with ordinary Python code, then build a graph for this process using delayed, and finally execute this graph using Dask, for a handy speed-up factor of more than two (there are only three inputs to parallelize over).

# In[ ]:

import pandas as pd
import os
filenames = [os.path.join('data', 'accounts.%d.csv' % i) for i in [0, 1, 2]]
filenames


# In[ ]:

get_ipython().run_cell_magic('time', '', '\n# normal, sequential code\na = pd.read_csv(filenames[0])\nb = pd.read_csv(filenames[1])\nc = pd.read_csv(filenames[2])\n\nna = len(a)\nnb = len(b)\nnc = len(c)\n\ntotal = sum([na, nb, nc])\nprint(total)')


# Your task is to recreate this graph again using the delayed function on the original Python code. The three functions you want to delay are `pd.read_csv`, `len` and `sum`.. 

# In[ ]:

from dask.multiprocessing import get

delayed_read_csv = delayed(pd.read_csv)
a = delayed_read_csv(filenames[0])
...

total = ...

# execute with multiprocessing scheduler
get_ipython().magic('time total.compute(get=get)')


# Next, repeat this using loops, rather than writing out all the variables.

# In[ ]:

get_ipython().magic('load solutions/Foundations-03.py')


# **Notes**
# 
# Delayed objects support various operations:
# ```python
#     x2 = x + 1
# ```

# if `x` was a delayed result (like `total`, above), then so is `x2`. Supported operations include arithmetic operators, item or slice selection, attribute access and method calls - essentially anything that could be phrased as a `lambda` expression.
# 
# Operations which are *not* supported include mutation, setter methods, iteration (for) and bool (predicate).

# ## Appendix: Further detail and examples

# The following examples show that the kinds of things Dask does are not so far removed from normal Python programming when dealing with big data. These examples are **only meant for experts**, typical users can continue with the next notebook in the tutorial.

# ### Example 1: simple word count

# This directory contains a file called `Dockerfile`. How would you count the number of words in that file?
# 
# The simplest approach would be to load all the data into memory, split on whitespace and count the number of results. Here we use a regular expression to split words.

# In[ ]:

import re
splitter = re.compile('\w+')
with open('README.md', 'r') as f:
    data = f.read()
result = len(splitter.findall(data))
result


# The trouble with this approach is that it does not scale - if the file is very large, it, and the generated list of words, might fill up memory. We can easily avoid that, because we only need a simple sum, and each line is totally independent of the others. Now we evaluate each piece of data and immediately free up the space again, so we could perform this on arbitrarily-large files. Note that there is often a trade-off between time-efficiency and memory footprint: the following uses very little memory, but may be slower for files that do not fill a large faction of memory. In general, one would like chunks small enough not to stress memory, but big enough for efficient use of the CPU.

# In[ ]:

result = 0
with open('README.md', 'r') as f:
    for line in f:
        result += len(splitter.findall(line))
result


# ### Example 2: background execution

# There are many tasks that take a while to complete, but don't actually require much of the CPU, for example anything that requires communication over a network, or input from a user. In typical sequential programming, execution would need to halt while the process completes, and then continue execution. That would be dreadful for a user experience (imagine the slow progress bar that locks up the application and cannot be canceled), and wasteful of time (the CPU could have been doing useful work in the meantime.
# 
# For example, we can launch processes and get their output as follows:
# ```python
#     import subprocess
#     p = subprocess.Popen(command, stdout=subprocess.PIPE)
#     p.returncode
# ```

# The task is run in a separate process, and the return-code will remain `None` until it completes, when it will change to `0`. To get the result back, we need `out = p.communicate()[0]` (which would block if the process was not complete).

# Similarly, we can launch Python processes and threads in the background. Some methods allow mapping over multiple inputs and gathering the results, more on that later.  The thread starts and the cell completes immediately, but the data associated with the download only appears in the queue object some time later.

# In[ ]:

import threading
import queue
import urllib

def get_webdata(url, q):
    u = urllib.request.urlopen(url)
    q.put(u.read())

q = queue.Queue()
t = threading.Thread(target=get_webdata, args=('http://www.google.com', q))
t.start()


# In[ ]:

# fetch result back into this thread. If the worker thread is not done, this would wait.
q.get()


# ### Example 3: delayed execution

# There are many ways in Python to specify the computation you want to execute, but only run it *later*.

# In[ ]:

def add(x, y):
    return x + y

# Sometimes we defer computations with strings
x = 15
y = 30
z = "add(x, y)"
eval(z)


# In[ ]:

# we can use lambda or other "closure"
x = 15
y = 30
z = lambda: add(x, y)
z()


# In[ ]:

# A very similar thing happens in functools.partial

import functools
z = functools.partial(add, x, y)
z()


# In[ ]:

# Python generators are delayed execution by default
# Many Python functions expect such iterable objects

def gen():
    res = x
    yield res
    res += y
    yield y

g = gen()


# In[ ]:

# run once: we get one value and execution halts within the generator
# run again and the execution completes
next(g)


# ### Dask graphs

# Any Dask object, such as `total`, above, has an attribute which describes the calculations necessary to produce that result. Indeed, this is exactly the graph that we have been talking about, which can be visualized. We see that it is a simple dictionary, the keys are unique task identifiers, and the values are the functions and inputs for calculation.
# 
# `delayed` is a handy mechanism for creating the Dask graph, but the adventerous may wish to play with the full fexibility afforded by building the graph dictionaries directly. Detailed information can be found [here](http://dask.pydata.org/en/latest/graphs.html).

# In[ ]:

total.dask


# In[ ]:

dict(total.dask)

