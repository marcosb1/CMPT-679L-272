
# coding: utf-8

# <img src="images/dask_horizontal.svg" align="right" width="30%">

# # Table of Contents
# * [Distributed, Advanced](#Distributed,-Advanced)
# 	* [Distributed futures](#Distributed-futures)
# 		* [Persist](#Persist)
# 	* [Debugging](#Debugging)
# 

# # Distributed, Advanced

# ## Distributed futures

# In[1]:


# be sure to shut down other kernels running distributed clients
from dask.distributed import Client
c = Client()


# In chapter Distributed, we showed that executing a calculation (created using delayed) with the distributed executor is identical to any other executor. However, we now have access to additional functionality, and control over what data is held in memory.
# 
# To begin, the `futures` interface (derived from the built-in `concurrent.futures`) allow map-reduce like functionality. We can submit individual functions for evaluation with one set of inputs, or evaluated over a sequence of inputs with `submit()` and `map()`. Notice that the call returns immediately, giving one or more *futures*, whose status begins as "pending" and later becomes "finished". There is no blocking of the local Python session.

# Here is the simplest example of `submit` in action:

# In[2]:


def inc(x):
    return x+1

fut = c.submit(inc, 1)
fut


# We can re-execute the following cell as often as we want as a way to poll the status of the future. This could of course be done in a loop, pausing for a short time on each iteration. We could continue with our work, or view a progressbar of work still going on, or force a wait until the future is ready.

# In[3]:


# functions runs on the cluster for a while, we have a local handle to
# that work
fut

# try the preceding cell again, but with
# distributed.diagnostics.progress(fut)
# or
# distributed.client.wait(fut)


# In[4]:


# grab the information back - this blocks if fut is not ready
c.gather(fut)


# Here we see an alternative way to execute work on the cluster: when you submit or map with the inputs as futures, the *computation moves to the data* rather than the other way around, and the client, in the local Python session, need never see the intermediate values. This is similar to building the graph using delayed, and indeed, delayed can be used in conjunction with futures. Here we use the delayed object `total` from before.

# In[5]:


# Some trivial work that takes time
# repeated from the Distributed chapter.

from dask import delayed
import time

def inc(x):
    time.sleep(5)
    return x + 1

def dec(x):
    time.sleep(3)
    return x - 1

def add(x, y):
    time.sleep(7)
    return x + y

x = delayed(inc)(1)
y = delayed(dec)(2)
total = delayed(add)(x, y)


# In[6]:


# notice the difference from total.compute()
# notice that this cell completes immediately
fut = c.compute(total)


# In[7]:


c.gather(fut)


# Critically, each futures represents a result held, or being evaluated by the cluster. Thus we can control caching of intermediate values - when a future is no longer referenced, its value is forgotten. For example, although we can explicitly pass data into the cluster using `scatter()`, we normally want to cause the workers to load as much of their own data as possible to avoid excessive communication overhead. 
# 
# The [full API](http://distributed.readthedocs.io/en/latest/api.html) of the distributed scheduler gives details of interacting with the cluster, which remember, can be on your local machine or possibly on a massive computational resource. 

# The futures API offers a work submission style that can easily emulate the map/reduce paradigm (see `c.map()`) that may be familiar to many people. The intermediate results, represented by futures, can be passed to new tasks without having to bring the pull locally from the cluster, and new work can be assigned to work on the output of previous jobs that haven't even begun yet.
# 
# Generally, any Dask operation that is executed using `.compute()` can be submitted for asynchronous execution using `c.compute()` instead, and this applies to all collections. Here is an example with the calculation previously seen in the Bag chapter. We have replaced the `.compute()` method there with the distributed client version, so, again, we could continue to submit more work (perhaps based on the result of the calculation), or, in the next cell, follow the progress of the computation. A similar progress-bar appears in the monitoring UI page.

# In[8]:


import dask.bag as db
import os
import json
from dask import distributed
filename = os.path.join('data', 'accounts.*.json.gz')
lines = db.read_text(filename)
js = lines.map(json.loads)

f = c.compute(js.filter(lambda record: record['name'] == 'Alice').pluck('transactions').flatten().pluck('amount').mean())


# In[9]:


# note that progress must be the last line of a cell
# in order to show up
distributed.diagnostics.progress(f)


# In[10]:


c.gather(f)


# ### Persist

# Considering which data should be loaded by the workers, as opposed to passed, and which intermediate values to persist in worker memory, will in many cases determine the computation efficiency of a process.
# 
# In the example here, we repeat a calculation from the Array chapter - notice that each call to `compute()` is roughly the same speed, because the loading of the data is included every time.

# In[11]:


import h5py
import os
f = h5py.File(os.path.join('data', 'random.hdf5'), mode='r')
dset = f['/x']
import dask.array as da
x = da.from_array(dset, chunks=(1000000,))

get_ipython().magic('time x.sum().compute()')
get_ipython().magic('time x.sum().compute()')


# If, instead, we persist the data to RAM up front (this takes a few seconds to complete - we could `wait()` on this process), then further computations will be much faster.

# In[12]:


# changes x from a set of delayed prescritions
# to a set of futures pointing to data in RAM
# See this on the UI dashboard.
x = c.persist(x)


# In[13]:


get_ipython().magic('time x.sum().compute()')
get_ipython().magic('time x.sum().compute()')


# Naturally, persisting every intermediate along the way is a bad idea, because this will tend to fill up all available RAM and make the whole system slow (or break!). The ideal persist point is often at the end of a set of data cleaning steps, when the data is in a form which will get queried often. 

# **Exercise**: how is the memory associated with `x` released, once we know we are done with it?

# The worker assumes that when `.compute()` is called, we no longer need the data. Thus, sending a message to RAM to clear that block in memory.

# ## Debugging

# When something goes wrong in a distributed job, it is hard to figure out what the problem was and what to do about it. When a task raises an exception, the exception will show up when that result, or other result that depend upon it, is gathered.
# 
# Consider the following delayed calculation to be computed by the cluster. As usual, we get back a future, which the cluster is working on to compute (this happens very slowly for the trivial procedure).

# In[14]:


@delayed
def ratio(a, b):
    return a // b

@delayed
def summation(*a):
    return sum(*a)

ina = [5, 25, 30]
inb = [5, 5, 6]
out = summation([ratio(a, b) for (a, b) in zip(ina, inb)])
f = c.compute(out)
f


# We only get to know what happened when we gather the result (this is also true for `out.compute()`, except we could not have done other stuff in the meantime). For the first set of inputs, it works fine.

# In[15]:


c.gather(f)


# But if we introduce bad input, an exception is raised. The exception happens in `ratio`, but only comes to our attention when calculating `summation`.

# In[16]:


ina = [5, 25, 30]
inb = [5, 0, 6]
out = summation([ratio(a, b) for (a, b) in zip(ina, inb)])
f = c.compute(out)
c.gather(f)


# The display in this case makes the origin of the exception obvious, but this is not always the case. How should this be debugged, how would we go about finding out the exact conditions that caused the exception? 
# 
# The first step, of course, is to write well-tested code which makes appropriate assertions about its input and clear warnings and error messages when something goes wrong. This applies to all code.
# 
# The most typical thing to do is to execute some portion of the computation in the local thread, so that we can run the Python debugger and query the state of things at the time that the exception happened. Obviously, this cannot be performed on the whole data-set when dealing with Big Data on a cluster, but a suitable sample will probably do even then.

# In[ ]:


with dask.set_options(get=dask.async.get_sync):
    # do NOT use c.compute(out) here - we specifically do not
    # want the distributed scheduler
    out.compute()


# In[ ]:


debug


# The trouble with this approach is that Dask is meant for the execution of large datasets/computations - you probably can't simply run the whole thing 
# in one local thread, else you wouldn't have used Dask in the first place. So the code above should only be used on a small part of the data that also exchibits the error. 
# Furthermore, the method will not work when you are dealing with futures (such as `f`, above, or after persisting) instead of delayed-based computations.
# 
# As alternative, you can ask the scheduler to analyze your calculation and find the specific sub-task responsible for the error, and pull only it and its dependnecies locally for execution.

# In[17]:


c.recreate_error_locally(f)


# In[ ]:


debug


# Finally, there are errors other than exceptions, when we need to look at the state of the scheduler/workers. In the standard "LocalCluster" we started, we
# have direct access to these.
# 

# In[19]:


c.cluster.scheduler.nbytes


# Or we could start ipython processed in remote workers/schedulers to enable examining their states from an interactive session.

# In[20]:


workdict = c.start_ipython_workers()


# In[21]:


w0 = list(workdict.values())[0]


# In[22]:


get_ipython().magic('remote w0 worker.keys()')

