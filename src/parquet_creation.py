import threading
import multiprocessing
import time
from time import time
import datetime
import pandas as pd
import itertools
from functools import reduce, wraps
from datetime import datetime, timedelta
import numpy as np
from  scipy.stats import zscore

import model.queries as qrs
from model.NodesMetaData import NodesMetaData
import utils.helpers as hp
from utils.helpers import timer

import urllib3
urllib3.disable_warnings()

import pyarrow as pa
import pyarrow.parquet as pq
import dask
import dask.dataframe as dd
from pandas.io.json import json_normalize
from fractions import Fraction
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
import asyncio




indices = ['ps_packetloss','ps_owd','ps_retransmits','ps_throughput'] 
limit = 4/24
  
    
async def queryfunc(idx,start,end):
    data = await qrs.getDataQuery(start,end,idx)
#     print('queryfunc wait over for ',idx)
    return data


async def createParquet(data,idx,p):
    async def write2Parquet(df, filename):
        table = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(table, filename)
    df = pd.DataFrame(data)
    finaldf = df.T
    await write2Parquet(finaldf,"..\\parquet\\"+idx+p)
#     print('Finished Parquet File for ', idx, ' for one hour range starting ',p,' hours ago.')
    
        
async def runall(idx,timerange):  
    print('Start Runall')
    
    for i in timerange:        
        data = await queryfunc(idx,i[0],i[1])
#         print('Query Wait over for ',idx,' starting ',i[2],' hours ago')
        await createParquet(data,idx,i[2])
    print('Done With Queries for ',idx)
    
    
def btwfunc(idx,timerange):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    loop.run_until_complete(runall(idx,timerange))
    loop.close()
    
    
def ReadParquet(idx,deflimit):
    print(idx)
    j = 1/24
    filenames = []
    while (j<=deflimit):
        p = str(int(j*24))
#             p = p.replace('/','_')
        timefile = "..\\parquet\\"+idx+p
        filenames += [timefile]
        j+=1/24
    return filenames


def queryrange(deflimit):
    timerange = []
    j=1/24
    k = 0
    while (j <= deflimit):
        p=str(int(j*24))
        start = hp.defaultTimeRange(days = j)
        end = hp.defaultTimeRange(days = k)
        period = hp.GetTimeRanges(start[0],end[0])
        timerange.append((period[0],period[1],p))
        k=j
        j+=1/24
    return timerange



if __name__ == "__main__":
    timerange = queryrange(limit)
    jobs = []
    for idx in indices:
        thread = threading.Thread(target=btwfunc,args=(idx,timerange))
        jobs.append(thread)
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
#     print('Finished Querying')
    for idx in indices:
        filenames = ReadParquet(idx,limit)
        if idx == 'ps_packetloss':
            print(filenames)
            plsdf = dd.read_parquet(filenames).compute()
            print('Before drops',len(plsdf))
            plsdf = plsdf.drop_duplicates()
            print('After Drops',len(plsdf))
            print('packetloss\n',plsdf)
        if idx == 'ps_owd':
            owddf = dd.read_parquet(filenames).compute()
            print('owd\n',owddf)
        if idx == 'ps_retransmits':
            rtmdf = dd.read_parquet(filenames).compute()
            print('retransmits\n',rtmdf)
        if idx == 'ps_throughput':
            trpdf = dd.read_parquet(filenames).compute()    
            print('throughput\n',trpdf)
        print('dask df complete')

        
        
        