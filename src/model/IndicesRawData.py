# from datetime import datetime, timedelta
# import numpy as np
# import pandas as pd
# import glob
# import os
# import dask.dataframe as dd

# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# import itertools
# import re
# import time

# import model.queries as qrs
# import utils.helpers as hp
# from utils.parquet import Parquet


# import logging
# logging.basicConfig(format = '%(asctime)s %(message)s',
#                     datefmt = '%m/%d/%Y %I:%M:%S %p',
#                     filename = 'progress.log',
#                     level=logging.INFO)


# class IndicesRawData(object):

#     def __init__(self):
#         self.location = 'parquet/raw/'
#         self.hours = self.__getLimit()
#         self.timerange = self.__queryrange(self.hours)
#         self.__run()


#     def __startThreads(self,idx):
#         with ThreadPoolExecutor(max_workers=4) as pool:
#             result = pool.map(self.__createParquet, list(itertools.product([idx], self.timerange)))


#     def __run(self):
#         with ProcessPoolExecutor(max_workers=5) as pool:
#             logging.info('Starting parallel processing....')
#             result = pool.map(self.__startThreads, hp.INDICES)


#     # get time ranges rounded up the hour
#     def __queryrange(self, defhours):
#         timerange = []
#         j=1/24
#         k = 0
#         while (j <= defhours):
#             p=str(round(j*24))
#             start = hp.defaultTimeRange(days = j)
#             end = hp.defaultTimeRange(days = k)
#             period = hp.GetTimeRanges(start[0],end[0])
#             timerange.append((period[0],period[1],p))
#             k=j
#             j+=1/24
# #         print(timerange)
#         return timerange


#     # save the data to a Parquet file
#     def __createParquet(self, param):
#         idx = param[0]
#         start, end, num = param[1]
#         data = qrs.queryIndex(start, end, idx)
#         df = pd.DataFrame(data)
#         pq = Parquet()
#         pq.writeToFile(df.T, f'{self.location}{idx}{num}')
#         logging.info(f'CHECK LATER \n {self.location}{idx}{num} \n {start} - {end}')


#     # shift the old files' names; delete the oldest and the most recent
#     def __renameFiles(self, files):
#         files = sorted(files, reverse=True)
#         logging.info('Renaming............')
#         fn = {}
#         for f in files:
#             num = re.findall('[0-9]+', f)[0]
#             idx = f[0:f.index(num)]
#             newNum = int(num)+1
#             final = f'{idx}{newNum}'
#             os.rename(f,final)

#             if idx in fn.keys():
#                 temp = fn[idx]
#                 temp.append(newNum)
#             else: fn[idx] = [newNum]

#         for idx in fn.keys():
#             # Here the oldest file gets deleted, bacause we want to keep only the last 24 hours of data
#             # The latest is also deleted in order to preserve the consistency of the data in case the latest file didn't cover the whole hour
#             oldestFile = f'{idx}{max(fn[idx])}'
#             latestFile = f'{idx}{min(fn[idx])}'
#             logging.info(f'Deleting the oldest and latest...... \n{oldestFile} & {latestFile}')
#             os.remove(oldestFile)
#             os.remove(latestFile)


#     # make a decision what period to query for based on the existing data in parquet/raw/
#     def __getLimit(self):
#         indices = hp.INDICES
#         files = glob.glob(f'{self.location}*')

#         timerange = self.__queryrange(4/24)
#         if len(files)>5:
#             df = dd.read_parquet(f'{self.location}ps_trace1').compute()
# #             df.loc[345, 'timestamp'] = 999999999999999
#         #     if recent timestamp is older that 1 hour ago, clear files and load the past 24 hours
#             if df.timestamp.max()<timerange[2][1]:
#                 logging.info('Data is too old. Clear and load the most recent data......')
#                 [os.remove(f) for f in files]
#                 hours = 24/24
#             else: 
#                 logging.info('Grab only the past hour of data.......')
#                 hours = 2/24
#                 self.__renameFiles(files)
#         else:
#             hours = 24/24
#             logging.info('Load fresh data.......')
        
#         return hours