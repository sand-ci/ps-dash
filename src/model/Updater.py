import os
import os.path
import time
import threading
import traceback

from utils.parquet import Parquet
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
import pandas as pd



class ParquetUpdater(object):
    
    def __init__(self, iterval):
        self.createLocation('parquet/')
        self.createLocation('parquet/raw/')
        self.cacheData()
        try:
            Scheduler(iterval, self.cacheData)
        except Exception as e:
            print(traceback.format_exc())

    @timer
    def queryData(self, idx, dateFrom, dateTo):
        intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/30)
        if idx in ['ps_throughput','ps_retransmits']:
            dateFrom, dateTo = hp.defaultTimeRange(21)
            intv = 42  # 12 hour bins

        data = []
        
        time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
        for i in range(len(time_list)-1):
            data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

        return data


    @timer
    def cacheData(self):
        pq = Parquet()
        location = 'parquet/raw/'
        dateFrom, dateTo = hp.defaultTimeRange(1)
        INDICES = ['ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput']
        measures = pd.DataFrame()
        for idx in INDICES:
            df = pd.DataFrame(self.queryData(idx, dateFrom, dateTo))
            # pq.writeToFile(df, f'{location}{idx}.parquet')
            df['idx'] = idx
            measures = pd.concat([measures, df])
        pq.writeToFile(measures, f'{location}measures.parquet')


    def createLocation(self,location):
        if os.path.isdir(location):
            print(location,"exists.")
        else:
            print(location, "doesn't exists. Creating...")
            os.mkdir(location)




class Scheduler(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.next_call = time.time()
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False