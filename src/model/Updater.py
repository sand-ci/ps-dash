import os
import os.path
import time
import threading
import traceback

from utils.parquet import Parquet
from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
import pandas as pd

from ml.create_thrpt_dataset import createThrptDataset
from ml.create_packet_loss_dataset import createPcktDataset

@timer
class ParquetUpdater(object):
    
    def __init__(self):
        self.location = 'parquet/'
        self.createLocation(self.location)
        self.createLocation('parquet/raw/')
        self.createLocation('parquet/frames/')
        self.createLocation('parquet/pivot/')
        self.createLocation('parquet/ml-datasets/')
        self.pq = Parquet()
        self.cacheIndexData()
        self.storeAlarms()
        self.storePathChangeDescDf()
        self.storeThroughputData()
        self.storePacketLossData()
        try:
            Scheduler(3600, self.cacheIndexData)
            Scheduler(1800, self.storeAlarms)
            Scheduler(1800, self.storePathChangeDescDf)

            # Store the data for the Major Alarms analysis
            Scheduler(int(60*60*12), self.storeThroughputData)
            Scheduler(int(60*60*12), self.storePacketLossData)
        except Exception as e:
            print(traceback.format_exc())

    @timer
    def queryData(self, idx, dateFrom, dateTo):
        intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/30)
        if idx in ['ps_throughput']:
            dateFrom, dateTo = hp.defaultTimeRange(21)
            intv = 42  # 12 hour bins

        data = []
        
        time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
        for i in range(len(time_list)-1):
            data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

        return data


    @timer
    def cacheIndexData(self):
        location = 'parquet/raw/'
        dateFrom, dateTo = hp.defaultTimeRange(1)
        INDICES = ['ps_packetloss', 'ps_owd', 'ps_throughput']
        measures = pd.DataFrame()
        for idx in INDICES:
            df = pd.DataFrame(self.queryData(idx, dateFrom, dateTo))
            # pq.writeToFile(df, f'{location}{idx}.parquet')
            df['idx'] = idx
            measures = pd.concat([measures, df])
        self.pq.writeToFile(measures, f'{location}measures.parquet')


    # @timer  
    # def cacheTraceChanges(self, days=60):
    #     location = 'parquet/raw/'
    #     dateFrom, dateTo = hp.defaultTimeRange(days)
    #     chdf, posDf, baseline, altPaths = qrs.queryTraceChanges(dateFrom, dateTo)
    #     chdf = chdf.round(2)
    #     posDf = posDf.round(2)
    #     baseline = baseline.round(2)
    #     altPaths = altPaths.round(2)

    #     self.pq.writeToFile(chdf, f'{location}chdf.parquet')
    #     self.pq.writeToFile(posDf, f'{location}posDf.parquet')
    #     self.pq.writeToFile(baseline, f'{location}baseline.parquet')
    #     self.pq.writeToFile(altPaths, f'{location}altPaths.parquet')


    @timer
    def storeAlarms(self):
        dateFrom, dateTo = hp.defaultTimeRange(60)
        print("Update data. Get all alarms for the past 60 days...", dateFrom, dateTo)
        oa = Alarms()
        frames, pivotFrames = oa.getAllAlarms(dateFrom, dateTo)
        
        for event,df in pivotFrames.items():
            filename = oa.eventCF(event)
            fdf = frames[event]
            if len(fdf)>0:
                self.pq.writeToFile(df, f"parquet/pivot/{filename}")
                self.pq.writeToFile(fdf, f"parquet/frames/{filename}")


    @staticmethod
    def descChange(chdf, posDf):
        owners = qrs.getASNInfo(posDf['asn'].values.tolist())
        owners['-1'] = 'OFF/Unavailable'
        howPathChanged = []
        for diff in chdf['diff'].values.tolist()[0]:

            for pos, P in posDf[(posDf['asn'] == diff)][['pos', 'P']].values:
                atPos = list(set(posDf[(posDf['pos'] == pos)]['asn'].values.tolist()))

                if len(atPos) > 0:
                    atPos.remove(diff)
                    for newASN in atPos:
                        if newASN not in [0, -1]:
                            if P < 1:
                                if str(newASN) in owners.keys():
                                    owner = owners[str(newASN)]
                                howPathChanged.append({'diff': diff,
                                            'diffOwner': owners[str(diff)], 'atPos': pos,
                                            'jumpedFrom': newASN, 'jumpedFromOwner': owner})
                # the following check is covering the cases when the change happened at the very end of the path
                # i.e. the only ASN that appears at that position is the diff detected
                if len(atPos) == 0:
                    howPathChanged.append({'diff': diff,
                                        'diffOwner': owners[str(diff)], 'atPos': pos,
                                        'jumpedFrom': 0, 'jumpedFromOwner': ''})

        if len(howPathChanged) > 0:
            return pd.DataFrame(howPathChanged).sort_values('atPos')

        return pd.DataFrame()


    @timer
    def storePathChangeDescDf(self):
        dateFrom, dateTo = hp.defaultTimeRange(days=4)
        chdf, posDf, baseline = qrs.queryTraceChanges(dateFrom, dateTo)[:3]

        df = pd.DataFrame()
        if len(chdf) > 0:
            for p in posDf['pair'].unique():
                # print(p)
                temp = self.descChange(chdf[chdf['pair'] == p], posDf[posDf['pair'] == p])
                temp['src_site'] = baseline[baseline['pair'] == p]['src_site'].values[0]
                temp['dest_site'] = baseline[baseline['pair'] == p]['dest_site'].values[0]
                temp['count'] = len(chdf[chdf['pair'] == p])
                df = pd.concat([df, temp])

            df['jumpedFrom'] = df['jumpedFrom'].astype(int)
            df['diff'] = df['diff'].astype(int)
            self.pq.writeToFile(df, f"parquet/frames/prev_next_asn")


    @staticmethod
    def createLocation(location):
        if os.path.isdir(location):
            print(location,"exists.")
        else:
            print(location, "doesn't exists. Creating...")
            os.mkdir(location)

    @timer
    def storeThroughputData(self):
        now = hp.defaultTimeRange(days=60, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        rawDf = createThrptDataset(start_date, end_date)

        self.pq.writeToFile(rawDf, f'{self.location}ml-datasets/rawDf.parquet')

    @timer
    def storePacketLossData(self):
        now = hp.defaultTimeRange(days=60, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        plsDf = createPcktDataset(start_date, end_date)

        self.pq.writeToFile(plsDf, f'{self.location}ml-datasets/plsDf.parquet')




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