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

class ParquetUpdater(object):
    
    def __init__(self):
        self.createLocation('parquet/')
        self.createLocation('parquet/raw/')
        self.createLocation('parquet/frames/')
        self.createLocation('parquet/pivot/')
        self.createLocation('parquet/ml-datasets/')
        self.pq = Parquet()
        self.cacheIndexData()
        self.storeAlarms()
        self.storePathChangeDescDf()
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
        if idx in ['ps_throughput','ps_retransmits']:
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
        INDICES = ['ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput']
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
        print("Update data. Get all alrms for the past 60 days...", dateFrom, dateTo)
        oa = Alarms()
        frames, pivotFrames = oa.getAllAlarms(dateFrom, dateTo)
        
        for event,df in pivotFrames.items():
            filename = oa.eventCF(event)
            fdf = frames[event]
            if len(fdf)>0:
                self.pq.writeToFile(df, f"parquet/pivot/{filename}")
                self.pq.writeToFile(fdf, f"parquet/frames/{filename}")

    @timer
    def storePrevNextASNData(self):
        dateFrom, dateTo = hp.defaultTimeRange(days=2)
        chdf, posDf, baseline = qrs.queryTraceChanges(dateFrom, dateTo)[:3]
        df = self.df4Sanckey(chdf, posDf, baseline)
        self.pq.writeToFile(df, f"parquet/frames/prev_next_asn")


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
                                howPathChanged.append({'diff': diff,
                                            'diffOwner': owners[str(diff)], 'atPos': pos,
                                            'jumpedFrom': newASN, 'jumpedFromOwner': owners[str(newASN)]})
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
        dateFrom, dateTo = hp.defaultTimeRange(days=3)
        chdf, posDf, baseline = qrs.queryTraceChanges(dateFrom, dateTo)[:3]
        posDf['asn'] = posDf['asn'].astype(int)

        df = pd.DataFrame()
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

    def storeThroughputData(self):
        location = 'parquet/ml-datasets/'

        now = hp.defaultTimeRange(days=90, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date} 00:01', f'{end_date} 23:59']

        rawDf = createThrptDataset(start_date, end_date)

        self.pq.writeToFile(rawDf, f'{location}rawDf.parquet')

    def storePacketLossData(self):
        location = 'parquet/ml-datasets/'

        now = hp.defaultTimeRange(days=90, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date} 00:01', f'{end_date} 23:59']

        plsDf = createPcktDataset(start_date, end_date)

        self.pq.writeToFile(plsDf, f'{location}plsDf.parquet')




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