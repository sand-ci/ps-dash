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
import pickle

from ml.create_thrpt_dataset import createThrptDataset
from ml.thrpt_dataset_model_train import trainMLmodel
from ml.create_packet_loss_dataset import createPcktDataset
from ml.packet_loss_one_month_onehot import one_month_data
from ml.packet_loss_train_model import packet_loss_train_model
import os
from datetime import datetime, timedelta

@timer
class ParquetUpdater(object):
    
    def __init__(self):
        self.pq = Parquet()
        self.location = 'parquet/'
        self.createLocation(self.location)

        folders = ['raw', 'frames', 'pivot', 'ml-datasets'] 
        for folder in folders:
            self.createLocation(self.location + folder + '/')

        # Prevent the data from being updated if it is fresh
        if self.__isDataFresh(self.location) == False:
            print("Data is too old or folders are empty. Updating...")
            self.storeMetaData()
            self.cacheIndexData()
            self.storeAlarms()
            self.storePathChangeDescDf()
            self.storeThroughputDataAndModel()
            self.storePacketLossDataAndModel()

        try:
            Scheduler(60*60, self.cacheIndexData)
            Scheduler(60*10, self.storeAlarms)
            Scheduler(60*30, self.storePathChangeDescDf)
            Scheduler(int(60*60*12), self.storeMetaData)

            # Store the data for the Major Alarms analysis
            Scheduler(int(60*60*12), self.storeThroughputDataAndModel)
            Scheduler(int(60*60*12), self.storePacketLossDataAndModel)
        except Exception as e:
            print(traceback.format_exc())


    # The following function is used to group alarms by site 
    # taking into account the most recent 24 hours only
    def groupAlarms(self, pivotFrames):
        dateFrom, dateTo = hp.defaultTimeRange(1)
        metaDf = self.pq.readFile('parquet/raw/metaDf.parquet')

        nodes = metaDf[~(metaDf['site'].isnull()) & ~(
            metaDf['site'] == '') & ~(metaDf['lat'] == '') & ~(metaDf['lat'].isnull())]
        alarmCnt = []

        for site, lat, lon in nodes[['site', 'lat', 'lon']].drop_duplicates().values.tolist():
            for e, df in pivotFrames.items():
                # column "to" is closest to the time the alarms was generated, 
                # thus we want to which approx. when the alarms was created,
                # to be between dateFrom and dateTo

                sdf = df[(df['tag'] == site) & ((df['to'] >= dateFrom) & (df['to'] <= dateTo))]
                if len(sdf) > 0:
                    # sdf['id'].unique() returns the number of unique alarms for the given site
                    # those are the documents generated and stored in ES. They can be found in frames folder
                    # While pivotFrames expands the alarms to the level of individual sites
                    entry = {"event": e, "site": site, 'cnt':  len(sdf['id'].unique()),
                            "lat": lat, "lon": lon}
                else:
                    entry = {"event": e, "site": site, 'cnt': 0,
                            "lat": lat, "lon": lon}
                alarmCnt.append(entry)

        alarmsGrouped = pd.DataFrame(alarmCnt)

        self.pq.writeToFile(alarmsGrouped, f'{self.location}alarmsGrouped.parquet')


    @staticmethod
    def __isDataFresh(folder_path):
        total_size = 0
        twenty_five_hours_ago = datetime.now() - timedelta(hours=25)

        if os.path.exists(folder_path):
            for path, dirs, files in os.walk(folder_path):
                if not dirs and not files:
                    return False

                for file in files:
                    file_path = os.path.join(path, file)
                    file_date_created = datetime.fromtimestamp(os.path.getctime(file_path))
                    total_size += os.path.getsize(file_path)

                    if total_size <= 10 or file_date_created <= twenty_five_hours_ago:
                        return False

        return True


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
            df.loc[:, 'src'] = df['src'].str.upper()
            df.loc[:, 'dest'] = df['dest'].str.upper()
            df.loc[:, 'src_site'] = df['src_site'].str.upper()
            df.loc[:, 'dest_site'] = df['dest_site'].str.upper()
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
    def storeMetaData(self):
        metaDf = qrs.getMetaData()
        self.pq.writeToFile(metaDf, f"{self.location}raw/metaDf.parquet")


    @timer
    def storeAlarms(self):
        dateFrom, dateTo = hp.defaultTimeRange(60)
        print("Update data. Get all alarms for the past 60 days...", dateFrom, dateTo)
        oa = Alarms()
        frames, pivotFrames = oa.getAllAlarms(dateFrom, dateTo)
        self.groupAlarms(pivotFrames)

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
        dateFrom, dateTo = hp.defaultTimeRange(days=2)
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
    def storeThroughputDataAndModel(self):
        now = hp.defaultTimeRange(days=90, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        rawDf = createThrptDataset(start_date, end_date)

        self.pq.writeToFile(rawDf, f'{self.location}ml-datasets/throughput_Df.parquet')

        # train the ML model on the loaded dataset
        rawDf_onehot, model = trainMLmodel(rawDf)
        del rawDf

        self.pq.writeToFile(rawDf_onehot, f'{self.location}ml-datasets/throughput_onehot_Df.parquet')
        # save the classification model as a pickle file
        model_pkl_file = f'{self.location}ml-datasets/XGB_Classifier_model_throughput.pkl'
        with open(model_pkl_file, 'wb') as file:
            pickle.dump(model, file)


    @timer
    def storePacketLossDataAndModel(self):
        now = hp.defaultTimeRange(days=60, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        plsDf = createPcktDataset(start_date, end_date)
        self.pq.writeToFile(plsDf, f'{self.location}ml-datasets/packet_loss_Df.parquet')

        # onehot encode the whole dataset and leave only one month for further ML training
        plsDf_onehot_month, plsDf_onehot = one_month_data(plsDf)
        self.pq.writeToFile(plsDf_onehot, f'{self.location}ml-datasets/packet_loss_onehot_Df.parquet')
        del plsDf_onehot

        # train the model on one month data
        model = packet_loss_train_model(plsDf_onehot_month)
        del plsDf_onehot_month

        # save the classification model as a pickle file
        model_pkl_file = f'{self.location}ml-datasets/XGB_Classifier_model_packet_loss.pkl'
        with open(model_pkl_file, 'wb') as file:
            pickle.dump(model, file)



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
