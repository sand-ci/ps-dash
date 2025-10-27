import gc
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
import logging
import requests
import json
from utils.hosts_audit import audit
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

CRIC_OPN_RCSITES = {"CERN-PROD":'CERN-PROD-LHCOPNE', "BNL-ATLAS":'BNL-ATLAS-LHCOPNE', "INFN-T1":'INFN-T1-LHCOPNE',
                    "USCMS-FNAL-WC1":'USCMS-FNAL-WC1-LHCOPNE', "FZK-LCG2":'FZK-LCG2-LHCOPNE', "IN2P3-CC":'IN2P3-CC-LHCOPNE', "RAL-LCG2":'RAL-LCG2-LHCOPN', 
                    "JINR-T1":'JINR-T1-LHCOPNE', "pic":'PIC-LHCOPNE', "SARA-MATRIX":'NLT1-SARA-LHCOPNE',
                    "TRIUMF-LCG2":'TRIUMF-LCG2-LHCOPNE', "NDGF-T1":'NDGF-T1-LHCOPNE', "KR-KISTI-GSDC-01":'KR-KISTI-GSDC-1-LHCOPNE',
                    "NCBJ-CIS":'NCBJ-LHCOPN'}
@timer
class ParquetUpdater(object):
    
    def __init__(self, location='parquet/'):
        self.pq = Parquet()
        self.alarms = Alarms()
        self.location = location
        required_folders = ['raw', 'frames', 'pivot', 'ml-datasets']
        self.createLocation(required_folders)

        try:
            if not self.__isDataFresh(required_folders):
                print("Updating...")
                self.storeMetaData()
                self.cacheIndexData()
                self.storeAlarms()
                self.storeASNPathChanged()

                # self.storeThroughputDataAndModel()
                # self.storePacketLossDataAndModel()
                
                self.psConfigDataAndAudit()
                self.storeCRICData()
                self.validOPNTraceroutes()

            # Set the schedulers
            Scheduler(60*60*12, self.storeMetaData)
            Scheduler(60*60, self.cacheIndexData)

            Scheduler(60*30, self.storeAlarms)
            Scheduler(60*60*12, self.storeASNPathChanged)
            Scheduler(60*60*24, self.storeCRICData)
            Scheduler(60*60*24, self.psConfigDataAndAudit)
            Scheduler(60*60*2, self.validOPNTraceroutes)


            # Scheduler(60*60*12, self.storeThroughputDataAndModel)
            # Scheduler(60*60*12, self.storePacketLossDataAndModel)
        except Exception as e:
            print(traceback.format_exc())


    # The following function is used to group alarms by site 
    # taking into account the most recent 48 hours only
    def groupAlarms(self, pivotFrames):
        dateFrom, dateTo = hp.defaultTimeRange(days=2)
        metaDf = self.pq.readFile('parquet/raw/metaDf.parquet')
        # frames, pivotFrames = self.alarms.loadData(dateFrom, dateTo)

        nodes = metaDf[~(metaDf['site'].isnull()) & (metaDf['site'] != '')\
               & (metaDf['lat'] != '') & (metaDf['lat'].isnull()==False)].drop_duplicates()
        lat_lon_count = nodes.groupby(['site', 'lat', 'lon']).size().reset_index(name='count')
        # Find the most common lat-lon for each site
        most_common_lat_lon = lat_lon_count.loc[lat_lon_count.groupby('site')['count'].idxmax()]

        alarmCnt = []
        
        for site, lat, lon in most_common_lat_lon[['site', 'lat', 'lon']].values.tolist():
            for e, df in pivotFrames.items():
                # column "to" is closest to the time the alarms was generated, 
                # thus we want to which approx. when the alarms was created,
                # to be between dateFrom and dateTo
                df['to'] = pd.to_datetime(df['to'], utc=True)
                sdf = df[(df['tag'].str.upper() == site.upper()) & (df['to'] >= dateFrom) & (df['to'] <= dateTo)]
                # print('e: ', e)
                # if e == 'high delay from/to multiple sites':
                #     print(sdf)
                if len(sdf) > 0:
                    # sdf['id'].unique() returns the number of unique alarms for the given site
                    # those are the documents generated and stored in ES. They can be found in frames folder
                    # While pivotFrames expands the alarms to the level of individual sites
                    if e == 'ASN path anomalies per site':
                        cnt =sum(sdf['total_paths_anomalies'].tolist())
                    
                    else:
                        cnt = len(sdf['id'].unique())
                    entry = {"event": e, "site": site, 'cnt':  cnt,
                            "lat": lat, "lon": lon}
                else:
                    entry = {"event": e, "site": site, 'cnt': 0,
                            "lat": lat, "lon": lon}
                alarmCnt.append(entry)

        print('Number of sites:', len(alarmCnt))
        alarmsGrouped = pd.DataFrame(alarmCnt)
        print('Number of site-alarms:', len(alarmsGrouped[alarmsGrouped['cnt']>0]))

        self.pq.writeToFile(alarmsGrouped, f'{self.location}alarmsGrouped.parquet')


    def __isDataFresh(self, required_folders, freshness_threshold=1*60*60): # 1 hour
        """
        Check if the data in the specified location is fresh.
        :param location: The directory to check.
        :param freshness_threshold: The freshness threshold in seconds (default is 1 hour).
        :return: True if the data is fresh, False otherwise.
        """
        current_time = time.time()
        
        for folder in required_folders:
            folder_path = os.path.join(self.location, folder)
            print(f"Checking folder: {folder_path}")
            if not os.listdir(folder_path):
                print(f"Folder {folder_path} is empty.")
                return False

        for root, dirs, files in os.walk(self.location):
            for file in files:
                file_path = os.path.join(root, file)
                file_mod_time = os.path.getmtime(file_path)
                file_age = current_time - file_mod_time
                print(f"File: {file_path}, Age: {file_age} seconds")
                if file_age > freshness_threshold:
                    print(f"File {file_path} is older than the freshness threshold.")
                    return False

        print("All files are fresh.")
        return True


    @timer
    def queryData(self, idx, dateFrom, dateTo):
        intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/30)
        if idx in ['ps_throughput']:
            dateFrom, dateTo = hp.defaultTimeRange(days=21)
            intv = 42  # 12 hour bins

        data = []
        
        time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
        for i in range(len(time_list)-1):
            data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

        return data


    @timer
    def cacheIndexData(self):
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
        self.pq.writeToFile(measures, f'{self.location}raw/measures.parquet')

    @timer
    def storeMetaData(self):
        metaDf = qrs.getMetaData()
        self.pq.writeToFile(metaDf, f"{self.location}raw/metaDf.parquet")
        
    @timer
    def storeCRICData(self):
        all_hosts = []
        r = requests.get(
            'https://wlcg-cric.cern.ch/api/core/service/query/?json&state=ACTIVE&type=PerfSonar',
            verify=False
        )
        res = r.json()
        for _key, val in res.items():
            if not val['endpoint']:
                print('no hostname? should not happen:', val)
                continue
            p = val['endpoint']
            all_hosts.append(p)
        df = pd.DataFrame(all_hosts, columns=['host'])
        self.pq.writeToFile(df, f"{self.location}raw/CRICDataHosts.parquet")
        
        subnets = []
        r = requests.get(
        'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json', verify=False).json()
        for site in CRIC_OPN_RCSITES.keys():
            try:
                for netroute in r[site]['netroutes'].values():
                    if netroute['lhcone_bandwidth_limit'] > 0:   
                        content = netroute['networks']
                        subnets.append([CRIC_OPN_RCSITES[site], content.get('ipv4', []) + content.get('ipv6', [])])
                        
            except Exception as e:
                print(e)
                print(f'While writing CRICDataOPNSubnets.parquet subnet for {site} was not found.\n')
        df2 = pd.DataFrame(subnets, columns=['site', 'subnets'])
        df2 = df2.groupby('site').agg('sum').reset_index()
        self.pq.writeToFile(df2, f"{self.location}raw/CRICDataOPNSubnets.parquet")
    
    @timer
    def validOPNTraceroutes(self):
        to_date = datetime.utcnow()
        from_date = to_date - timedelta(hours=2)
        records = qrs.queryOPNTraceroutes(from_date.strftime(hp.DATE_FORMAT), to_date.strftime(hp.DATE_FORMAT), list(CRIC_OPN_RCSITES.values())+["pic-LHCOPNE"]) 
        df = pd.DataFrame(records)
        self.pq.writeToFile(df, f"{self.location}raw/traceroutes_OPN.parquet")
     

    @timer
    def psConfigDataAndAudit(self):
        log = logging.getLogger(__name__)
        last_modified = datetime.fromtimestamp(Path("parquet/audited_hosts.parquet").stat().st_mtime, tz=timezone.utc)
        print(f"Host audit last modified: {last_modified}")
        if last_modified - datetime.now(timezone.utc) > timedelta(hours=24) or not os.path.exists("parquet/audited_hosts.parquet") or not os.path.exists("parquet/raw/psConfigData.parquet"):
            print("Updating audit...")
            def request(url, hostcert=None, hostkey=None, verify=False):
                log.debug('Retrieving {}'.format(url))
                if hostcert and hostkey:
                    req = requests.get(url, verify=verify, timeout=120, cert=(hostcert, hostkey))
                else:
                    req = requests.get(url, timeout=120, verify=verify)
                req.raise_for_status()
                return req.content
            
            def extract_host_info(config):
                hosts = config.get("hosts", {})
                groups = config.get("groups", {})
                tasks = config.get("tasks", {})
                tests = config.get("tests", {})
                schedules = config.get("schedules", {})
                group_type_map = {}
                test_schedule_map = {}
                for task_name, task_info in tasks.items():
                    group_name = task_info.get("group")
                    if group_name:
                        test_type = tests.get(group_name).get("type")
                        group_type_map[group_name] = test_type
                    schedule = task_info.get("schedule")
                    test_schedule_map[group_name] = schedules.get(schedule, None)
                # For each host, find groups it belongs to
                # groups have list of addresses with names
                # Check which groups contain this host
                host_rows = []
                for host in hosts.keys():
                    participating_groups = []
                    participating_types = []
                    participating_schedules = []
                    participating_tests = []

                    for group_name, group_info in groups.items():
                        addr_list = group_info.get("addresses", [])
                        # check if host is in this group's addresses
                        if any(addr.get("name") == host for addr in addr_list):
                            participating_groups.append(group_name)
                            # get type of test associated with group
                            test_type = group_type_map.get(group_name, None)
                            if test_type:
                                participating_types.append(test_type)
                            # get schedule(s) from tests that belong to this group
                            # find tests whose group matches group_name
                            for task_name, task_info in tasks.items():
                                if task_info.get("group") == group_name:
                                    schedule = test_schedule_map.get(task_name, {})
                                    participating_schedules.append(schedule)
                                    participating_tests.append(group_name)
                    extract_cric_site = False
                    row = {
                        "Host": host,
                        # "Site": queryNetsiteForHost(host),
                        "Groups": participating_groups,
                        "Types": participating_types,
                        "Schedules": participating_schedules,
                        "Test Count": len(set(participating_tests))
                    }
                    host_rows.append(row)
                
                #     print(row)
                # print("\n\n\n")
                return host_rows
            
            
            url = "https://psconfig.aglt2.org/pub/config"
            req = request(url)
            config_st = json.loads(req)
            configs_df = pd.DataFrame() 
            for e in config_st:
                mesh_url = e['include'][0]
                mesh_r = request(mesh_url)

                
                mesh_config = json.loads(mesh_r)
                host_info_list = extract_host_info(mesh_config)  # list of dicts
                current_df = pd.DataFrame(host_info_list)
                configs_df = pd.concat([configs_df, current_df], ignore_index=True)
                
            all_hosts = configs_df['Host'].unique()
            audited = asyncio.run(audit(all_hosts))  # <-- your coroutine
            audited_df = pd.DataFrame(audited)
                
            self.pq.writeToFile(configs_df, f"{self.location}raw/psConfigData.parquet")
            self.pq.writeToFile(audited_df, f"{self.location}audited_hosts.parquet")
            
        print("Audit is up-to-date.")
            
    @timer
    def storeAlarms(self):
        dateFrom, dateTo = hp.defaultTimeRange(30)
        print("Update data. Get all alarms for the past 30 days...", dateFrom, dateTo)
        frames, pivotFrames = self.alarms.getAllAlarms(dateFrom, dateTo)
        self.groupAlarms(pivotFrames)

        for event,df in pivotFrames.items():
            if event == 'ASN path anomalies per site':
                print(df.info())
            filename = self.alarms.eventCF(event)
            fdf = frames[event]
            if len(fdf)>0:
                self.pq.writeToFile(df, f"parquet/pivot/{filename}.parquet")
                self.pq.writeToFile(fdf, f"parquet/frames/{filename}.parquet")

    @timer
    def storeASNPathChanged(self):
        dateFrom, dateTo = hp.defaultTimeRange(days=3)
        df = qrs.queryPathAnomaliesDetails(dateFrom, dateTo)
        self.pq.writeToFile(df, f"parquet/asn_path_changes.parquet")

    def createLocation(self, required_folders):

        if os.path.isdir(self.location):
            print(self.location,"exists.")
        else:
            print(self.location, "doesn't exists. Creating...")
            os.mkdir(self.location)

        for folder in required_folders:
            folder_path = os.path.join(self.location, folder)
            if not os.path.isdir(folder_path):
                print(folder_path, "doesn't exists. Creating...")
                os.mkdir(folder_path)


    @timer
    def storeThroughputDataAndModel(self):
        print('Starting storeThroughputDataAndModel')
        now = hp.defaultTimeRange(days=90, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        rawDf = createThrptDataset(start_date, end_date)

        self.pq.writeToFile(rawDf, f'{self.location}ml-datasets/throughput_Df.parquet')

        # train the ML model on the loaded dataset
        rawDf_onehot, model = trainMLmodel(rawDf)
        del rawDf
        print('Trained ML model')

        self.pq.writeToFile(rawDf_onehot, f'{self.location}ml-datasets/throughput_onehot_Df.parquet')
        # save the classification model as a pickle file
        model_pkl_file = f'{self.location}ml-datasets/XGB_Classifier_model_throughput.pkl'
        with open(model_pkl_file, 'wb') as file:
            pickle.dump(model, file)
        print('Saved XGB_Classifier_model_throughput.pkl')
        del rawDf_onehot, model
        gc.collect()


    @timer
    def storePacketLossDataAndModel(self):
        start_time = time.time()
        print("Starting storePacketLossDataAndModel")
        now = hp.defaultTimeRange(days=60, datesOnly=True)
        start_date = now[0]
        end_date = now[1]
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']

        plsDf = createPcktDataset(start_date, end_date)
        self.pq.writeToFile(plsDf, f'{self.location}ml-datasets/packet_loss_Df.parquet')

        print("One-hot encoding the dataset")
        plsDf_onehot_month, plsDf_onehot = one_month_data(plsDf)
        self.pq.writeToFile(plsDf_onehot, f'{self.location}ml-datasets/packet_loss_onehot_Df.parquet')
        del plsDf_onehot

        print("Training the model on one month data")
        model = packet_loss_train_model(plsDf_onehot_month)
        del plsDf_onehot_month

        print("Saving the classification model as a pickle file")
        model_pkl_file = f'{self.location}ml-datasets/XGB_Classifier_model_packet_loss.pkl'
        with open(model_pkl_file, 'wb') as file:
            pickle.dump(model, file)
        del plsDf_onehot, model
        gc.collect()
        end_time = time.time()
        print(f"Finished storePacketLossDataAndModel in {end_time - start_time} seconds")


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
        if self._timer:
            self._timer.cancel()
        self.is_running = False
