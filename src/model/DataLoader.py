import threading
import time
import datetime
import pandas as pd
import time
from functools import reduce, wraps
from datetime import datetime, timedelta
import numpy as np
from  scipy.stats import zscore

import model.queries as qrs
from model.NodesMetaData import NodesMetaData
import utils.helpers as hp
from utils.helpers import timer


class Singleton(type):

    def __init__(cls, name, bases, attibutes):
        cls._dict = {}
        cls._registered = []

    def __call__(cls, dateFrom=None, dateTo=None, *args):
        if (dateFrom is None) or (dateTo is None):
            defaultDT = hp.defaultTimeRange()
            dateFrom = defaultDT[0]
            dateTo = defaultDT[1]

        if (dateFrom, dateTo) in cls._dict:
            print('OBJECT EXISTS', cls, dateFrom, dateTo)
            instance = cls._dict[(dateFrom, dateTo)]
        else:
            print('OBJECT DOES NOT EXIST', cls, dateFrom, dateTo)
            if ([dateFrom, dateTo] != cls._registered) :
                print(' >>> CREATING NEW INSTANCE')
                cls._registered = [dateFrom, dateTo]
                instance = super().__call__(dateFrom, dateTo, *args)
                cls._dict[(dateFrom, dateTo)] = instance
            elif [dateFrom, dateTo] == cls._registered:
                print(' >>> CREATION IS IN PROGRESS FOR', dateFrom, dateTo)
                print(' >>> GET LAST INSTANCE INSTEAD', list(cls._dict.keys())[-1])
                instance = cls._dict[list(cls._dict.keys())[-1]]

                # keep only a few objects in memory
                if len(cls._dict) >= 3:
                    cls._dict.pop(list(cls._dict.keys())[0])

        return instance


class Updater(object):

    def __init__(self):
        self.StartThread()

    @timer
    def UpdateAllData(self):
        print()
        print(f'{datetime.utcnow()}: New data is on its way...')
        print('Active threads:',threading.active_count())
        # query period must be the same for all data loaders
        defaultDT = hp.defaultTimeRange()
        GeneralDataLoader(defaultDT[0], defaultDT[1])
        SiteDataLoader(defaultDT[0], defaultDT[1])
        PrtoblematicPairsDataLoader(defaultDT[0], defaultDT[1])
        self.lastUpdated = hp.roundTime(datetime.utcnow())
        self.StartThread()

    def StartThread(self):
        self.thread = threading.Timer(3600, self.UpdateAllData) # 1hour
        self.thread.daemon = True
        self.thread.start()


class GeneralDataLoader(object, metaclass=Singleton):

    def __init__(self, dateFrom,  dateTo):
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.lastUpdated = None
        self.pls = pd.DataFrame()
        self.owd = pd.DataFrame()
        self.thp = pd.DataFrame()
        self.rtm = pd.DataFrame()
        self.UpdateGeneralInfo()

    @property
    def dateFrom(self):
        return self._dateFrom

    @dateFrom.setter
    def dateFrom(self, value):
        self._dateFrom = int(time.mktime(datetime.strptime(value, "%Y-%m-%d %H:%M").timetuple())*1000)

    @property
    def dateTo(self):
        return self._dateTo

    @dateTo.setter
    def dateTo(self, value):
        self._dateTo = int(time.mktime(datetime.strptime(value, "%Y-%m-%d %H:%M").timetuple())*1000)

    @property
    def lastUpdated(self):
        return self._lastUpdated

    @lastUpdated.setter
    def lastUpdated(self, value):
        self._lastUpdated = value

    @timer
    def UpdateGeneralInfo(self):
        print("last updated: {0}, new start: {1} new end: {2} ".format(self.lastUpdated, self.dateFrom, self.dateTo))

        self.pls = NodesMetaData('ps_packetloss', self.dateFrom, self.dateTo).df
        self.owd = NodesMetaData('ps_owd', self.dateFrom, self.dateTo).df
        self.thp = NodesMetaData('ps_throughput', self.dateFrom, self.dateTo).df
        self.rtm = NodesMetaData('ps_retransmits', self.dateFrom, self.dateTo).df
        self.latency_df = pd.merge(self.pls, self.owd, how='outer')
        self.throughput_df = pd.merge(self.thp, self.rtm, how='outer')
        all_df = pd.merge(self.latency_df, self.throughput_df, how='outer')
        self.all_df = all_df.drop_duplicates()

        self.pls_related_only = self.pls[self.pls['host_in_ps_meta'] == True]
        self.owd_related_only = self.owd[self.owd['host_in_ps_meta'] == True]
        self.thp_related_only = self.thp[self.thp['host_in_ps_meta'] == True]
        self.rtm_related_only = self.rtm[self.rtm['host_in_ps_meta'] == True]

        self.latency_df_related_only = self.latency_df[self.latency_df['host_in_ps_meta'] == True]
        self.throughput_df_related_only = self.throughput_df[self.throughput_df['host_in_ps_meta'] == True]
        self.all_df_related_only = self.all_df[self.all_df['host_in_ps_meta'] == True]

        self.lastUpdated = datetime.now()


class SiteDataLoader(object, metaclass=Singleton):

    genData = GeneralDataLoader()

    def __init__(self, dateFrom, dateTo):
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.UpdateSiteData()

    def UpdateSiteData(self):
        print('UpdateSiteData >>> ', self.dateFrom, self.dateTo)
        pls_site_in_out = self.InOutDf("ps_packetloss", self.genData.pls_related_only)
        self.pls_data = pls_site_in_out['data']
        self.pls_dates = pls_site_in_out['dates']
        owd_site_in_out = self.InOutDf("ps_owd", self.genData.owd_related_only)
        self.owd_data = owd_site_in_out['data']
        self.owd_dates = owd_site_in_out['dates']
        thp_site_in_out = self.InOutDf("ps_throughput", self.genData.thp_related_only)
        self.thp_data = thp_site_in_out['data']
        self.thp_dates = thp_site_in_out['dates']
        rtm_site_in_out = self.InOutDf("ps_retransmits", self.genData.rtm_related_only)
        self.rtm_data = rtm_site_in_out['data']
        self.rtm_dates = rtm_site_in_out['dates']

        self.latency_df_related_only = self.genData.latency_df_related_only
        self.throughput_df_related_only = self.genData.throughput_df_related_only

        self.sites = self.orderSites()


    @timer
    def InOutDf(self, idx, idx_df):
        print(idx)
        in_out_values = []
        time_list = hp.GetTimeRanges(self.dateFrom, self.dateTo)
        for t in ['dest_host', 'src_host']:
            meta_df = idx_df.copy()

            df = pd.DataFrame(qrs.queryDailyAvg(idx, t, time_list[0], time_list[1])).reset_index()

            df['index'] = pd.to_datetime(df['index'], unit='ms').dt.strftime('%d/%m')
            df = df.transpose()
            header = df.iloc[0]
            df = df[1:]
            df.columns = ['day-3', 'day-2', 'day-1', 'day']

            meta_df = pd.merge(meta_df, df, left_on="host", right_index=True)

            three_days_ago = meta_df.groupby('site').agg({'day-3': lambda x: x.mean(skipna=False)}, axis=1).reset_index()
            two_days_ago = meta_df.groupby('site').agg({'day-2': lambda x: x.mean(skipna=False)}, axis=1).reset_index()
            one_day_ago = meta_df.groupby('site').agg({'day-1': lambda x: x.mean(skipna=False)}, axis=1).reset_index()
            today = meta_df.groupby('site').agg({'day': lambda x: x.mean(skipna=False)}, axis=1).reset_index()

            site_avg_df = reduce(lambda x,y: pd.merge(x,y, on='site', how='outer'), [three_days_ago, two_days_ago, one_day_ago, today])
            site_avg_df.set_index('site', inplace=True)
            change = site_avg_df.pct_change(axis='columns')
            site_avg_df = pd.merge(site_avg_df, change, left_index=True, right_index=True, suffixes=('_val', ''))
            site_avg_df['direction'] = 'IN' if t == 'dest_host' else 'OUT'

            in_out_values.append(site_avg_df)

        site_df = pd.concat(in_out_values).reset_index()
        site_df = site_df.round(2)

        return {"data": site_df,
                "dates": header}

    def orderSites(self):
        problematic = []
        problematic.extend(self.thp_data.nsmallest(20, ['day-3_val', 'day-2_val', 'day-1_val', 'day_val'])['site'].values)
        problematic.extend(self.rtm_data.nlargest(20, ['day-3_val', 'day-2_val', 'day-1_val', 'day_val'])['site'].values)
        problematic.extend(self.pls_data.nlargest(20, ['day-3_val', 'day-2_val', 'day-1_val', 'day_val'])['site'].values)
        problematic.extend(self.owd_data.nlargest(20, ['day-3_val', 'day-2_val', 'day-1_val', 'day_val'])['site'].values)
        problematic = list(set(problematic))
        all_df = self.genData.all_df_related_only.copy()
        all_df['has_problems'] = all_df['site'].apply(lambda x: True if x in problematic else False)
        sites = all_df.sort_values(by='has_problems', ascending=False).drop_duplicates(['site'])['site'].values
        return sites


class PrtoblematicPairsDataLoader(object, metaclass=Singleton):

    gobj = GeneralDataLoader()
    LIST_IDXS = ['ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput']

    def __init__(self, dateFrom, dateTo):
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.all_df = self.gobj.all_df_related_only[['ip', 'is_ipv6', 'host', 'site', 'admin_email', 'admin_name', 'ip_in_ps_meta',
                 'host_in_ps_meta', 'host_index', 'site_index', 'host_meta', 'site_meta']].sort_values(by=['ip_in_ps_meta', 'host_in_ps_meta', 'ip'], ascending=False)
        self.df = self.markNodes()


    @timer
    def buildProblems(self, idx):
        print('buildProblems...',idx)
        data = []
        intv = int(hp.CalcMinutes4Period(self.dateFrom, self.dateTo)/60)
        time_list = hp.GetTimeRanges(self.dateFrom, self.dateTo, intv)
        for i in range(len(time_list)-1):
            data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

        return data


    @timer
    def getPercentageMeasuresDone(self, grouped, tempdf):
        measures_done = tempdf.groupby('hash').agg({'doc_count':'sum'})
        def findRatio(row, total_minutes):
            if pd.isna(row['doc_count']):
                count = '0'
            else: count = str(round((row['doc_count']/total_minutes)*100))+'%'
            return count

        one_test_per_min = hp.CalcMinutes4Period(self.dateFrom, self.dateTo)
        measures_done['tests_done'] = measures_done.apply(lambda x: findRatio(x, one_test_per_min), axis=1)
        grouped = pd.merge(grouped, measures_done, on='hash', how='left')

        return grouped


    @timer
    def markNodes(self):
        df = pd.DataFrame()
        for idx in self.LIST_IDXS:
            tempdf = pd.DataFrame(self.buildProblems(idx))
            grouped = tempdf.groupby(['src', 'dest', 'hash']).agg({'value': lambda x: x.mean(skipna=False)}, axis=1).reset_index()

            grouped = self.getRelHosts(grouped)
            # zscore based on a each pair value
            tempdf['zscore'] = tempdf.groupby('hash')['value'].apply(lambda x: (x - x.mean())/x.std())
            # add max zscore so that it is possible to order by worst
            max_z = tempdf.groupby('hash').agg({'zscore':'max'}).rename(columns={'zscore':'max_hash_zscore'})
            grouped = pd.merge(grouped, max_z, on='hash', how='left')

            # zscore based on the whole dataset
            grouped['zscore'] = grouped[['value']].apply(lambda x: (x - x.mean())/x.std())
            grouped['idx'] = idx

            # calculate the percentage of measures based on the assumption that ideally measures are done once every minute
            grouped = self.getPercentageMeasuresDone(grouped, tempdf)

            # this is not accurate since we have some cases with 4-5 times more tests than expected
            avg_numtests = tempdf.groupby('hash').agg({'doc_count':'mean'}).values[0][0]

            # Add flags for some general problems
            if (idx == 'ps_packetloss'):
                grouped['all_packets_lost'] = grouped['hash'].apply(lambda x: 1 if x in grouped[grouped['value']==1]['hash'].values else 0)
            else: grouped['all_packets_lost'] = -1

            grouped['high_sigma'] = grouped['hash'].apply(lambda x: 1
                                                          if x in grouped[grouped['zscore'] > 3].drop_duplicates()['hash'].values
                                                          else 0)
            grouped['has_bursts'] = grouped['hash'].apply(lambda x: 1
                                                           if x in tempdf[tempdf['zscore']>5]['hash'].values
                                                           else 0)
            grouped['src_not_in'] = grouped['hash'].apply(lambda x: 1
                                                          if x in grouped[grouped['src'].isin(self.all_df['ip']) == False]['hash'].values
                                                          else 0)
            grouped['dest_not_in'] = grouped['hash'].apply(lambda x: 1
                                                           if x in grouped[grouped['dest'].isin(self.all_df['ip']) == False]['hash'].values
                                                           else 0)

            grouped['measures'] = grouped['doc_count'].astype(str)+'('+grouped['tests_done'].astype(str)+')'

            df = df.append(grouped, ignore_index=True)
        print(f'Total number of hashes: {len(df)}')

        return df

    @timer
    def getValues(self, probdf):
    #     probdf = markNodes()
        df = pd.DataFrame(columns=['timestamp', 'value', 'idx', 'hash'])
        time_list = hp.GetTimeRanges(self.dateFrom, self.dateTo)
        for item in probdf[['src', 'dest', 'idx']].values:
            print(item)
            tempdf = pd.DataFrame(qrs.queryAllValues(item[2], item, time_list[0], time_list[1]))
            tempdf['idx'] = item[2]
            tempdf['hash'] = item[0]+"-"+item[1]
            tempdf['src'] = item[0]
            tempdf['dest'] = item[1]
            tempdf.rename(columns={hp.getValueField(item[2]): 'value'}, inplace=True)
            df = df.append(tempdf, ignore_index=True)

        return df

    @timer
    def getRelHosts(self, probdf):
        df1 = pd.merge(self.all_df[['host', 'ip', 'site']], probdf[['src', 'hash']], left_on='ip', right_on='src', how='right')
        df2 = pd.merge(self.all_df[['host', 'ip', 'site']], probdf[['dest', 'hash']], left_on='ip', right_on='dest', how='right')
        df = pd.merge(df1, df2, on=['hash'], suffixes=('_src', '_dest'), how='inner')
    #     df = df[df.duplicated(subset=['hash'])==False]

        df = df.drop(columns=['ip_src', 'ip_dest'])
        df = pd.merge(probdf, df, on=['hash', 'src', 'dest'], how='left')

        return df