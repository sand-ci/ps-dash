import pandas as pd

import model.queries as qrs
import utils.helpers as hp
from ml.helpers import timer

@timer
def loadPacketLossData(dateFrom, dateTo):
    data = []
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo) / 60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list) - 1):
        data.extend(qrs.query4Avg('ps_packetloss', time_list[i], time_list[i + 1]))

    return pd.DataFrame(data)


def getPercentageMeasuresDone(grouped, tempdf):
    measures_done = tempdf.groupby('pair').agg({'doc_count': 'sum'})

    def findRatio(row, total_minutes):
        if pd.isna(row['doc_count']):
            count = '0'
        else:
            count = str(round((row['doc_count'] / total_minutes) * 100)) + '%'
        return count

    one_test_per_min = hp.CalcMinutes4Period(dateFrom, dateTo)
    measures_done['tests_done'] = measures_done.apply(
        lambda x: findRatio(x, one_test_per_min), axis=1)
    grouped = pd.merge(grouped, measures_done, on='pair', how='left')

    return grouped


@timer
def markPairs(dateFrom, dateTo):
    df = pd.DataFrame()

    tempdf = loadPacketLossData(dateFrom, dateTo)
    grouped = fixMissingMetadata(tempdf)

    # calculate the percentage of measures based on the assumption that ideally measures are done once every minute
    grouped = getPercentageMeasuresDone(grouped, tempdf)

    # set value to 0 - we consider there is no issue bellow 2% loss
    # set value to 1 - the pair is marked problematic between 2% and 100% loss
    # set value to 2 - the pair shows 100% loss
    def setFlag(x):
        if x >= 0 and x < 0.02:
            return 0
        elif x >= 0.02 and x < 1:
            return 1
        elif x == 1:
            return 2
        return 'something is wrong'

    grouped['flag'] = grouped['value'].apply(lambda val: setFlag(val))

    df = pd.concat([df, grouped], ignore_index=True)
    df.rename(columns={'value': 'avg_value'}, inplace=True)
    df = df.round({'avg_value': 3})

    return df


@timer
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
        columns={'host': 'host_src', 'site': 'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
        columns={'host': 'host_dest', 'site': 'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['src_site'].fillna(rawDf.pop('site_src'))
    rawDf['dest_site'] = rawDf['dest_site'].fillna(rawDf.pop('site_dest'))
    rawDf['src_host'] = rawDf['src_host'].fillna(rawDf.pop('host_src'))
    rawDf['dest_host'] = rawDf['dest_host'].fillna(rawDf.pop('host_dest'))

    return rawDf

"""
'src', 'src_host', 'src_site':      info about the source
'dest', 'dest_host', 'dest_site':   info about the destination
'avg_value':                        the average value for the pair
'tests_done':                       % of tests done for the whole period. The calculation is based on the assumption
                                    that there should be 1 measure per minute
"""

def createPcktDataset(dateTo, dateFrom):

    # dateFrom, dateTo = ['2023-01-01 03:00', '2023-05-31 03:00']

    plsDf = markPairs(dateFrom, dateTo)
    plsDf = plsDf[plsDf['tests_done'] != '0%']

    plsDf['src_site'] = plsDf['src_site'].str.upper()
    plsDf['dest_site'] = plsDf['dest_site'].str.upper()

    return plsDf