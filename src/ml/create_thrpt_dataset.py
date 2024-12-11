import pandas as pd

import model.queries as qrs
import utils.helpers as hp




def queryData(dateFrom, dateTo):
    data = []
    # query in portions since ES does not allow aggregations with more than 10000 bins
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo) / 60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    print(dateFrom, dateTo)
    for i in range(len(time_list) - 1):
        print(f' {i+1}/{len(time_list)-1} throughput query', time_list[i], time_list[i + 1])
        data.extend(qrs.queryThroughputIdx(time_list[i], time_list[i + 1]))

    return data


def createThrptDataset(dateFrom, dateTo):
    # dateFrom, dateTo = ['2023-10-01 03:00', '2023-11-01 03:00']
    # get the data
    rawDf = pd.DataFrame(queryData(dateFrom, dateTo))
    print(rawDf.head())
    rawDf['dt'] = pd.to_datetime(rawDf['from'], utc=True)
    rawDf['src_site'] = rawDf['src_site'].str.upper()
    rawDf['dest_site'] = rawDf['dest_site'].str.upper()

    booleanDictionary = {True: 'ipv6', False: 'ipv4'}
    rawDf['ipv'] = rawDf['ipv6'].map(booleanDictionary)

    # convert to MB
    rawDf['value'] = round(rawDf['value'] * 1e-6)

    return rawDf