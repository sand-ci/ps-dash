import pandas as pd
from datetime import datetime, timedelta

import model.queries as qrs
import utils.helpers as hp
from utils.parquet import Parquet


import logging
logging.basicConfig(format = '%(asctime)s %(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S %p',
                    filename = 'progress.log',
                    level=logging.INFO)

class AlarmsData(object):

    def __init__(self, dateFrom=None,  dateTo=None):
        if dateFrom is None or dateTo is None:
            now = hp.roundTime(datetime.utcnow())
            dateFrom = datetime.strftime(now - timedelta(days=1), '%Y-%m-%d %H:%M')
            dateTo = datetime.strftime(now, '%Y-%m-%d %H:%M')

        pq = Parquet()
        data = qrs.alarms(hp.GetTimeRanges(dateFrom, dateTo))
        for k,v in data.items():
            filename = k.replace(' ','_')
            filename = filename.replace('/','-')
            if len(v)>0:
                pq.writeToFile(pd.DataFrame(v), f"parquet/alarm_{filename}")
                
        logging.info(f'Alarms for period {dateFrom} - {dateTo} \n found the following types \n {data.keys()}')