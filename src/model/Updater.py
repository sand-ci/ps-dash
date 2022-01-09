import os
import os.path
import time
import threading
import traceback

from utils.helpers import timer
from model.AlarmsData import AlarmsData
from model.IndicesRawData import IndicesRawData
from model.MetaData import MetaData


class ParquetUpdater(object):

    def __init__(self):
        self.__createLocation('parquet/')
        self.__createLocation('parquet/raw/')
        self.__updateAlarmsData()
        self.__updateMetaData()
        self.__updateRawIndecesData()
        try:
            Scheduler(300, self.__updateAlarmsData)
            Scheduler(350, self.__updateMetaData)
            Scheduler(1250, self.__updateRawIndecesData)
        except Exception as e:
            print(traceback.format_exc())


    @timer
    def __updateAlarmsData(self):
        print()
        AlarmsData()

    @timer
    def __updateMetaData(self):
        print()
        MetaData()

    @timer
    def __updateRawIndecesData(self):
        print()
        IndicesRawData()


    def __createLocation(self,location):
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
        self.__start()

    def __run(self):
        self.is_running = False
        self.__start()
        self.function(*self.args, **self.kwargs)

    def __start(self):
        if not self.is_running:
            self.next_call += self.interval
            self._timer = threading.Timer(self.next_call - time.time(), self.__run)
            self._timer.__start()
            self.is_running = True

    def __stop(self):
        self._timer.cancel()
        self.is_running = False