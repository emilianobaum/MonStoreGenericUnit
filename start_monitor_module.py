#!/usr/bin/python3
# coding: utf-8

from monitor_unit import StartService as SS
from es_index__module import ESIndices

from datetime import datetime
from time import sleep
from load_config import ConfigData as CD
import logging

logger = logging.getLogger(__name__)

class MonitorModule(object):
    
    def __init__(self):
        data = CD("configurations/configuration.json")
        print("Time Based? ",data.elasticIndexTime)
        if data.elasticIndexTime == 'True':
            timeFormating = datetime.strftime(datetime.utcnow(), data.elasticIndexTimeFormat)
            data.elasticIndex = "%s-%s"%(data.elasticIndex, timeFormating)
        print("Time Formating ",timeFormating)
        print("Time Format: ",data.elasticIndexTimeFormat)
        print("Index Type: ",data.elasticIndexType)
        ESI = ESIndices()
        logger.debug("Try connection to the cluster")
        print("ES Connection: ",ESI.conn(data))
        while True:
            telemetry = SS(data).telemetry
            print("Stream: \n ",telemetry)
            print("Index Name: ",data.elasticIndex)
            print("ES Index Exist: ",ESI.is_index_exist(data.elasticIndex))
            logger.debug("Connection successfully")
            logger.debug("Formatting data ...")
            indexPath ="%s%s"%(data.indexConf, data.elasticFile)

            ESI.indexing_data(data.elasticIndex, data.elasticIndexType, 
                              telemetry, data.elasticDefinition, indexPath)
            sleep(5)
            print("=============================================================")
            
if __name__ == '__main__':
    MonitorModule()