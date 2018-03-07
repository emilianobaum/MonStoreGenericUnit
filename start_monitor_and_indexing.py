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
        DATA = CD("configurations/configuration.json")
        if DATA.elasticIndexTime == 'True':
            timeFormating = datetime.strftime(datetime.utcnow(), 
                                              DATA.elasticIndexTimeFormat)
            DATA.elasticIndex = "%s-%s"%(DATA.elasticIndex, timeFormating)

        ESI = ESIndices()
        logger.debug("Try connection to the cluster")
        ESI.conn(DATA)
        logger.debug("Start data server.")
        unit_telemetry= Queue(1)
        proc = Process(target=DataServer, 
                       name= "DataServer", args=(DATA.srvrHost, DATA.srvrPort, 
                                                 DATA.unitName, unit_telemetry)
                       )
        proc.daemon = False
        proc.start()
        logger.info("The process %s was created" % (Process.__name__))
        while True:
            telemetry = SS(DATA).telemetry
            unit_telemetry.put(telemetry)            

            if ESI.is_index_exist(DATA.elasticIndex) == False:
                ESI.create_index(DATA.elasticIndex,
                                 DATA.elasticShards,
                                 DATA.elasticReplicas
                                 )
            logger.debug("Connection successfully")
            logger.debug("Formatting data ...")
            indexPath ="%s%s"%(DATA.indexConf, DATA.elasticFile)

            ESI.indexing_data(DATA.elasticIndex, DATA.elasticIndexType, 
                              telemetry, DATA.elasticDefinition, indexPath)
            sleep(5)
            
if __name__ == '__main__':
    MonitorModule()