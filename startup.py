#!/usr/bin/python3
# -*- coding: utf-8 -*-

from __future__ import absolute_import

from monitor_unit import StartService as MU
from es_index__module import ESIndices
from data_server import DataServer
from load_config import ConfigData

from multiprocessing import Process, Queue
from datetime import datetime
from time import sleep
from load_config import ConfigData as CD
import logging
import logging.handlers

logger = logging.getLogger("Monitor & Indexing Unit")

class MonitorModule(ConfigData):
    
    def load_logger(self, data):
        """
        Load logger configuration.
        """
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler(
            filename=('%s/%s'%(data.dirLog, data.fileLog)),
                      mode='a', maxBytes=5000000, backupCount=5, 
                      encoding='utf-8')
        formatter = logging.Formatter(
            "%(asctime) 15s - %(process)d - %(name)s -\
             %(lineno)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return True

    def __init__(self):
        __author__ = "Emiliano A. Baum"
        __contact__ = "ebaum@conae.gov.ar"
        __copyrigth__ = "2018-01-26, Monitoring unit module."
        __license__ = "GPLv3"
        __version__ = ("20180202, v3.0 Using docker running like service")
        __description__ = "Tool for monitor and indexing data to cluster and server for "
        __last_revision__= "2018-03-12"
        
        DATA = CD("configurations/configuration.json")
        self.load_logger(DATA)
        logger.info("Starting unit %s."%DATA.unitName)
        if DATA.elasticIndexTime == 'True':
            timeFormating = datetime.strftime(datetime.utcnow(), 
                                              DATA.elasticIndexTimeFormat)
            DATA.elasticIndex = "%s-%s"%(DATA.elasticIndex, timeFormating)
        ESI = ESIndices()
        logger.debug("Try connection to the cluster")
        ESI.conn(DATA)
        logger.debug("Connection successfully.")
        logger.debug("Start data server.")
        unitTelemetry = Queue(1)
        proc = Process(target=DataServer, 
                       name= "DataServer", args=(DATA.srvrHost, DATA.srvrPort, 
                                                 DATA.unitName, unitTelemetry, 
                                                 DATA.elasticDefinition)
                       )
        proc.daemon = True
        proc.start()
        logger.info("The process %s was created." % (Process.__name__))

        while True:
            telemetry = MU(DATA).telemetry
            if telemetry:
                if unitTelemetry.full() :
                    unitTelemetry.get()
                unitTelemetry.put(telemetry)
                if not ESI.is_index_exist(DATA.elasticIndex):
                    logger.info("The index is not created, send the message to the cluster.")
                    ESI.create_index(DATA.elasticIndex, 
                                     DATA.elasticShards,
                                     DATA.elasticReplicas
                                     )
                logger.debug("Formatting data ...")
                indexPath ="%s%s"%(DATA.indexConf, DATA.elasticFile)
                ESI.indexing_data(DATA.elasticIndex, DATA.elasticIndexType, 
                                  telemetry, DATA.elasticDefinition, indexPath)
            sleep(5)

if __name__ == '__main__':
    MonitorModule()
