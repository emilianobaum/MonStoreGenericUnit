#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .server import CreateServer
from json import load
from json import JSONDecoder 
import logging

logger = logging.getLogger('Monitor & Indexing Unit.Data Server')

class DataServer(CreateServer):
    
    def __init__(self, host, port, unit, unitTelemetry, elasticDefinition):
        __author__ = "Emiliano A. Baum"
        __contact__ = "emilianobaum@conae.gov.ar"
        __copyrigth__ = "2017/04/27, Data Server Unit"
        __last_revision__= "2017-10-02"
        __license__ = "GPLv3"
        __version__ = "2.0, 20171002. According python3. Include activity list."
        __description__ = "Data server to distribute telemetry and activity list for the equipment defined in configuration file."
        s = self.create_socket(host, port)
        while True:
            try:
#                 print("Unit telemetry: ", unitTelemetry)
                if not unitTelemetry.empty():
                    telemetry = unitTelemetry.get()
                    self.__load_definition( telemetry[0], elasticDefinition)
                    self.data_server(s, unit, telemetry, 
                                     self.__load_definition(
                                         telemetry[0], elasticDefinition)
                                     )
            except Exception as err:
                print("Error: ",err)
                logger.error(err)
#                 self.create_socket(host, port)
                pass

    def __load_definition(self,unit,  elasticDefinition):
        f = open("configurations/%s"%elasticDefinition)
        jsonLoad= load(f)
        f.close()
        tags = []
        for n in jsonLoad[unit]['description']:
            tags.append(n['value'][0])
        return tags
            
