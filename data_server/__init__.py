#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .server import CreateServer
from json import load
from json import JSONDecoder 
import logging
import logging.handlers

logger = logging.getLogger('Monitor & Indexing Unit.Data Server')

class DataServer(CreateServer):
    
    def __init__(self, host, port, unit, unitTelemetry, elasticDefinition):
        __description__ = "Data server to distribute telemetry and activity list for the equipment defined in configuration file."
        s = self.create_socket(host, port)
        while True:
            try:
                if not unitTelemetry.empty():
                    telemetry = unitTelemetry.get()
                    print("Telemetry: ",telemetry)
                    self.__load_definition( telemetry[0], elasticDefinition)
                    self.data_server(s, unit, telemetry, 
                                     self.__load_definition(
                                         telemetry[0], elasticDefinition)
                                     )
            except Exception as err:
                logger.error(err)
                pass

    def __load_definition(self,unit,  elasticDefinition):
        f = open("configurations/%s"%elasticDefinition)
        jsonLoad= load(f)
        f.close()
        tags = []
        for n in jsonLoad[unit]['description']:
            tags.append(n['value'][0])
        return tags
            
