#!/usr/bin/python3
# coding: utf-8

from json import load
from sys import exit
from datetime import datetime
import logging

logger = logging.getLogger('Py-ES Storage.Load Index Configuration')

class LoadIndexConfiguration():
    
    def load_file(self, file):
        """
        Load configuration file.
        """
        try:
            f = open(file)
            self.indexConf = load(f)
            f.close()
            print("Open configuration file %s." % (file))
        except (IOError,OSError, ValueError) as e:
            print("Error ",e)
            logger.error(('Error reading configuration file -> %s. Error -> %s'
                           % (file, e)))
            exit()
    
    def __analize_data(self, telemetryValue, fieldType):
        
        print("Field Type: ",fieldType)
        print("Telemetry Value: ",telemetryValue)
        if fieldType == 'int':
            value = int(telemetryValue)
        elif fieldType == 'float':
            value = float(telemetryValue)
        elif fieldType == 'char':
            value = chr(telemetryValue)
        elif fieldType == 'string':
            value = str(telemetryValue)
            
        return value
    
    def make_json(self, telemetry):
        print("Make index stream")
        self.msg = {}
        self.msg['@timestamp'] = datetime.utcnow()
        telemetryId = telemetry[0]
        telemetryData = telemetry[1].split(";")
#         print("telemetryData ",telemetryData)
#         print("Len telemetry data: ",(len(telemetryData)))
        i = 0
        for key in self.indexConf[telemetryId]['description']:
#             print("Indice %s:%s "%(key,value))
            fieldName = key['value'][0]
            fieldType = key['value'][1]
            try:
#                 print("Field Name: %s"%(fieldName))
#                 print("Field Type: %s"%(fieldType))
#                 print("Telemetry type: ",type(telemetry))
#                 print("Telemetry i: ",(telemetryData[i]))             
#                 self.__analize_data(telemetryData[i], fieldType)
                self.msg[fieldName] = self.__analize_data(telemetryData[i], fieldType)
            except IndexError:
                logger.debug("No data available for %s. Complete with 'no data' string"%fieldName)
                self.msg[fieldName] = 'No Data'
                pass
            except BaseException as e:
                logger.critical("Exit by error ",e)
                exit(0)
            i += 1
    def __init__(self, file, telemetry):
        
        __description__ = "Load index configuration for indexing data, return json doc"
        
        self.load_file(file)
        self.make_json(telemetry)
        
    