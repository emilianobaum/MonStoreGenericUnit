#!/usr/bin/python3
# coding: utf-8

from json import load
from sys import exit
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
    
    def __analize_data(self, telemtryValue, fieldType):
        
        if 
    
    def make_json(self, telemetry):
        print("Make index stream")
        msg = {}
        msg['@timestamp'] = datetime.utcnow()
        for key in self.indexConf['Unit to Monitorize']['description']:
#             print("Indice %s:%s "%(key,value))
            print("Indice Item: %s"%(key['value']))
            self.__analize_data(self, telemtryValue, fieldType)
            msg[key['value'][0]] = key['value'][1]
        return msg
    def __init__(self, file, telemetry):
        
        __description__ = "Load index configuration for indexing data, return json doc"
        
        self.load_file(file)
        self.__analize_data(telemetry)
        
    