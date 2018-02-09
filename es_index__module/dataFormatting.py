#!/usr/bin/python3
# -*- coding: utf-8 -*-

from sys import exit
from datetime import datetime
import logging

logger = logging.getLogger('Py-ES Storage.Data Formating')

class DataTypes():
    """
    Identifies data type and transforms according directives `unit configuration file`.
    The elements in the `index file` should be in the same order to be acquires by server instance.
    """    
    def __timetypes(self,fmt,value):
        """Convert to datetime objects."""
        #~ print "Ingresa a formatear como tipo datetime ",item
        value = ''
        return value
        
    def __numTypes(self,item,value):
        """Convert to python numeric types"""
        #~ print "Ingresa a formatear como tipo numerico ",item
        if item == 'integer':
            value = int(value)
        elif item == 'float':
            try:
                value = float(value)
            except ValueError:
                value = 0.0
                pass
        elif item == 'long':
            value = int(value)
        elif item == 'absolute':
            value = abs(value)
        elif item == 'hex':
            value = hex(value)
        return value

    def __characters(self,item,value):
        """Manage string types."""
        #~ print "filter string types."
        value = value
        return value
        
    def defined_type(self,stream,config):
        """
        Converts the data, according to the configuration file for the index, which will be stored in the cluster Elasticsearch.
        """
        config.elastic_data()
        logger.debug("CONFIG elastic_type: %s."%config.elastic_type)
        logger.debug("CONFIG elastic_type: %s."%config.elastic_filter)
        msg = {}
        msg['@timestamp'] = datetime.utcnow()
        for item in config.elastic_filter:
            #~ Filter for different kind of data stream formats
            if item == 'convert':
                a = 0
                #~ Processing values defines in mutate type
                for value in config.elastic_filter[item]:
                    #~ print "VALUE: ",value
                    try:
                        if (value['value'][1] == 'integer' or value['value'][1] == 'float' or value['value'][1] == 'complex' or value['value'][1] == 'hex'):
                            msg[value['value'][0]] = self.__numTypes(value['value'][1],stream[a])
                        elif (value['value'][1] == 'char' or value['value'][1] == 'string'):
                            msg[value['value'][0]] = self.__characters(value['value'][1],stream[a])
                        elif (value['value'][1] == 'boolean'):
                            msg[value['value'][0]] = bool(stream[a])
                        elif value['value'][1] == 'datetime':
                            msg[value['value'][0]] = self.__timetypes(value['value'][1],stream[a])
                        else:
                            #~ Undefined type.
                            msg[value['value'][0]] = stream[a]
                        a+=1                        
                    except IndexError as e:
                        logger.debug("Not enough arguments to convert for index %s. Inserting 'None' value."%(config.elastic_index))
                        msg[value['value'][0]] = 0
                        pass
                    except ValueError as e:
                        logger.warning("Error (%s) when formating data %s. Replace with False"%(e,value))
                        msg[value['value'][0]] = 0
                        pass
                    except Exception as e:
                        logger.critical("Error (%s) when formating data %s."%(e,value))
                        exit(1)
        return msg
    
