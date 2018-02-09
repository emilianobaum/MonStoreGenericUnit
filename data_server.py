#!/usr/bin/python3
# -*- coding: utf-8 -*-
__author__ = "Emiliano A. Baum"
__contact__ = "emilianobaum@conae.gov.ar"
__copyrigth__ = "2017/04/27, Data Server Unit"
__last_revision__= "2017-10-02"
__license__ = "GPLv3"
__version__ = "2.0, 20171002. According python3. Include activity list."
__description__ = "Data server to distribute telemetry and activity list\
 for the equipment defined in configuration file."

from sys import  exit
from time import sleep
from datetime import datetime
import socket
import logging

logger = logging.getLogger('Monitor Unit.Data Server')

class DataServer(object):
    
    def __create_socket(self):
        #===============================================================
        # Creates the  socket to publish telemetry stream
        #===============================================================
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 20)
            s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP,  1)
            s.settimeout(6.0)
        except socket.error as msg:
            logger. error("Error creating socket %s"% msg)
            s.close()
            s = None
            exit(1)
        return s

    def __data_server(self, s, unit, telemetry, activities, host, port):
        data = '%s | %s |' % (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            unit)
        if type(telemetry ) == list :
            for n in telemetry:
                if type(n) == list:
                    data += ','.join(n)
                else:
                    if len(n) == 0:
                        data += "%s," % n
                    else:
                        data += ",%s" % n
        else:
            data += "%s" % telemetry
        
        # Added activitie list
        data += "|Activities assign to the unit: %s" % activities
        # Sending data to multidicast address activity
        s.sendto(data.encode(), (host, port))
        
        return True
    
    def main_srvr(self,  unit_telemetry, timer):
        """
        Start data server.
        """
        unit = self.cfg["unit description"]["name"]
        host = self.cfg["program configuration"]["data server"]["host"]
        port = self.cfg["program configuration"]["data server"]["port"]
        
        refresh = [int(self.cfg['program configuration']
                            ['configuration']['slow refresh']), False]
        s = self.__create_socket()
        while 1:
            try:
                telemetry = unit_telemetry.get()
                logger.debug("Telemetry received in Data Server %s." %
                                                            telemetry)
                if timer.empty() == False:
                    refresh = timer.get()
                if s != None:
                    self.__data_server(s, unit, telemetry, refresh[1],
                                                    host, port)
                """
                Es importante el refresh, de lo contrario toma los dos 
                elementos guardados en la Queue
                """
                sleep(refresh[0])
            except socket.error as e: 
                logger.error("Socket error (%s), retry connection" % e)
                try:
                    s.close()
                except:pass
                sleep(refresh)
                s = self.__create_socket()
                pass
            except Exception as e:
                logger.critical("Undefined Fatal Error in data server\
                                                            %s." % e)
                pass
        return True
