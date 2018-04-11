#!/usr/bin/python3
#-*- coding utf-8 -*-

__author__ = "Emiliano A. Baum"
__contact__ = "emilianobaum@gmail.com"
__copyrigth__ = "2017-10-26, Antenna 13 Datron Monitoring Module."
__last_revision__= "2017-10-26, Refresh time and encoded for queries"
__license__ = "GPLv3"
__homepage__ = ""
__description__ = ("Monitoring module for antenna Datron 13 mts and \
sending to Python Cluster Administrator indexing tool for store in \
GS-Cluster.")
__version__ = ("20171026,v1.1. Adapted for new monitoring module.")

import socket
from time import sleep
import logging, logging.handlers

logger = logging.getLogger('Monitor Unit.Antenna 13 DTR')

class Antena135dtr(object):

    def __connection(self):
        try:
            logger.info("Tries to connect with the unit.")
            ip = self.cfg['unit description']['ip']
            puerto = self.cfg['unit description']['port']
            timeout = self.cfg['unit description']['socket timeout']

            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                                                        socket.SOL_TCP)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((ip, int(puerto)))
            s.settimeout(float(timeout))
            logger.info("Connection sucesfull.")
        except socket.error as e:
            logger.error(
            "Error when try to connect with the unit (%s)." % e)
            print("Error when try to connect with the unit (%s)." % e)
        except Exception as e:
            logger.error(
            "Error when try to connect with antenna (%s)." % e)
            print("Error when try to connect with antenna (%s)." % e)
        return s

    def __get_data(self, s):
        #~ try:
        comando = 'S0\r\n'
        enviado = s.send(comando.encode())
        recibido = s.recv(2048)
        return recibido

    def __parse_data(self, received):
        try:
            stream = str()
            for n in ((received).split(',')[1:]):
                if n.find("\r\n") != -1:
                    stream += "%s"%n[:-2]
                else:
                    stream += "%s;"%n
            
            summaryAlertFlag = str(stream.split(";")[0])
            selectedControl = str(stream.split(";")[1])
            tilt = str(stream.split(";")[2])
            coordinateSystem = str(stream.split(";")[3])
            selectedScan = str(stream.split(";")[4])
            scanGoHalt = str(stream.split(";")[5])
            elStow = str(stream.split(";")[6])
            elLimit = str(stream.split(";")[7])
            elMode = str(stream.split(";")[8])
            elAcquisitionMode = str(stream.split(";")[9])
            elProgramServoType = str(stream.split(";")[10])
            elCommand = str(stream.split(";")[11])
            elPosition = str(stream.split(";")[12])
            elProgramOffset = str(stream.split(";")[13])
            elCableWrap = str(stream.split(";")[14])
            azStow = str(stream.split(";")[15])
            azLimit = str(stream.split(";")[16])
            azMode = str(stream.split(";")[17])
            azAcquisitionMode = str(stream.split(";")[18])
            azProgramServoType = str(stream.split(";")[19])
            azCommand = str(stream.split(";")[20])
            azPosition = str(stream.split(";")[21])
            interLock = str(stream.split(";")[22])
            autoTrack = str(stream.split(";")[23])
            timeTag = str(stream.split(";")[24])
            azMleTarget = str(stream.split(";")[25])
            elMleTarget = str(stream.split(";")[26])
            azActualServoType = str(stream.split(";")[27])
            elActualServoType = str(stream.split(";")[28])
            cor = str(stream.split(";")[29])
            azAutoTrackError = str(stream.split(";")[30])
            elAutoTrackError = str(stream.split(";")[31])
            sSignalStrength = str(stream.split(";")[32])
            xSignalStrength = str(stream.split(";")[33])
            clockSource = str(stream.split(";")[34][:-1])
            
            stream =[summaryAlertFlag, selectedControl, tilt, 
            coordinateSystem, selectedScan, scanGoHalt, elStow, elLimit,
             elMode, elAcquisitionMode, elProgramServoType, elCommand, 
             elPosition, elProgramOffset, elCableWrap, azStow, azLimit, 
             azMode, azAcquisitionMode, azProgramServoType, azCommand, 
             azPosition, interLock, autoTrack, timeTag, azMleTarget, 
             elMleTarget, azActualServoType, elActualServoType, cor, 
             azAutoTrackError, elAutoTrackError, sSignalStrength, 
             xSignalStrength, clockSource]
        except (IndexError, socket.error) as e:
            logger.error('Error %s parsing data %s'%(e, received))
            stream = False
            pass
        return stream

    def main_mon(self, unit_telemetry, timer):
        s = self.__connection()
        refresh = [self.cfg['program configuration']['configuration']
                                                ['slow refresh'], False]
        while True:
            try:
                received = self.__get_data(s)
                #~ if received != False:
                dataFormated = self.__parse_data(received)
                """
                El primer elemento de la lista debe ser el valor\
                de "PyEs Cluster Admin" -> "puerto"
                """
                data2publish =['puerto antena','; '.join(dataFormated)]
                unit_telemetry.put(data2publish)
                unit_telemetry.put(data2publish)
                if timer.empty() == False:
                    refresh = timer.get()
                sleep(int(refresh[0]))
            except socket.error as e:
                try:
                    s = self.__connection()
                except Exception:
                    logger.error("Connection with unit error: %s."%e)
                    pass
                pass
            except Exception as e:
                logger.error("Undefined error: %s."%e)
                sleep(int(refresh[0]))
                pass
