#!/usr/bin/python

__author__ = "Emiliano A. Baum"
__contact__ = "ebaum@conae.gov.ar"
__copyrigth__ = "29/09/2014, Zodiac HDR Global Table Monitor Unit."
__last_revision__= "2017-09-28"
__license__ = "GPLv3"
__version__ = ("20170928, v2 Modifications creating directory structure,\
format according python 3")
__homepage__ = ""
__description__ = ("Generic module for Zodiac CRT Global tables.")

import socket
from time import sleep
from datetime import datetime,timedelta
from os import path
from ctypes import pointer, c_int,c_float, cast, POINTER
from struct import unpack_from, error
from multiprocessing import Queue
import logging, logging.handlers
logger = logging.getLogger('Monitor Unit.Z1 CRT Global Table')


class Z1CRTGlobalTable(object):

    def __connection(self):
        try:
            logger.info("Tries to connect with the unit.")
            ip = self.cfg['unit description']['ip']
            puerto = self.cfg['unit description']['port']
            timeout = self.cfg['unit description']['socket timeout']
            
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM, 
                            socket.SOL_TCP)
            s.connect((ip, int(puerto)))
            s.settimeout(float(timeout))
            logger.info("Connection sucesfull.")
        except socket.error as e:
            logger.error("Error when try to connect with the unit (%s)." % e)
            s = False
            sleep(2)
            pass
        except Exception as e:
            logger.error("Error when try to connect with CRT Global T(%s)." % e)
            s = False
            sleep(2)
            pass
        return s
            
    def __get_data(self, s):
        #~ try:
        comando = b'\x49\x96\02\xd2\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x10\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb6\x69\xfd\x2e'
        enviado = s.send(comando)
        recibido = s.recv(2048)
        #~ except (socket.error, UnboundLocalError) as e:
            #~ logger.critical('Connection fail %s.'%e)
            #~ recibido = False
            #~ pass
        return recibido

    def __parse_data(self, recibido):
        try:
            tiempo = datetime.utcnow().strftime("%Y-%jT%H:%M:%S")
            encabezado = unpack_from('!i', recibido,0)[0]
            largoMensaje = unpack_from('!i', recibido,4)[0]
            tabla = unpack_from('!i',recibido,12)[0]
            logging = unpack_from('!i', recibido,20)[0]
            reffreqsel = unpack_from('!i', recibido,24)[0]
            leapyear = unpack_from('!i', recibido,28)[0]
            timecurroffset = unpack_from('!i', recibido,32)[0]
            timedatenextoffset1 = unpack_from('!i', recibido,36)[0]
            timenextoffset1 = unpack_from('!i', recibido,40)[0]
            timedatenextoffset2 = unpack_from('!i', recibido,44)[0]
            timenextoffset2 = unpack_from('!i', recibido,48)[0]
            videotestpointA = unpack_from('!i', recibido,52)[0]
            timecode = unpack_from('!i', recibido,56)[0]
            downlinkinputselA = unpack_from('!i', recibido,60)[0]
            downlinkinputselB = unpack_from('!i', recibido,64)[0]
            videotestpointB = unpack_from('!i', recibido,68)[0]
            ifinputportconfA = unpack_from('!i', recibido,72)[0]
            ifinputportconfB = unpack_from('!i', recibido,76)[0]
            ifinputportconfC = unpack_from('!i', recibido,80)[0] 
            
            ctxdoc = unpack_from('!i', recibido,104)[0]
            opmode = unpack_from('!i', recibido,108)[0]
            tcutsutms1_2 = unpack_from('!i', recibido,112)[0]
            rau = unpack_from('!i', recibido,116)[0]
            ifm = unpack_from('!i', recibido,120)[0]
            ifrdcu = unpack_from('!i', recibido,124)[0]
            tmu = unpack_from('!i', recibido,128)[0]
            noisegeneratorstatus = unpack_from('!i', recibido,132)[0]
            tmuifrasociation = unpack_from('!i', recibido,136)[0]
            timealarm = unpack_from('!i', recibido,140)[0]
            #~ 2 X Reserved
            tcutmsalarm = unpack_from('!i', recibido,152)[0]
            #~ 1 X Reserved
            #~ IFM Alarma
            ifm1alarm = unpack_from('!b', recibido,160)[0]
            ifm2alarm = unpack_from('!b', recibido,161)[0]
            ifralarm = unpack_from('!i', recibido,164)[0]
            tmualarm = unpack_from('!i', recibido,168)[0]
            noisegenerator = unpack_from('!i', recibido,172)[0]
            #~ ==========================
            #~ Alarma de temperatura de FPGA
            masterboard = unpack_from('!b', recibido,176)[0]
            masterfpga = unpack_from('!b', recibido,177)[0]
            slaveboard = unpack_from('!b', recibido,178)[0]
            slavefpga = unpack_from('!b', recibido,179)[0]
            #~ ==========================
            miscellaneousalarm = unpack_from('!i', recibido,180)[0]
            projectid = unpack_from('!i', recibido,184)[0]
            softwareid = unpack_from('!i', recibido,188)[0]
            hardwareid = unpack_from('!i', recibido,192)[0]
            selectfreqandstatuspllvcx0 = unpack_from('!i', recibido,196)[0]
            timetagcod = unpack_from('!i', recibido,200)[0]
            timetagA = unpack_from('!i', recibido,204)[0]
            timetagB = unpack_from('!i', recibido,208)[0]
            irigb_nasa36decoder = unpack_from('!i', recibido,212)[0]
            monitoringclients = unpack_from('!i', recibido,216)[0]
            controlclients = unpack_from('!i', recibido,220)[0]
            simclient = unpack_from('!i', recibido,224)[0]
            telcommandclients = unpack_from('!i', recibido,224)[0]
            rngclients = unpack_from('!i', recibido,228)[0]
            measclients = unpack_from('!i', recibido,232)[0]
            loggingclients = unpack_from('!i', recibido,236)[0]
            spectrumanalysisclients = unpack_from('!i', recibido,240)[0]
            doppleralysisclients = unpack_from('!i', recibido,244)[0]
            fmrealtimealysisclients = unpack_from('!i', recibido,248)[0]
            telemetryAclient = unpack_from('!i', recibido,252)[0]
            telemetryBclient = unpack_from('!i', recibido,256)[0]
            telemetryCclient = unpack_from('!i', recibido,260)[0]
            telemetryDclient = unpack_from('!i', recibido,264)[0]
            telemetryEclient = unpack_from('!i', recibido,268)[0]
            telemetryFclient = unpack_from('!i', recibido,270)[0]
            
            stream = ('%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n'%(
            reffreqsel,leapyear,timecurroffset,timedatenextoffset1,timenextoffset1,timedatenextoffset2,timenextoffset2,videotestpointA,timecode,downlinkinputselA,downlinkinputselB,videotestpointB,ifinputportconfA,
            ifinputportconfB,ifinputportconfC,ctxdoc,opmode,tcutsutms1_2,rau,ifm,ifrdcu,tmu,noisegeneratorstatus,tmuifrasociation,timealarm,tcutmsalarm,ifm1alarm,ifm2alarm,ifralarm,tmualarm,noisegenerator,
            masterboard,masterfpga,slaveboard,slavefpga,miscellaneousalarm,projectid,softwareid,hardwareid,selectfreqandstatuspllvcx0,timetagcod,timetagA,timetagB,irigb_nasa36decoder,monitoringclients,
            controlclients,simclient,telcommandclients,rngclients,measclients,loggingclients,spectrumanalysisclients,doppleralysisclients,fmrealtimealysisclients,telemetryAclient,telemetryBclient,telemetryCclient,
            telemetryDclient,telemetryEclient,telemetryFclient))
        except (IndexError, error) as e:
            logger.error('Error %s parsing data %s'%(e, recibido))
            data = False
            pass
        return stream
    
    def main_mon(self, unit_telemetry, timer):
        s = self.__connection()
        refresh = [self.cfg["program configuration"]["configuration"]
                                            ["slow refresh"], False]
        while True:
            try:
                received = self.__get_data(s)
                #~ if received != False:
                dataFormated = self.__parse_data(received)
                """
                El primer elemento de la lista debe ser el valor\
                de "PyEs Cluster Admin" -> "puerto"
                """
                data2publish =['puerto glob',('%s '%dataFormated)]                
                unit_telemetry.put(data2publish)
                unit_telemetry.put(data2publish)
                if timer.empty() == False:
                    refresh = timer.get()
                sleep(int(refresh[0]))
            except socket.error as e:
                s = self.__connection()
                logger.error("Connection with unit error: %s."%e)
                pass
            except Exception as e:
                logger.error("Undefined error: %s."%e)
                sleep(int(refresh[0]))
                pass
