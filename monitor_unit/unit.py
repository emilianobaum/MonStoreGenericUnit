#!/usr/bin/python3
# -*- coding: utf-8 -*-

import socket
import logging
import logging.handlers
# from struct import unpack_from

logger = logging.getLogger('Monitor & Indexing Unit.Monitor.Unit')

class Monitor(object):
    
    def monitor(self, cfg):
#         Here you should set the command to be sent. 
        comando = ('<?xml version="1.0" encoding="utf-8"?>\
            <soap:Envelope xmlns:\
            xsi="http://www.w3.org/2001/XMLSchema-instance"\
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"\
            xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">\
            <soap:Body>\
            <Execute xmlns="http://www.conae.gov.ar/CGSS/XPNet/GSOAP">\
            <aXPNetMessage>\
            <I_INU-S xmlns="http://www.conae.gov.ar/CGSS/XPNet">\
            <queryVars><query><INU>\
            <ID>sStartTime</ID><ID>iDataFreq</ID><ID>iClkFreq</ID>\
            <ID>sLineCode</ID><ID>sClkSample</ID><ID>iRxTotalBytes</ID>\
            <ID>sFileName</ID><ID>sStopTime</ID><ID>sUnitStatus</ID>\
            <ID>sUnitAlarms</ID><ID>iUSBRate</ID>\
            </INU></query></queryVars>\
            </I_INU-S></aXPNetMessage></Execute></soap:Body>\
            </soap:Envelope>')
        ip = cfg.unitIp
        puerto= cfg.unitPort
        try:
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, int(puerto)))
            logger.debug("Connection sucesfully")
            try:
                s.send(comando)
            except Exception:
                s.send(comando.encode())
            logger.debug("Message send ...")
            recibido = s.recv(4096).decode()
            logger.debug("Message close ...")
            telemetry = [cfg.unitName,self.format_stream(recibido)]
            return telemetry
        except Exception as e:
            telemetry = False
            logger.error("Error %s connecting with the unit."%e)
            print("Error %s connecting with the unit."%e)
            pass

    def format_stream(self, received):
#         This function must return a csv stream
        try:
            stream = received[received.find('&lt;iClkFreq&gt;'):received.rfind('&lt;/sUnitStatus&gt')]
            stream = stream.replace(';','')
            stream = stream.replace('&lt','')
            stream = stream.replace('&gt','')
            stream = stream.replace('iClkFreq','')
            stream = stream.replace('iDataFreq','')
            stream = stream.replace('iRxTotalBytes','')
            stream = stream.replace('iUSBRate','')
            stream = stream.replace('sClkSampleSet','')
            stream = stream.replace('sClkSample','')
            stream = stream.replace('sFileName','')
            stream = stream.replace('sLineCodeSet','')
            stream = stream.replace('sLineCode','')
            stream = stream.replace('Set','')
            stream = stream.replace('sStartTime','')
            stream = stream.replace('sStopTime','')
            stream = stream.replace('sUnitAlarms','')
            stream = stream.replace('sUnitStatus','')
            stream = stream.replace('//',';')
            stream = stream.replace('/',';')
        except (IndexError, socket.error) as e:
            logger.error('Error %s parsing data %s'%(e, received))
            print('Error %s parsing data %s'%(e, received))
            stream = False
            pass
        return stream
    
