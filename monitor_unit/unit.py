#!/usr/bin/python3
# coding: utf-8

import socket
import logging

logger = logging.getLogger('Monitor Unit.unit')

class Monitor(object):
    
    def monitoreo(self, cfg):
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
        print("CFG: ",cfg)
        ip = cfg.unitIp
        puerto= cfg.unitPort
        print("IP: ",ip)
        print("Puerto: ",puerto)
        try:
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, int(puerto)))
            logger.debug("Connection sucesfully")
            s.send(comando.encode())
            logger.info("Message send ...")
            recibido = s.recv(2048)
            logger.debug("Message close ...")
            unitTelemetry = self.format_stream(recibido.decode())
            return unitTelemetry
        except Exception as e:
            logger.error("Error %s conectado con la unidad."%e)
            pass

    def format_stream(self, recibido):
        
        msje = recibido[recibido.find('&lt;iClkFreq&gt;'):recibido.rfind('&lt;/sUnitStatus&gt')]
        msje = msje.replace(';','')
        msje = msje.replace('&lt','')
        msje = msje.replace('&gt','')
        msje = msje.replace('iClkFreq','')
        msje = msje.replace('iDataFreq','')
        msje = msje.replace('iRxTotalBytes','')
        msje = msje.replace('iUSBRate','')
        msje = msje.replace('sClkSampleSet','')
        msje = msje.replace('sClkSample','')
        msje = msje.replace('sFileName','')
        msje = msje.replace('sLineCodeSet','')
        msje = msje.replace('sLineCode','')
        msje = msje.replace('Set','')
        msje = msje.replace('sStartTime','')
        msje = msje.replace('sStopTime','')
        msje = msje.replace('sUnitAlarms','')
        msje = msje.replace('sUnitStatus','')
        msje = msje.replace('//',';')
        msje = msje.replace('/',';')
        
        msje =  ['inu05',msje]
        return msje
    
            