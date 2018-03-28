#!/usr/bin/python3
# -*- coding: utf-8 -*-

import socket
import logging
import logging.handlers
from struct import unpack_from

logger = logging.getLogger('Monitor & Indexing Unit.Monitor.Unit')

class Monitor(object):
    
    def monitor(self, cfg):
#         Here you should set the command to be sent. 
        comando = b'\x49\x96\02\xd2\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x10\x61\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb6\x69\xfd\x2e'
        ip = cfg.unitIp
        puerto= cfg.unitPort
        try:
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, int(puerto)))
            logger.debug("Connection sucesfully")
            s.send(comando)
            logger.debug("Message send ...")
            recibido = s.recv(4096)
            logger.debug("Message close ...")
            telemetry = [cfg.unitName,self.format_stream(recibido)]
            return telemetry
        except Exception as e:
            telemetry = False
            logger.error("Error %s conectado con la unidad."%e)
            pass

    def format_stream(self, recibido):
#         This function must return a csv stream
        header = unpack_from('!i', recibido,0)[0]
        msgSize = unpack_from('!i', recibido,4)[0]
        tdfi = unpack_from('!i', recibido,8)[0]
        bitRate = unpack_from('!f', recibido,12)[0]
        bpskScf = unpack_from('!f', recibido,16)[0]
        pskDemLoopBW = unpack_from('!i', recibido,20)[0]
        pcmCode = unpack_from('!i', recibido,24)[0]
        synchWordMask = unpack_from('!i', recibido,28)[0]
        viterbiDec  = unpack_from('!i', recibido,32)[0]
        reedSolomon = unpack_from('!i', recibido,36)[0]
        frameBlocksize = unpack_from('!i', recibido,44)[0]
        synchWord = unpack_from('!i', recibido,48)[0]
        synchWordLength = unpack_from('!i', recibido,52)[0]
        operatingMode = unpack_from('!i', recibido,56)[0]
        syncThreshold = unpack_from('!i', recibido,60)[0]
        ctlThreshold = unpack_from('!i', recibido,64)[0]
        ltsThreshold = unpack_from('!i', recibido,68)[0]
        bitSlipWindow = unpack_from('!i', recibido,72)[0]
        frameSynch = unpack_from('!i', recibido,76)[0]
        frameCheck = unpack_from('!i', recibido,84)[0]
        crcPolynomial = unpack_from('!i', recibido,88)[0]
        crcPreset = unpack_from('!i', recibido,92)[0]
        frameSynchViterbiDec = unpack_from('!i', recibido,112)[0]
        matchedFilter = unpack_from('!i', recibido,116)[0]
        rollOffFactor = unpack_from('!f', recibido,120)[0]
        bpskDemodInput = unpack_from('!i', recibido,124)[0]
        tmStorageIDfirstFile= unpack_from('!i', recibido,128)[0]
        tmStorageNumbFilesWrite = unpack_from('!i', recibido,132)[0]
        ebN0 = unpack_from('!f', recibido,196)[0]
        bitSynchStat= unpack_from('!i', recibido,200)[0]
        pskDemStat = unpack_from('!i', recibido,204)[0]
        frameSynchStat = unpack_from('!i', recibido,208)[0]
        bitSlip= unpack_from('!i', recibido,212)[0]
        viterbiDecStat = unpack_from('!i', recibido,216)[0]
        reedSolomonDecStat = unpack_from('!i', recibido,220)[0]
        frameCheckStat = unpack_from('!i', recibido,224)[0]
        tmStorageNumbFilesTelStore = unpack_from('!i', recibido,236)[0]
        tmStorageFileSize = unpack_from('!i', recibido,240)[0]
        externalInputFrameSynchViterbiDecod = unpack_from('!i', recibido,244)[0]
        tmProfile = unpack_from('!i', recibido,248)[0]
        viterbiDecoderBER = unpack_from('!f', recibido,252)[0]
        tmStorageCurrFileIDnumb = unpack_from('!i', recibido,256)[0]
        tmStorageCurrOffsetCurrBytesFile = unpack_from('!i', recibido,260)[0]
        tmStorage = unpack_from('!i', recibido,264)[0]
        reedSolomonDecodBER = unpack_from('!i', recibido,268)[0]
        goodTransfFramesCountRSDecod = unpack_from('!i', recibido,272)[0] 
        badTransfFramesCountRSDecod = unpack_from('!i', recibido,276)[0]

        telemetry = (
            "%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s"%(
            header, msgSize, tdfi, bitRate, bpskScf, pskDemLoopBW,pcmCode,synchWordMask, viterbiDec, 
            reedSolomon, frameBlocksize, synchWord, synchWordLength, operatingMode,
            syncThreshold, ctlThreshold, ltsThreshold, bitSlipWindow, frameSynch, frameCheck, crcPolynomial, 
            crcPreset, frameSynchViterbiDec, matchedFilter, rollOffFactor, bpskDemodInput, 
            tmStorageIDfirstFile,  tmStorageNumbFilesWrite, ebN0, bitSynchStat, pskDemStat, frameSynchStat, 
            bitSlip, viterbiDecStat, reedSolomonDecStat, frameCheckStat, tmStorageNumbFilesTelStore, 
            tmStorageFileSize, externalInputFrameSynchViterbiDecod, tmProfile, viterbiDecoderBER, 
            tmStorageCurrFileIDnumb, tmStorageCurrOffsetCurrBytesFile, tmStorage, reedSolomonDecodBER, 
            goodTransfFramesCountRSDecod, badTransfFramesCountRSDecod))
        return telemetry
    
