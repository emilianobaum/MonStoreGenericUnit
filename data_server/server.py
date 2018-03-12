#!/usr/bin/python3
# -*- coding: utf-8 -*-

from sys import  exit
from time import sleep
from datetime import datetime
import socket
import logging
import logging.handlers

logger = logging.getLogger('Monitor & Indexing Unit.Data Server.Server')

class CreateServer():
    
    def create_socket(self, host, port):
        """
        Creates the  socket to publish telemetry stream.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError as err:
            logger.error(err)
            s = None
            exit()
        try:
            s.bind((host, port))
            s.listen(5)
        except OSError as err:
            logger.error(err)
            s.close()
            exit()
        return s

    def data_server(self, s, unit, telemetry, tags):
        data2send = '%s | %s |' % (
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), 
            unit)
        data2send += telemetry[0]
        tagPos = 0
        for n in telemetry[1].split(';'):
            if len(n) == 0:
                data2send += "%s : %s" %(tags[tagPos], n)
            else:
                data2send += ";%s : %s" %(tags[tagPos], n)
            tagPos += 1
        conn, addr = s.accept()
        logger.debug("Accepted connection (%s) from addr %s."%(conn, addr))
        data = conn.recv(10)
        try:
            conn.send(data2send.encode())
        except Exception as e:
            logger.error("Error sending data: ",e)
        return True
