#!/usr/bin/python3
# -*- coding: utf-8 -*-

from sys import  exit
from time import sleep
from datetime import datetime
import socket
import logging

logger = logging.getLogger('Monitor & Indexing Unit.Data Server.Server')

# host = "127.0.0.1"
# port = 2345

class CreateServer():
    
    def create_socket(self, host, port):
        #===============================================================
        # Creates the  socket to publish telemetry stream
        #===============================================================
        print("HOST: ",host)
        print("PORT: ",port)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             s.setblocking(False)
            print("S: ",s)
        except OSError as err:
            print("MSG: ",err)
            logger.error(err)
            s = None
            exit()
        try:
            s.bind((host, port))
            s.listen(5)
        except OSError as err:
            print("MSG: ",err)
            logger.error(err)
            s.close()
            exit()
        return s

    def data_server(self, s, unit, telemetry, tags):
        print("S: ",s)
        print("Telemetria  en el server: ",telemetry)
        print("Tags: ",tags)
        data2send = '%s | %s |' % (
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), 
            unit)
#         if type(telemetry) == list :
        data2send += telemetry[0]
        tagPos = 0
        for n in telemetry[1].split(';'):
            print("N: ",n)
            print("Tag pos: ",tagPos)
            print("tags[tagPos]: ",tags[tagPos])
            if len(n) == 0:
                data2send += "%s : %s" %(tags[tagPos], n)
            else:
                data2send += ";%s : %s" %(tags[tagPos], n)
            tagPos += 1
#         else:
#             data2send += "%s"%(telemetry)
        print("Data 2 send: ",data2send)
        conn, addr = s.accept()
        print('Connected by', addr)
        data = conn.recv(10)
        print("Data: ",data)
        try:
            conn.send(data2send.encode())
        except Exception as e:
            print("Error sending data: ",e)
        return True
