#!/usr/bin/python3
# -*- coding: utf-8 -*-


from sys import  exit
from time import sleep
from datetime import datetime
import socket
import logging

logger = logging.getLogger('Monitor Unit.Data Server')

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
        except OSError as msg:
            print("MSG: ",msg)
            s = None
            exit()
        try:
            s.bind((host, port))
            s.listen(5)
        except OSError as msg:
            print("MSG: ",msg)
            s.close()
            exit()
        return s

    def data_server(self, s, unit, unit_telemetry):
        data2send = '%s | %s |' % (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            unit)
        telemetry = unit_telemetry.get()
        if type(telemetry) == list :
            data2send += telemetry[0]
            for n in telemetry[1:]:
                if type(n) == list:
                    data2send += ','.join(n)
                else:
                    if len(n) == 0:
                        data2send += "%s," % n
                    else:
                        data2send += ",%s" % n
        else:
            data2send += "%s"%(telemetry)
        print("Data 2 send: ",data2send)        
        conn, addr = s.accept()
        print('Connected by', addr)
        data = conn.recv(10)
        print("Data: ",data)
        conn.send(data2send.encode())
        return True


                
        
