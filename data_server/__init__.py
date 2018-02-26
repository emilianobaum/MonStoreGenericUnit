#!/usr/bin/python3
from .server import CreateServer

class DataServer(CreateServer):
    def __init__(self, host, port, unit, unit_telemetry):
        __author__ = "Emiliano A. Baum"
        __contact__ = "emilianobaum@conae.gov.ar"
        __copyrigth__ = "2017/04/27, Data Server Unit"
        __last_revision__= "2017-10-02"
        __license__ = "GPLv3"
        __version__ = "2.0, 20171002. According python3. Include activity list."
        __description__ = "Data server to distribute telemetry and activity list\
         for the equipment defined in configuration file."
        s = self.create_socket(host, port)
        while True:
            try:
                self.data_server(s, unit, unit_telemetry)
            except Exception as e:
                print("E: ",e)
                self.create_socket(host, port)
                pass
