#!/usr/bin/python3
# coding: utf-8

from __future__ import absolute_import

from .unit import Monitor
from json import load
import logging 
import logging.handlers

logger = logging.getLogger("Monitor & Indexing Unit.Monitor")

class StartService(Monitor):
    
    def load_conf(self, file):
        """
        Load configuration file from monitoring unit.
        """
        with open(file, "r") as fp:
            config = load(fp)
        #=======================================================================
        # The main tag of configuration.json document 
        # indicates the name of the unit to be monitoring.
        #=======================================================================
        self.cfg = config["Data Server & Monitor Configuration"]
        return True
    
    def __init__(self, data):

        __author__ = "Emiliano A. Baum"
        __contact__ = "ebaum@conae.gov.ar"
        __copyrigth__ = "2018-01-26, Monitoring unit module."
        __license__ = "GPLv3"
        __version__ = ("20180202, v3.0 Using docker running like service")
        __last_revision__= "2018-02-06"
        self.telemetry = self.monitor(data)
        
        