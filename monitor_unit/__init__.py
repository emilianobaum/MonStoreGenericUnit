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
#          The main tag of configuration.json document indicates the name of the unit to be monitoring.
        self.cfg = config["Data Server & Monitor Configuration"]
        return True
    
    def __init__(self, data):
        __description__ = ("Generic monitor unit module. Replace the unit module with\
         the new one but don't touch this module.")
        self.telemetry = self.monitor(data)
        
        