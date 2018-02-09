#!/usr/bin/python3
# -*- coding: utf-8 -*-

__author__ = "Emiliano A. Baum"
__contact__ = "ebaum@conae.gov.ar"
__copyrigth__ = "2017-01-26, Starter module for monitoring unit."
__version__ = "2"
__last_revision__= "2017-09-26"
__license__ = "GPLv3"
__homepage__ = ""
__description__ = ("Generic module to start all monitoring modules\
 for single unit")

from multiprocessing import Process, Queue
import logging
 
logger = logging.getLogger('Monitor Unit.Instance Generator')

class InstanceGenerator(object):
    
    def init_process(self, list_of_processes):
        """
        Generates the processes connected by a Queue of large 1 ...
        """
        try:
            """
            Create a queue of large 2, for data server and monitor unit.
            """
            unit_telemetry= Queue(2)
            timer = Queue(2)
            for process in list_of_processes:
                proc = Process(target=process, name= "%s" %
                                   process, args=(unit_telemetry, timer))
                proc.daemon = False
                proc.start()
                logger.info("The process %s was created" % 
                                                    (process.__name__))
            return True
        except Exception as e:
            logger.info("Error %s starting a new process %s."%(
                            e, process.__name__))
            return False
