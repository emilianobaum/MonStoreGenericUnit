#!/usr/bin/python3
# -*- coding: utf-8 -*-

from multiprocessing import Process, Queue
import logging
 
logger = logging.getLogger('Monitor Unit.Instance Generator')

class ProcessControl(object):
    
    def start_processes(self, list_of_processes):
        """
        Generates the processes connected by a Queue of large 1 ...
        """
        try:
#             Create a queue of large 2, for data server and monitor unit.
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
        
    def check_proccess_running(self,listOfProcesses):
        """
        check the process is running.
        """
        print("List of processes: ",listOfProcesses)
        return True
