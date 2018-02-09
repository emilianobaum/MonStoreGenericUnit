#!/usr/bin/python3
#-*- coding utf-8 -*-
from __future__ import absolute_import

from .elastic import ESCluster, ESIndices


    
class Elastic(object):
    
    def __init__(self):
        __author__ = "Emiliano A. Baum"
        __license__ = "GPLv3"
        __description__ = "Indexing data module for ElasticSearch. Process "
                
        self.ESI = ESIndices()
        self.ESC = ESCluster()
        #             create index
                    
        #         self.indexing_data(index, index_type, data)
        
    