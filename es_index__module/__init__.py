#!/usr/bin/python3
#-*- coding utf-8 -*-
from __future__ import absolute_import

from .elastic import ESCluster, ESIndices



__author__ = "Emiliano A. Baum"
__license__ = "GPLv3"
__description__ = "Indexing data module for ElasticSearch. Process "
        
ESI = ESIndices()
ESC = ESCluster()
