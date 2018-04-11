#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .load_index_conf import LoadIndexConfiguration as LIC
from elasticsearch import Elasticsearch, ElasticsearchException, ConnectionTimeout, TransportError
from datetime import datetime
import logging
import logging.handlers

logger = logging.getLogger('Monitor & Indexing Unit.Elastic.elastic')


class ESCluster():
    """
    Connection and indexing data in the cluster.
    """
    def conn(self,data):
        """
        Create connection with the cluster.
        """
        try:
#             Load elastic variables.
            conexion = []
            for n in data.elasticHost:
                conexion.append('http://%s:%s@%s:%s'%(data.elasticUser, 
                                                      data.elasticPassword, n, 
                                                      data.elasticPort)
                                                      )
            self.es = Elasticsearch(conexion, username= data.elasticUser, 
                               password= data.elasticPassword, 
                               timeout=10, request_timeout=10, 
                               sniff_timeout=1000,
                               verify_certs=False, retry_on_timeout=True
                               )
            logger.debug("Connected to the cluster.")
        except TransportError as e:
            logger.critical("Error connection with the cluster: %s."%e)
        except ConnectionTimeout as e:
            logger.critical("Error connection with the cluster: %s."%e)
        except Exception as e:
            logger.critical(
                "Undefined error in connection with the cluster: %s."%e)
        return True
    
    def info(self, data):
        """
        Verifies index exists. Check
        """
        es = self.conn(data)
        info = es.info()
        return info
    
    def status(self, index, data):
        """
        Display status of the cluster.
        """
        es = self.conn(data)
#         Hay que reemplazar este comando por el del cluster, este muestra estado de un indice.
        index= es.cat.indices(index=index,h='health,status,index,store.size',pri='false',bytes='k',v=True)
        return True
    
    def list_index(self, data, index):
        """
        List open index in the cluster.
        """
        try:
            if index == '':
                indices = self.es.cat.indices(h='health,status,index,store.size',pri='false',bytes='k',v=True)
            else:
                indices = self.es.cat.indices(index = index, 
                                         h='health,status,index,pri,rep,docs.count,docs.deleted,store.size,pri.store.size',
                                         pri='false',bytes='k',v=True).split()
        except Exception as e:
            logger.error("Error %s listing index."%e)
            indices = e
            pass
        return indices 

         
class ESIndices(ESCluster):
    """
    Make operations over indices on Cluster.
    """
    def is_index_exist(self,indexName):
        """
        Verifies index exists. Check
        """
        try:
            indexExist = self.es.indices.exists(index=indexName)
            return indexExist
        except:  return False 
        

    def indexing_data(self, indexName, indexType, telemetry, 
                      elasticDefinition, elasticFile):
        """
        Inserts the data in the index who defined in configuration 
        file. The format time for index timestamp is ISO 8601.
        """
        try:
            data2index = LIC(elasticFile, telemetry)
            (self.es.index(index = indexName, doc_type = indexType, 
                           timestamp = datetime.utcnow().isoformat(), 
                           body = data2index.msg))
            logger.debug("Data indexing ok.")
        except TypeError as err:
            logger.error(
                "Error formatting data for %s. %s."%(indexName,e)
                )
            pass
        except (ElasticsearchException, ConnectionTimeout, 
                TransportError)as e:
            logger.error(
                "Error inserting data %s in cluster: %s"%(indexName,e)
                )
            pass
        except Exception as e:
            logger.error(
                "Undefined error for index %s,%s"%(indexName,e)
                )
            pass
        return True
        
    def create_index(self, indexName, indexShards, indexReplicas):
        """
        Creates the index defined in the configuration file.  
        Sharding and replicas are defined in setup.json.  
        """
        logger.info("Creates index %s with %s shards and %s replicas."%(
            indexName, indexShards, indexReplicas)
            )
        try:
            self.es.indices.create(index=indexName, 
                              body='{"settings":{"index":{"number_of_shards":%s,"number_of_replicas":%s}}}'%
                              (indexShards, indexReplicas))
            logger.info("Creates the index %s."%(indexName))
        except (ElasticsearchException, ConnectionTimeout, TransportError) as e:
            logger.error(
                "Error when try to creates the index %s.Error %s"%(indexName,e)
                )
            pass
        except Exception as e:
            logger.error(
                "Error when try to creates the index %s.Error %s"%(indexName, e)
                )
            pass
        return True
        
    def format_index_name(self, index_name, time_based):
        """
        Formating index name.
        """
        if bool(time_based) == True:
            index_name = index_name.split('-')
            index_name = "%s-%s"%(index_name[0],datetime.utcnow().strftime(index_name[1]))
        return index_name

    def close_single_index(self, configfile, index):
        """
        Close an index at requirement.
        """
        try:
            self.load_file("%s"%(configfile))
            self.elastic_data()
            es = self.conn()
            estado = es.indices.close(index=index)
            logger.info("Close the index %s."%(index))
        except Exception as e:
            logger.error("Error %s closing index %s."%(e,index))
            estado = e
            pass
        return estado

    def open_index(self, configfile,  index):
        """
        Open an index at requirement.
        """
        try:
            self.load_file("%s"%(configfile))
            self.elastic_data()
            es = self.conn()
            estado = es.indices.open(index=index)
            logger.info("Open the index %s."%(index))
        except Exception as e:
            logger.error("Error %s opening index %s."%(e,index))
            estado = e
            pass
        return estado
  
    def index_status(self, index_config, indice):
        """
        List index status on the cluster.
        """
        try:
            self.load_file("%s"%(index_config))
            self.elastic_data()
            es = self.conn()
            status = es.cat.indices(index=indice,h='health,status,index,store.size',pri='false',bytes='k',v=True)
        except Exception as e:
            logger.error("Error %s listing index."%e)
            status = e
            pass
        return status  
        
    def flush(self,index_config,indice):
        """
        List index status on the cluster.
        The flush API allows to flush one or more indices through an API. The flush process 
        of an index basically frees memory from the index by flushing data to the index 
        storage and clearing the internal transaction log. By default, Elasticsearch uses 
        memory heuristics in order to automatically trigger flush operations as required
         in order to clear memory.
        """
        try:
            self.load_file("%s"%(index_config))
            self.elastic_data()
            es = self.conn()
            status = es.indices.flush(index=indice)
            logger.info("Flushing %s index." % indice)
        except Exception as e:
            logger.error("Error %s flushing index."%e)
            status = e
            pass
        return status

#     def __init__(self):
#         _description__ = "Index operation over ElasticSearch cluster."
        
        
class ESSnapshot(ESCluster):
    """
    Backups operations over the cluster. 
    """
    def create_snapshot(self,index, repository, snapshot_name, body, timeout):
        """Creates the snapshot in the format define in the configuration file."""
        self.load_file(index)
        self.elastic_data()
        es = self.conn()
        logger.debug("Create_snapshot: ",snapshot_name)
        logger.info("Create_snapshot %s."%snapshot_name)
        try:
            resp = es.snapshot.create(
                repository=repository,snapshot=snapshot_name, 
                body=body)
        except Exception as e:
            logger.error("Error making snapshot %s. Code %s."%(body,e))
            resp = e
            pass
        return resp
        
    def create_repository(self,index, repository,body):
        """
        Creates the repository for the snapshot in the format define in the configuration file.
        """
        logger.info("Create_repository %s."%repository)
        self.load_file(index)
        self.elastic_data()
        es = self.conn()
        data = es.snapshot.create_repository(repository,body)
        return data
        
    def is_repository_exist(self,index, repository,timeout):
        """
        Verifies repository exists.
        """
        self.load_file(index)
        self.elastic_data()
        es = self.conn()
        logger.info("Trying to verify repositories.")
        data = es.snapshot.verify_repository([repository])
        logger.info("Repositories exist %s."%data)
        return data

    def get_repositories(self,repository):
        """
        Verifies repository exists.
        """
        self.elastic_data()
        es = self.conn()
        data = es.snapshot.get_repository([repository])
        logger.info("Repositories %s."%data)
        return data

class ESRoutines(ESSnapshot, ESIndices):
    """
    Routines Activities over the Cluster.
    """
    def close_routines(self, index_config,  index):
        """
        Execute index close routine according configuration index file.
        """
#        The ES documentation states that flushing before closing an index is good practice.
        self.flush(index_config, index)
#        Send close index commands
        self.close_single_index(index_config, index)
        return True
        
    def snapshot_routines(self, index,  snapshotName,  repository, timeout):
        """
        Does the routines to create backups of the cluster.
        """
        body = '{"type" : "fs", "settings" : {"location" : "%s", "compress" : "True"} }' % self.snapshotPath
        logger.info("Start snapshot routine process.") 
        if self.is_repository_exist (index, repository,timeout) == False:
            logger.info("Repository %s not exist, try to create." % repository)
            self.create_repository(index, repository,body)
        self.create_snapshot(repository,snapshotName,body,timeout)
        logger.info("Snapshot %s was created sucesfully." % snapshotName)
        return True
        
    def indexing_routine_tasks(self, index, data):
        """
        Do the operations to index data.
        """
        self.load_file("%s"%(index))
        self.elastic_data()
        try:
            es = self.conn()
            index_name = self.format_index_name(self.elastic_index,self.elastic_index_time)
            if es != False:
                if  self.is_index_exist(es,index_name) == False:
                    self.create_index(es,index_name)
                self.indexing_data(es,index_name, self.elastic_type, data)
            else:
                logger.critical("Cannot create and indexing data for %s index."%(index_name))
        except ElasticsearchException as e:
                logger.error("Connection error %s for index %s." %(e, index_name))
                pass
        return True
