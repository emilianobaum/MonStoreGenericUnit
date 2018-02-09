#!/usr/bin/python3
#-*- coding utf-8 -*-
#----------------------------------------------- from operator import itemgetter
from datetime import datetime,timedelta
from sys import exit
from json import load
from time import sleep
from unicodedata import normalize
import pymssql
import logging

__version__ = "v3.5, solo vuelve a consultar cuando la lista de \
                actividades esta vacia. Resuelto problema de salida por\
                 errores de BBDD. Formatea tiempo y nombre de satelite,\
                  y lo carga en la queue refresh para copartir los\
                   datos en el data server."
__copyrigth__ = "GPLv3, Obtaining activities for the unit."
__description__ = "Obtain from database activities for the monitor unit."
__last_revision__= "2017-09-26"

#===============================================================================
# Selector de Pasadas
# ===================
# 
# Realiza las consultas a diferentes tablas de L-HPI-DB-S para obtener las 
# pasadas que se ejecutan unicamente con esta unidad."
#===============================================================================


logger = logging.getLogger("Monitor Unit.Selector Pasadas")

class PassSelector():
    """
    Obtiene las actividades para la unidad definida.
    """
    def __query_plan(self):
        """
        Obtiene las actividades para el lapso indicado en initTime and endTime.
        """
        ip = self.cfg["data bases configuration"]["activities table"]["ip"]
        user = (self.cfg["data bases configuration"]
                ["activities table"]["user"])
        passwd = (self.cfg["data bases configuration"]
                  ["activities table"]["pass"])
        bbdd = (self.cfg["data bases configuration"]
                ["activities table"]["bbdd"])
        table = (self.cfg["data bases configuration"]
                 ["activities table"]["table"])
        
        initTime = (datetime.utcnow()-timedelta(hours=0.5)).strftime(
            "%Y-%m-%dT%H:%M:%S")
        endTime = (datetime.utcnow()+timedelta(hours=48)).strftime(
            "%Y-%m-%dT%H:%M:%S")
        # Create the list with activities ...
        plan = []
        # Ask to BBDD for activities
        try:
            conn = pymssql.connect(host=ip, user=user, password=passwd,
                                     database=bbdd)
            cur = conn.cursor(as_dict=True)
            query = "SELECT [Satelite], [Orbita], [FechaHoraInicial],\
            [FechaHoraFinal],[idMacro], [Estado] FROM [%s].[dbo].[%s] WHERE\
            [FechaHoraInicial] >= '%s' AND [FechaHoraFinal] <= '%s' AND\
            [Estado] = 'A' ORDER BY FechaHoraInicial\
               ASC"%(bbdd,table,initTime,endTime)
            # Make queries
            cur.execute(query)
            for n in cur: plan.append(n)
            # Close connection ...
            conn.close()
        except pymssql.Error as e:
            logger.critical('Error al conectarse a %s (%s).'%(bbdd,e))
            pass
        return plan

    def __query_macro(self, macro):
        """
        Obtiene los oid de las macro.
        """
        ip = self.cfg["data bases configuration"]["macros table"]["ip"]
        user = self.cfg["data bases configuration"]["macros table"]["user"]
        passwd = self.cfg["data bases configuration"]["macros table"]["pass"]
        bbdd = self.cfg["data bases configuration"]["macros table"]["bbdd"]
        table = self.cfg["data bases configuration"]["macros table"]["table"]
        macro_list = []
        try:
            conn = pymssql.connect(host=ip, user=user, password=passwd,
                                     database=bbdd)
            cur = conn.cursor(as_dict=False)
            cur.execute('SELECT NAME, OID FROM [%s].[dbo].[%s] WHERE [OID] = %s'%(bbdd,
                                                                  table, macro))
            for n in cur: macro_list.append(n)
            conn.close()
            return macro_list
        except pymssql.Error as e:
            logger.critical('Error al conectarse a %s (%s).'%(bbdd,e))
            pass
        except TypeError as e:
            logger.critical('Error al conectarse a %s (%s).'%(bbdd,e))
            pass
    
    def __query_conf(self, config):
        """
        Obtengo las configuraciones almacenadas en la BBDD.
        """
        ip = (self.cfg["data bases configuration"]
              ["configuration table"]["ip"])
        user = (self.cfg["data bases configuration"]
                ["configuration table"]["user"])
        passwd = (self.cfg["data bases configuration"]
                  ["configuration table"]["pass"])
        bbdd = (self.cfg["data bases configuration"]
                ["configuration table"]["bbdd"])
        table = (self.cfg["data bases configuration"]
                 ["configuration table"]["table"])

        try:
            conn = pymssql.connect(host=ip, user=user, password=passwd,
                                     database=bbdd)
            cur = conn.cursor()
            (cur.execute("SELECT [descripcion],[antena],[sistemaIngestion],\
            [demodulador] FROM [L-HPI-DB-S ].[dbo].["+table+"] WHERE\
             [estado] = 'OK' and [descripcion] LIKE %s" %config))
            # Create the list with activities
            config_list = []
            for n in cur: config_list.append(n)
            # Close connection to BBDD
            conn.close()
            return config_list
        except (pymssql.Error, ) as e:
            logger.critical('Error al conectarse para obtener las\
             configuraciones %s.'%e)
            pass

    def __filter_unit_data(self, config):
        """
        Filtering the unit type  for loading configuration and retrieving 
        information of BBDD.
        indexTable: Position of identificator for this type of unit in 
        configuration table.
        """
        if (self.cfg['unit description']['type'] 
            == 'antenna'):
            ip = (self.cfg["data bases configuration"]
                  ['antennas table']['ip'])
            user = (self.cfg["data bases configuration"]
                    ['antennas table']['user'])
            passwd = (self.cfg["data bases configuration"]
                      ['antennas table']['pass'])
            db = (self.cfg["data bases configuration"]
                  ['antennas table']['bbdd'])
            table = (self.cfg["data bases configuration"]
                     ['antennas table']['table'])
            indexTable = (self.cfg["data bases configuration"]
                          ['antennas table']['index table position'])
        elif (self.cfg['unit description']['type'] 
              == 'sys_ingest'):
            ip = (self.cfg["data bases configuration"]
                  ['ingestion system table']['ip'])
            user = (self.cfg["data bases configuration"]
                    ['ingestion system table']['user'])
            passwd = (self.cfg["data bases configuration"]
                      ['ingestion system table']['pass'])
            db = (self.cfg["data bases configuration"]
                  ['ingestion system table']['bbdd'])
            table = (self.cfg["data bases configuration"]
                     ['ingestion system table']['table'])
            indexTable = (self.cfg["data bases configuration"]
                          ['ingestion system table']['index table position'])
        elif (self.cfg['unit description']['type'] 
              == 'demodulator'):
            ip = (self.cfg["data bases configuration"]
                  ['demodulator table']['ip'])
            user = (self.cfg["data bases configuration"]
                    ['demodulator table']['user'])
            passwd = (self.cfg["data bases configuration"]
                      ['demodulator table']['pass'])
            db = (self.cfg["data bases configuration"]
                  ['demodulator table']['bbdd'])
            table = (self.cfg["data bases configuration"]
                     ['demodulator table']['table'])
            indexTable = (self.cfg["data bases configuration"]
                          ['demodulator table']['index table position'])
        """
        Get unit data ...
        """
        configUnitId = str(str(config[0]).split(',')[indexTable]).replace(')','')

        unitData = self.__get_unit_data(ip, user, passwd, db, table,configUnitId)

        for n in unitData:
            unitData = n

        return unitData
    
    def __get_unit_data(self, ip, user, passwd, db, table,configUnitId):
        """
        Retrieving units information.
        """
        try:
            conn = pymssql.connect(host=ip, user=user, password=passwd, 
                                    database=db)
            cur = conn.cursor(as_dict=False)
            cur.execute(
                "SELECT [identificador], [nombre] FROM ["+db+"].[dbo].["+table
                +"] WHERE identificador = %s"%configUnitId)
            dataUnit = []
            for n in cur:
                dataUnit.append(n)
            conn.close()
            return dataUnit
        except BaseException as e:
            logger.critical('Error al conectarse a %s (%s).'%(db,e))
            pass

    def __transform_norad2satname(self, satellite):
        """
        Transform the norad identificator number to satelitte name use in ETC.
        For this, consults the satellite table in planning database.
        """
        ip = (self.cfg["data bases configuration"]
              ["satellites table"]["ip"])
        user = (self.cfg["data bases configuration"]
                ["satellites table"]["user"])
        passwd = (self.cfg["data bases configuration"]
                  ["satellites table"]["pass"])
        bbdd = (self.cfg["data bases configuration"]
                ["satellites table"]["bbdd"])
        table = (self.cfg["data bases configuration"]
                 ["satellites table"]["table"])
        try:
            conn = pymssql.connect(host=ip, user=user, password=passwd, 
                                    database=bbdd)
            cur = conn.cursor(as_dict=False)
            cur.execute(
                "SELECT [Descripcion] FROM ["+bbdd+"].[dbo].["+table
                +"] WHERE codigo = '"+satellite+"'")
            data = []
            for n in cur:
                data.append(n)
            conn.close()
            return data[0]
        except BaseException as e:
            print("Error ejecutando la consulta ",e)
            logger.critical('Error al conectarse a %s (%s).'%(bbdd,e))
            pass
       
    def selector_activities(self):
        """
        Selecciono las pasadas que corresponden a las actividades de la unidad.
        """
        #=======================================================================
        # Creates and sorting list f activities. Is not the same self.activities
        #=======================================================================
        logger.info("Reload list of activities.")
        activities = self.__query_plan()
        #=======================================================================
        # Analisis de casos:
        #     *activity['idMacro' vale None -> sale
        #     *activity['idMacro' es menor que el 
        #               primer elemento de macros[0] -> sale
        #     *activity['idMacro' es mayor que el 
        #               ultimo elemento de macros[0] -> sale
        #     *Cualquier otra lo analiza.
        # *Realiza la busqueda segun el id en la load_config de la unidad.
        #=======================================================================

        """
        Comienza iterando sobre las actividades recopiladas en el select
        """
        refresh = False 

        for activity in activities:
            if activity['idMacro'] is None:
                logger.warning("Not valid macro for activity %s."%(activity))
                break
            else:
                """
                Comienza a buscar la macro corrrespondiente a las actividad.
                """
                try:
                    macro = self.__query_macro(activity['idMacro'])
                    macro = ((str(str(macro[0]).split(' - ')[0])).
                            replace("(u'Conf",'').replace("('Conf",''))
                except IndexError as e:
                    logger.error("%s. Macro: %s."%(e, macro))
                    break
                try:
                    config = self.__query_conf(macro)
                    unit = self.__filter_unit_data(config)
                except TypeError as e:
                    (logger.error("Not configuration available for\
                                    this macro %s. %s"%(e, activity)))
                    break
                    pass
                """
                Si la actividad corresponde a la unidad la almacena para
                 ejecutarla.
                """
                if (unit[1].find(self.cfg['unit description']['name']) 
                    != -1):
                    activity['Satelite'] = (
                        self.__transform_norad2satname(
                                                activity['Satelite']))
                    activity['FechaHoraInicial'] = (
                                activity['FechaHoraInicial'].strftime(
                                                "%Y-%m-%dT%H:%M:%S.%f"))
                    activity['FechaHoraFinal'] = (
                                    activity['FechaHoraFinal'].strftime(
                                                "%Y-%m-%dT%H:%M:%S.%f"))
                    self.unitActivities.append(activity)
        logger.info("New list of activities: %s"%(self.unitActivities))
        return True

class Refresh(PassSelector):
    """
    Calcula el tiempo de Refresco
    """
    def __calculate_refresh(self):
        """
        Calcula el timepo restante para el inicio de una actividad. Toma el 
        tiempo antes en que incrementara la frecuencia de consulta a la unidad 
        del archivo de configuracion, al igual que la frecuencia con la que 
        la realizara.
        """ 
        if len(self.unitActivities) == 0:
            refresh = [int(self.cfg['program configuration']['configuration']
                   ['slow refresh']), False]
        else:
            for activity in self.unitActivities:
                if (((datetime.utcnow() - timedelta(self.cfg['program configuration']
                    ['configuration']['start before'])) > activity['FechaHoraInicial']) and 
                    (datetime.utcnow() < activity['FechaHoraFinal'])):
                    refresh = [self.cfg['program configuration']['configuration']
                        ['quick refresh'], self.unitActivities]
                elif (datetime.utcnow() > activity['FechaHoraFinal']):
                    logger.debug("Delete activity %s."%activity)
                    self.unitActivities.pop(self.unitActivities.index(activity))
                    refresh = [self.cfg['program configuration']['configuration']
                        ['slow refresh'], self.unitActivities]
                    logger.debug("Actual activities %s"%(self.unitActivities))
                else:
                    refresh = [self.cfg['program configuration']['configuration']
                        ['slow refresh'], self.unitActivities]
        
        return refresh

    def main_refresh(self, PassSelector, timer):
        """
        Inicia el proceso para calcular la frecuencia de consulta/comando
         a el equipos en el archivo de configuracion.
        """
        self.unitActivities = []
        """
        El refresh se ejecuta de permanentemente verificando que existan
        actividades para la unidad y las almacena en la pila de memoria.
        """
        refresh = [self.cfg['program configuration']['configuration']
                        ['slow refresh'], self.unitActivities]
        while True:
            #~ Cuando no hay actividades en la lista vuelve a buscar 
            #~ a la bbdd
            try:
                if len(self.unitActivities) == 0:
                    """
                    Llama al selector de actividades 
                    """ 
                    self.selector_activities()
                for n in self.unitActivities:
                    logger.debug("Activities in the list: %s."%(n))
                """
                Calculo el tiempo de refresco.
                """
                refresh = self.__calculate_refresh()
                """
                Guarda en la Queue timer el valor de refresco.
                """
            except Exception as e:
                logger.critical("Error %s setting refresh time."%(e))
                pass
            timer.put(refresh)
            timer.put(refresh)
            sleep(refresh[0])

