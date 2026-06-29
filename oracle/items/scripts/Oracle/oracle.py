# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base

from linkaform_api import base
from lkf_addons.addons.employee.app import Employee
from lkf_addons.addons.activo_fijo.app import Vehiculo

from lkf_addons.addons.oracle.app import Oracle


class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings
        self.etl_values = {}
        self.f.update({
            'base_de_datos_oracle':'68ae9831a113d169e05af40d',
            'fecha':'683753204328adb3fa0bfd2b',
            'tipo_registro':'68ae9831a113d169e05af40e',
            'variable_criticas':'68375178ec91ea1bc3e92b65'
            })

        self.VARIABLES_CRITICAS_PRODUCCION = 134148
        self.VARIABLES_CRITICAS_PRODUCCION_FORM = 139471

        self.db_id_fields = {
            'CONTACTOID':'FEC_MODIF',
            'PAISID':'FEC_MODIF',
            'PROVINCIAID':'FEC_MODIF',
            'DEPARTAMENTOID':'FEC_MODIF',
            'CARGOID':'FEC_MODIF',
            'MARCAID':'FEC_MODIF',
            'MODELOID':'FEC_MODIF_MODELO',
            'VEHICULO_TALID':'FEC_MODIF',
            }

        
        """Aqui configuramos las vistas que vamos a mandar llamar, declaramos su schema, form_id y catalog_id de ser necesario"""

    def sync_db_catalog(self, db_name, query={}):
        # header= ['FECHA_HORA', 'ANSWERS']
        # import datetime

        #data= [
            # {
            # 'FECHA_HORA': datetime.datetime(2025, 11, 4, 2, 0), 
            # 'ANSWERS': '{"CONDAGPURGCALD6":610,"PHAGPURGACAL7":8.1,"PHAGPURGACAL6":9.1,"CONDAGPURGCALD7":756}'}, 
            # {'FECHA_HORA': datetime.datetime(2025, 11, 4, 4, 0), 
            # 'ANSWERS': '{"%BRIXLICORFUN":62.67,"TURBILIDECOLORA":46,"TURBILICLARIFIC":174,"TURBILICFUNDI":471,"SODAAPLICADCALD":1,"PHAGPURGACAL7":8.3,"PHAGPURGACAL6":9.1,"CONDTANQEB3":1590,"CONDAGPURGCALD7":770,"CONDAGPURGCALD6":640,"COLORLICORFUND":701,"COLORLICLARIF":558,"COLORLICDECOL":122}'}, 
            # {'FECHA_HORA': datetime.datetime(2025, 11, 4, 5, 0), 'ANSWERS': '{"%PUREZAMAGMAB":89.95}'}]
        header, data = self.query_view(db_name, query=query, date_format=True)
        return header, data