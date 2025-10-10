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

        schema_valiables_criticas ={
                    'BASE_DE_DATOS_ORACLE':'68ae9831a113d169e05af40d',
                    'FECHA':'683753204328adb3fa0bfd2b',
                    'LECBRIXSOLUT2': 'ccaaa5b15ad84734fae92bae',
                    '%BRIXJARABE': 'ccaaa548cf22411a910aabf2',
                    'SODAAPLICADCALD': 'ccaaa864c62694640f0c69c7',
                    'FOSFATOSKITCAL5': 'ccaaa864c62694640f0c69ca',
                    'FOSFATOSKITCAL6': 'ccaaa864c62694640f0c69cd',
                    'CONDAGPURGCALD7': 'ccaaa864c62694640f0c69ce',
                    'FOSFATOSKITCAL7': 'ccaaa864c62694640f0c69d0',
                    'CONDTANQEB3': 'ccaaa864c62694640f0c69d1',
                    '%BRIXJARABE': 'ccaaa548cf22411a910aabf2',
                    '%BRIXLICORFUN': 'ccaaa548cf22411a910aabf5',
                    '%BRIXMIELA': 'ccaaa548cf22411a910aabf3',
                    '%BRIXMIELENVPAC': 'ccaaa548cf22411a910aabf4',
                    '%HUMBZOT2': 'ccaaa864c62694640f0c69d2',
                    '%HUMCACHAMEZ': 'ccaaa548cf22411a910aabfa',
                    '%PUREZAJARABE': 'ccaaa548cf22411a910aabf6',
                    '%PUREZAJUFILMEZ': 'ccaaa5b15ad84734fae92bb4',
                    '%PUREZAMAGMAB': 'ccaaa548cf22411a910aabf7',
                    '%PUREZAMIELA': 'ccaaa548cf22411a910aabf8', 
                    '%PZAMIELENVIPAC': 'ccaaa548cf22411a910aabf9',
                    '%SACBZOT2': 'ccaaa5f5c7289512fa3f646a',
                    '02.fit_08_008.daca_pv': 'ccaaa864c62694640f0c69d4',
                    '04.fit_08_001.daca_pv': 'ccaaa864c62694640f0c69d5',
                    '08.fit_08_010.daca_pv': 'ccaaa864c62694640f0c69d3',
                    'calent.ait2260.daca_pv': 'ccaaa5b15ad84734fae92bac',
                    'calent.ait2280.daca_pv': 'ccaaa5b15ad84734fae92bad',
                    'COLORJARABE': 'ccaaa548cf22411a910aabfd',
                    'COLORJUGOCLARO': 'ccaaa548cf22411a910aabfc',
                    'COLORJUGODILT2': 'ccaaa548cf22411a910aabfb',
                    'COLORLICDECOL': 'ccaaa548cf22411a910aac01',
                    'COLORLICLARIF': 'ccaaa548cf22411a910aac00',
                    'COLORLICORFUND': 'ccaaa548cf22411a910aabff',
                    'COLORMAGMAB': 'ccaaa548cf22411a910aabfe',
                    'CONDAGCONDCALDE': 'ccaaa864c62694640f0c69c6',
                    'CONDAGPURGCALD5': 'ccaaa864c62694640f0c69c8',
                    'CONDAGPURGCALD6': 'ccaaa864c62694640f0c69cb',
                    'CONDAGPURGCALD7': 'ccaaa864c62694640f0c69ce',
                    'CONDTANQEB3': 'ccaaa864c62694640f0c69d1',
                    'FOSFATOSJDILT2': 'ccaaaab67a5f67619b7097cb',
                    'FOSFATOSKITCAL5': 'ccaaa864c62694640f0c69ca',
                    'FOSFATOSKITCAL6': 'ccaaa864c62694640f0c69cd',
                    'FOSFATOSKITCAL7': 'ccaaa864c62694640f0c69d0',
                    'LECBRIXJCLARO': 'ccaaa5b15ad84734fae92baf',
                    'LECBRIXJUFILPRE': 'ccaaa5b15ad84734fae92bb1',
                    'LECBRIXSOLUT2': 'ccaaa5b15ad84734fae92bae',
                    'LECPOLCACHAZAMZ': 'ccaaa5b15ad84734fae92bb6',
                    'LECPOLCAFILPREN': 'ccaaa5b15ad84734fae92bb5',
                    'PHAGPURGACAL5': 'ccaaa864c62694640f0c69c9',
                    'PHAGPURGACAL6': 'ccaaa864c62694640f0c69cc',
                    'PHAGPURGACAL7': 'ccaaa864c62694640f0c69cf',
                    'PUREZAJUGCLARO': 'ccaaa5b15ad84734fae92bb3',
                    'PUREZAJUGDILT2': 'ccaaabf038a9fab9950dba60',
                    'TURB420LCLARIFI': 'ccaaa548cf22411a910aac04',
                    'TURB420LFUNDIDO': 'ccaaa548cf22411a910aac03',
                    'TURBIEDADJCLARO': 'ccaaa5b15ad84734fae92bb7',
                    'TURBIEDADJDILT2': 'ccaaa548cf22411a910aac02',
                    'TIPO_REGISTRO': '68ae9831a113d169e05af40e',
                    'TURBILIDECOLORA': 'ccaaa548cf22411a910aac05',
                    'LECBRIXJCLARO': 'ccaaa5b15ad84734fae92baf',
                    'LECBRIXJUFILPRE': 'ccaaa5b15ad84734fae92bb1',
                    'LECBRIXSOLUT2': 'ccaaa5b15ad84734fae92bae',
                    'LECPOLCACHAZAMZ': 'ccaaa5b15ad84734fae92bb6',
                    'LECPOLCAFILPREN': 'ccaaa5b15ad84734fae92bb5',
                    'PHAGPURGACAL5': 'ccaaa864c62694640f0c69c9',
                    'PHAGPURGACAL6': 'ccaaa864c62694640f0c69cc',
                    'PHAGPURGACAL7': 'ccaaa864c62694640f0c69cf',
                    'PHLICORCLARIF': 'ccaaa548cf22411a910aabf1',
                    'PUREZAJUGCLARO': 'ccaaa5b15ad84734fae92bb3',
                    'TURB420LCLARIFI': 'ccaaa548cf22411a910aac04',
                    'TURB420LFUNDIDO': 'ccaaa548cf22411a910aac03',
                    'TURBIEDADJCLARO': 'ccaaa5b15ad84734fae92bb7',
                    'TURBIEDADJDILT2': 'ccaaa548cf22411a910aac02',
                    'TURBILIDECOLORA': 'ccaaa548cf22411a910aac05',
                    'WT_TCH.TCH_PV': 'ccaaa5f5c7289512fa3f646c',
                    }

        """Aqui configuramos las vistas que vamos a mandar llamar, declaramos su schema, form_id y catalog_id de ser necesario"""
        self.views = {
            'PRODUCCION.VW_LinkAForm_Hora':{
                'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
                'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM,
                'schema':schema_valiables_criticas
            },
            # 'PRODUCCION.VW_LinkAForm_Dia':{
            #     'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
            #     'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM,
            #     'schema':schema_valiables_criticas
            #     },
            'REPORTES.vw_linkaform_fab':{
                'catalog_id': self.VARIABLES_CRITICAS_PRODUCCION,
                'form_id': self.VARIABLES_CRITICAS_PRODUCCION_FORM,
                'schema':schema_valiables_criticas
                },
            }

    def sync_db_catalog(self, db_name, query={}):
        header, data = self.query_view(db_name, query=query, date_format=True)
        return header, data