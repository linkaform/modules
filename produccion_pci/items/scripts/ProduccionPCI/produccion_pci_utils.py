# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.produccion_pci.app import Produccion_PCI


class Produccion_PCI(Produccion_PCI):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        
        self.CATALOGO_CONTRATISTAS = self.lkm.catalog_id('catalogo_de_contratistas_10',{} )
        self.CATALOGO_CONTRATISTAS_ID = self.CATALOGO_CONTRATISTAS.get('id')
        self.CATALOGO_CONTRATISTAS_OBJ_ID = self.CATALOGO_CONTRATISTAS.get('obj_id')


        self.FORM_ID_CARGA_PROD = self.lkm.form_id('carga_de_produccin_diaria_por_contratistaiasa','id')
        self.FORM_ID_EXPEDIENTES_DE_TECNICOS = self.lkm.form_id('expedientes_de_tcnicosiasa','id')
        
        self.ORDEN_SERVICIO_FIBRA = self.lkm.form_id('orden_de_servicio_metro_ftthiasa','id')
        self.ORDEN_SERVICIO_COBRE = self.lkm.form_id('orden_de_servicio_metro_cobreiasa','id')
        self.ORDEN_SERVICIO_FIBRA_OCCIDENTE = self.lkm.form_id('orden_de_servicio_occidente_ftthiasa','id')
        self.ORDEN_SERVICIO_COBRE_OCCIDENTE = self.lkm.form_id('orden_de_servicio_occidente_cobreiasa','id')
        self.ORDEN_SERVICIO_FIBRA_NORTE = self.lkm.form_id('orden_de_servicio_norte_ftthiasa','id')
        self.ORDEN_SERVICIO_COBRE_NORTE = self.lkm.form_id('orden_de_servicio_norte_cobreiasa','id')
        self.ORDEN_SERVICIO_FIBRA_SURESTE = self.lkm.form_id('orden_de_servicio_sur_ftthiasa','id')
        self.ORDEN_SERVICIO_COBRE_SURESTE = self.lkm.form_id('orden_de_servicio_sur_cobreiasa','id')

        # Equivalencias de las formas de orden de servicio en las cuentas de SC y en Admin
        self.dict_equivalences_forms_id = { 
            self.ORDEN_SERVICIO_FIBRA : 11044,
            self.ORDEN_SERVICIO_COBRE : 10540,
            self.ORDEN_SERVICIO_FIBRA_OCCIDENTE : 21953,
            self.ORDEN_SERVICIO_COBRE_OCCIDENTE : 25929,
            self.ORDEN_SERVICIO_FIBRA_NORTE : 21954,
            self.ORDEN_SERVICIO_COBRE_NORTE : 25928,
            self.ORDEN_SERVICIO_FIBRA_SURESTE : 16343,
            self.ORDEN_SERVICIO_COBRE_SURESTE : 25927
        }

        self.dict_ids_os_pdf = {
            self.ORDEN_SERVICIO_FIBRA: '5a8aefa7b43fdd100602f7be',
            self.ORDEN_SERVICIO_FIBRA_SURESTE: '5ad14051f851c220dd0eb772',
            self.ORDEN_SERVICIO_FIBRA_NORTE: '5ad14687f851c23d8a4d95c9',
            self.ORDEN_SERVICIO_FIBRA_OCCIDENTE: '5ad4b4a9b43fdd7af0f65899',
            self.ORDEN_SERVICIO_COBRE: '5ad13efef851c23d8a4d95af',
            self.ORDEN_SERVICIO_COBRE_SURESTE: '5ad13e8cf851c220dd0eb769',
            self.ORDEN_SERVICIO_COBRE_NORTE: '5ad13f49f851c2510b0d210a',
            self.ORDEN_SERVICIO_COBRE_OCCIDENTE: '5ad13f95f851c2467770da9e'
        }