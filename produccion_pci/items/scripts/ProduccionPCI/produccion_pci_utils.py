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

        # Aqui los ids de las formas de liberacion
        self.FORMA_LIBERACION_FIBRA = self.lkm.form_id('liberacin_de_pagos_socio', 'id')
        self.FORMA_LIBERACION_FIBRA_SURESTE = self.lkm.form_id('liberacin_de_pagos_sur_socio', 'id')
        self.FORMA_LIBERACION_FIBRA_NORTE = self.lkm.form_id('liberacin_de_pagos_norte_socio', 'id')
        self.FORMA_LIBERACION_FIBRA_OCCIDENTE = self.lkm.form_id('liberacin_de_pagos_occidente_socio', 'id')
        self.FORMA_LIBERACION_COBRE = self.lkm.form_id('liberacin_de_pagos_cobre_socio', 'id')
        self.FORMA_LIBERACION_COBRE_SURESTE = self.lkm.form_id('liberacin_de_pagos_sur_cobre_socio', 'id')
        self.FORMA_LIBERACION_COBRE_NORTE = self.lkm.form_id('liberacin_de_pagos_norte_cobre_socio', 'id')
        self.FORMA_LIBERACION_COBRE_OCCIDENTE = self.lkm.form_id('liberacin_de_pagos_occidente_cobre_socio', 'id')

        # Aqui los ids de las formas de orden de compra
        self.FORMA_ORDEN_COMPRA_FIBRA = self.lkm.form_id('orden_compra_contratista_ftth_metro_socio', 'id')
        self.FORMA_ORDEN_COMPRA_FIBRA_SURESTE = self.lkm.form_id('orden_compra_contratista_ftth_sur_socio', 'id')
        self.FORMA_ORDEN_COMPRA_FIBRA_NORTE = self.lkm.form_id('orden_compra_contratista_ftth_norte_socio', 'id')
        self.FORMA_ORDEN_COMPRA_FIBRA_OCCIDENTE = self.lkm.form_id('orden_compra_contratista_ftth_occidente_socio', 'id')
        self.FORMA_ORDEN_COMPRA_COBRE = self.lkm.form_id('orden_compra_contratista_cobre_metro_socio', 'id')
        self.FORMA_ORDEN_COMPRA_COBRE_SURESTE = self.lkm.form_id('orden_compra_contratista_cobre_sur_socio', 'id')
        self.FORMA_ORDEN_COMPRA_COBRE_NORTE = self.lkm.form_id('orden_compra_contratista_cobre_norte_socio', 'id')
        self.FORMA_ORDEN_COMPRA_COBRE_OCCIDENTE = self.lkm.form_id('orden_compra_contratista_cobre_occidente_socio', 'id')
        self.FORM_ID_PRECIOS_FTTH = self.lkm.form_id('precios_fibra_socio', 'id')
        self.FORM_ID_PRECIOS_COBRE = self.lkm.form_id('precios_cobre_socio', 'id')

        # Formas complemento para la validacion de facturas
        self.FORMA_COMPLEMENTOS_PAGO = self.lkm.form_id('contratistas_para_complementos_de_pago', 'id')
        self.FORMA_PAGOS_SAP = self.lkm.form_id('pagos_sap_complementos_de_pagos', 'id')
        self.FORMA_CONFIGS_VALIDACIONES = self.lkm.form_id('configuracin_de_validaciones', 'id')

        self.ID_CONTRATISTA_TIPO_MAQTEL = 2823
        self.id_tecnicos_directos = [2071, 2072, 2073]

        self.MONTO_MAXIMO_POR_OC = 200000 # Monto maximo por OCs en Cobre

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

        self.all_divisiones = [
            {'tecnologia':'fibra','division':'metro'}, {'tecnologia':'fibra','division':'sur'}, 
            {'tecnologia':'fibra','division':'norte'}, {'tecnologia':'fibra','division':'occidente'},
            {'tecnologia':'cobre','division':'metro'}, {'tecnologia':'cobre','division':'sur'}, 
            {'tecnologia':'cobre','division':'norte'}, {'tecnologia':'cobre','division':'occidente'}
        ]

        self.f.update({
            'xls_email_contratistas': '60105b997b3c64bb35043c3c'
        })

    def get_only_connections(self):
        """ Se lee excel de emails con las conexiones a las que se va a liberar y crear OC """
        
        # Se va a leer la lista de contratistas que el usuario requiera desde un excel
        if not self.answers.get( self.f['xls_email_contratistas'] ):
            return {}

        # Se leen los emails del excel
        file_url = self.answers[ self.f['xls_email_contratistas'] ]['file_url']
        header_contratistas, records_contratistas = self.lkf_api.read_file( file_url )

        # Se obtienen las conexiones de la cuenta padre
        all_connections = self.lkf_api.get_all_connections()

        dict_emails_connections = { infCon['email']: infCon['id'] for infCon in all_connections if infCon.get('email') and infCon.get('id') }
        dict_info_connection = { infCon['id']: infCon for infCon in all_connections if infCon.has_key('id') }

        emails_not_exists, emails_connections = [], []
        for email_contratista in records_contratistas:
            email_record = email_contratista[0]
            if not dict_emails_connections.get( email_record ):
                emails_not_exists.append( email_record )
                continue
            emails_connections.append( dict_emails_connections[ email_record ] )
        
        if emails_not_exists:
            return {'error': emails_not_exists}

        return {'result': emails_connections, 'info_connections': dict_info_connection}