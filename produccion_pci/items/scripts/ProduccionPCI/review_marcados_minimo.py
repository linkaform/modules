# -*- coding: utf-8 -*-
import re, sys, simplejson
from datetime import datetime

from produccion_pci_utils import Produccion_PCI

from account_settings import *


class Review_Marcados( Produccion_PCI ):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def review_folios(self):


        print(self.FORMA_ORDEN_COMPRA_FIBRA)
        print(self.FORMA_ORDEN_COMPRA_FIBRA_SURESTE)
        print(self.FORMA_ORDEN_COMPRA_FIBRA_NORTE)
        print(self.FORMA_ORDEN_COMPRA_FIBRA_OCCIDENTE)
        print(self.FORMA_ORDEN_COMPRA_COBRE)
        print(self.FORMA_ORDEN_COMPRA_COBRE_SURESTE)
        print(self.FORMA_ORDEN_COMPRA_COBRE_NORTE)
        print(self.FORMA_ORDEN_COMPRA_COBRE_OCCIDENTE)


        return False



        forms = list( self.dict_equivalences_forms_id.keys() )
        records = self.get_records(
            forms, 
            query_answers={'answers.601c7ae006478d9cbee17e00': 'sí'}, 
            select_columns=['folio', 'answers.5f0e23eaca2ca23aa12f21a9', 'form_id']
        )

        for r in records:
            folio = r['folio']
            fecha_carga_contratista = r['answers']['5f0e23eaca2ca23aa12f21a9']
            form_admin = self.dict_equivalences_forms_id[ r['form_id'] ]
            record_admin = cr_admin.find_one({
                'form_id': form_admin,
                'deleted_at': {'$exists': False},
                'folio': folio
            },{'created_at': 1})

            fecha_created = record_admin.get('created_at')

            fecha_liquidada = fecha_created.strftime("%Y-%m-%d")
            date_fecha_liquidada = datetime.strptime(fecha_liquidada, '%Y-%m-%d')
            
            fecha_actual = datetime.strptime(fecha_carga_contratista.split(' ')[0], '%Y-%m-%d')

            diff_dates = fecha_actual - date_fecha_liquidada
            dias_transcurridos = diff_dates.days

            if not dias_transcurridos > self.dias_para_marcar_desfase:
                print(f"folio = {folio} cargado por contratista = {fecha_carga_contratista} creado en = {fecha_created} Dias transcurridos = {dias_transcurridos}")

if __name__ == '__main__':
    lkf_obj = Review_Marcados(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    # BD Admin
    from pci_get_connection_db import CollectionConnection
    colection_connection = CollectionConnection(1259, settings)
    cr_admin = colection_connection.get_collections_connection()
    

    lkf_obj.review_folios()