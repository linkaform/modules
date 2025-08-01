# -*- coding: utf-8 -*-
import sys, simplejson
from bson import ObjectId
from datetime import datetime
from produccion_pci_utils import Produccion_PCI
from account_settings import *

class ReviewReasignados( Produccion_PCI ):
    """docstring for ReviewReasignados"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def review_record(self, record_os, field_cliente='58e6d4cff851c244a78f35ca'):
        # print(f"... {record_os['folio']} {record_os['form_id']}")
        form_admin = self.dict_equivalences_forms_id[ record_os['form_id'] ]
        record_admin = cr_admin.find_one({
            'form_id': form_admin, 
            'folio': record_os['folio'], 
            'connection_id': 1868, 
            'other_versions': {'$exists': True}
        },{'other_versions': 1, f'answers.{field_cliente}': 1, 'form_id': 1})
        if not record_admin:
            print('[ERROR] No se encontro el registro en Admin')
            return
        
        # Se revisa si alguna de las versiones el cliente cambio de valor
        ids_version = [ ObjectId(v['uri'].split('/')[-2]) for v in record_admin.get('other_versions', []) ]
        #print('... ... versiones =',ids_version)
        actual_cliente = record_admin.get('answers', {}).get(field_cliente)
        record_version = cr_version.find_one({
            'form_id': form_admin,
            '_id': {'$in': ids_version},
            'connection_id': 1868,
            '$and': [
                {f'answers.{field_cliente}': {'$exists': True}},
                {f'answers.{field_cliente}': {'$nin': [actual_cliente]}}
            ]
        },{f'answers.{field_cliente}': 1})
        if not record_version:
            return
        print(f"[ERROR] form= {record_admin['form_id']} recordId= {record_admin['_id']} folio= {record_os['folio']} cambio de cliente de: {record_version['answers'].get(field_cliente)} a: {actual_cliente}")

    def review_folios_reasignados(self):
        # print('formas os copia = ',self.dict_equivalences_forms_id.keys())
        # forms_cobre = [132840, 132856, 132855, 132854] # 437
        #form_fibra_p1 = [132853] # 4,221
        form_fibra_p2 = [132846] # 11,137
        
        # Se consultan todos los registros de OS que han sido creados en las formas copias
        records_os_copia = lkf_obj.get_records(form_fibra_p2, 
            query_answers={
                # 'created_at': {'$gte': datetime.strptime("2025-05-27", "%Y-%m-%d"), '$lte': datetime.strptime("2025-06-30", "%Y-%m-%d")}
                'created_at': {'$gte': datetime.strptime("2025-06-30", "%Y-%m-%d")}
            }, 
            select_columns=['form_id', 'folio']
        )
        
        for rec_copia in records_os_copia:
            self.review_record(rec_copia, 'f1054000a0100000000000c5')

if __name__ == '__main__':
    lkf_obj = ReviewReasignados(settings, sys_argv=sys.argv)

    from pci_get_connection_db import CollectionConnection
    colection_connection = CollectionConnection(1259, settings)
    cr_admin = colection_connection.get_collections_connection()
    cr_version = colection_connection.get_collection_version()

    lkf_obj.review_folios_reasignados()