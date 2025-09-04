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

        self.account_id_base = 1868
        self.form_id_carga = 132847
        self.map_forms_admin_account = {
            11044: self.ORDEN_SERVICIO_FIBRA,
            10540: self.ORDEN_SERVICIO_COBRE,
            21953: self.ORDEN_SERVICIO_FIBRA_OCCIDENTE,
            25929: self.ORDEN_SERVICIO_COBRE_OCCIDENTE,
            21954: self.ORDEN_SERVICIO_FIBRA_NORTE,
            25928: self.ORDEN_SERVICIO_COBRE_NORTE,
            16343: self.ORDEN_SERVICIO_FIBRA_SURESTE,
            25927: self.ORDEN_SERVICIO_COBRE_SURESTE
        }

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

    """
    # ==========================================================
    Revisar folios en las cargas de produccion donde no se creo la copia pero se quedaron reasignados
    """
    def process_group_folios(self, group_tecs_fols, folio_carga):
        for tecnologia, folios in group_tecs_fols.items():
            forms_admin = [10540,25929,25928,25927] if tecnologia == 'cobre' else [11044,21954,16343,21953]
            records_admin = cr_admin.find({
                'form_id': {'$in': forms_admin},
                'deleted_at': {'$exists': False},
                'folio': {'$in': folios},
                'connection_id': self.account_id_base,
                'properties.device_properties.folio carga': folio_carga
            }, {'form_id': 1, 'folio': 1})

            # Se recorren los registros de Admin para ver si estan sus copias
            for rec_admin in records_admin:
                form_id = self.map_forms_admin_account[ rec_admin['form_id'] ]
                exists_in_account = cr_account.find_one({
                    'form_id': form_id,
                    'deleted_at': {'$exists': False},
                    'folio': rec_admin['folio']
                }, {'folio':1})
                if not exists_in_account:
                    print(f"    ... ... Registro de Admin {rec_admin['folio']} forma = {rec_admin['form_id']}")
                    # stop

    def process_archivo_carga(self, file_url, folio_carga):
        header, records = p_utils.read_file(file_url)
        header_dict = p_utils.make_header_dict(header)
        pos_folio = p_utils.get_record_folio(header_dict)
        pos_tecnologia = header_dict['tecnologia_orden']

        if pos_folio is None:
            print('[ERROR] No hay columna folio... posible degradados')
            return


        # Agrupamos los folios por tecnologia
        group_folios = {}
        for record in records:
            value_pos_folio = record[pos_folio]
            if not value_pos_folio:
                continue
            folio = str( value_pos_folio ).strip()
            if not folio:
                continue
            group_folios.setdefault( str(record[pos_tecnologia]).lower().strip(), [] ).append( folio )
        
        # Se procesan los folios encontrados para identificar si existe su registro copia y correctamente asignado
        self.process_group_folios(group_folios, folio_carga)
    
    def revisar_asignados_sin_copia(self):
        select_columns = self.get_selected_columns(['answers.f1074100a010000000000001', 'folio', 'created_at', 'connection_email'])
        records_carga_prod = cr_account.find({
            'form_id': self.form_id_carga,
            'deleted_at': {'$exists': False},
            'answers.f1074100a010000000000003': {'$exists': True},
            'created_at': {'$gte': datetime.strptime("2025-08-31", "%Y-%m-%d"), '$lte': datetime.strptime("2025-09-05", "%Y-%m-%d")}
        }, select_columns)

        for rec_carga_prod in records_carga_prod:
            file_carga = rec_carga_prod['answers'].get('f1074100a010000000000001', {}).get('file_url')
            if not file_carga:
                continue
            print(f"=== === === PROCESANDO CARGA {rec_carga_prod['folio']} ({rec_carga_prod['_id']}) creado el {rec_carga_prod['created_at']} por {rec_carga_prod['connection_email']}")
            self.process_archivo_carga(file_carga, rec_carga_prod['folio'])
            # stop

    # ==========================================================

if __name__ == '__main__':
    lkf_obj = ReviewReasignados(settings, sys_argv=sys.argv)

    from pci_get_connection_db import CollectionConnection
    colection_connection = CollectionConnection(1259, settings)
    cr_admin = colection_connection.get_collections_connection()
    # cr_version = colection_connection.get_collection_version()

    cr_account = lkf_obj.cr

    from pci_base_utils import PCI_Utils
    p_utils = PCI_Utils(cr=cr_account, cr_admin=cr_admin, lkf_api=lkf_obj.lkf_api, net=lkf_obj.net, settings=settings, lkf_obj=lkf_obj)

    # lkf_obj.review_folios_reasignados()
    lkf_obj.revisar_asignados_sin_copia()