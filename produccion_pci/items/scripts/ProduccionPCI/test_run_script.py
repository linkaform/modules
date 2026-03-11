# -*- coding: utf-8 -*-

"""
python test_run_script.py '{}' '{"jwt": "jwt ", "account_id": 1868, "docker_image": "linkaform/addons:latest", "name": "test_run_script.py"}'
"""

import sys, simplejson
from produccion_pci_utils import Produccion_PCI

from account_settings import *

class ParticularAction( Produccion_PCI ):
    """docstring for ParticularAction"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def review_os_no_liberadas(self):
        print('... ... Revisando Ordenes de Servicio actualizadas pero no liberadas FTTH - METRO ... ...')
        query_pendientes = {
            'answers.f1054000a030000000000012': 'paco',
            'answers.f1054000a030000000000013': 'en_proceso',
            'answers.5f40131c9bca6a32f518d9a9': {'$exists': False}
        }
        records = self.get_records(self.ORDEN_SERVICIO_FIBRA, query_answers=query_pendientes, select_columns=[
            'folio', 
            'answers.f1054000a010000000000005',
            'updated_at',
            'connection_id'
        ])

        c = 0
        group_conexions = {}
        for r in records:
            folio_lib = f"{r['folio']}{r['answers']['f1054000a010000000000005']}"
            record_lib = self.cr.find_one({
                'form_id': self.FORMA_LIBERACION_FIBRA,
                'deleted_at': {'$exists': False},
                'folio': folio_lib
            })
            if not record_lib:
                c += 1
                print(f"---- {c} :: {r['folio']} NO se creo registro de liberacion")
                conexion = r['connection_id']
                group_conexions.setdefault(conexion, []).append(r['folio'])
        print(' ==== ==== ====')
        print(simplejson.dumps(group_conexions, indent=4))

    def delete_ocs_and_libs(self):
        print('=======================================')
        print('Borrando OCs y Liberaciones')
        print('=======================================')
        records_ocs = self.cr.find({
            'form_id': {'$in': [
                self.FORMA_ORDEN_COMPRA_FIBRA_TELNOR,
                self.FORMA_ORDEN_COMPRA_FIBRA_OCCIDENTE,
                self.FORMA_ORDEN_COMPRA_COBRE_OCCIDENTE,
            ]},
            'deleted_at': {'$exists': False},
            'created_at': {'$gte': self.str_to_date('2026-03-10 00:00:00', format_to_date='%Y-%m-%d %H:%M:%S')}
        }
        ,{
            'folio': 1, 
            'answers.f1962000000000000000fc10.f19620000000000000001fc1': 1, # folio os
            'answers.f1962000000000000000fc10.f19620000000000000001fc2': 1, # telefono os
            'connection_id': 1,
            'form_id': 1
        }
        )

        # print(f"Eliminados: {records_ocs.deleted_count}")
        # return

        c = 0
        folios_libs_totales = []
        for rec_oc in records_ocs:
            c += 1
            conexion = rec_oc.get('connection_id')
            print(f"==== {c} = {rec_oc['folio']} conexion = {conexion}")
            for detail_os in rec_oc['answers'].get('f1962000000000000000fc10', []):
                folios_libs_totales.append( f"{detail_os['f19620000000000000001fc1']}{detail_os['f19620000000000000001fc2']}" )

            # Buscando la OC en la conexion para borrarla
            if conexion:
                colection_connection = CollectionConnection(conexion, settings)
                cr_contratista = colection_connection.get_collections_connection()
                delete_conexion = cr_contratista.delete_one({
                    'form_id': rec_oc['form_id'], 
                    'folio': rec_oc['folio'], 
                    'deleted_at': {'$exists': False}
                }
                # , {'folio': 1, 'connection_id': 1}
                )
                print(f"Eliminado de la conexion = {delete_conexion.deleted_count}")
                

        # print(f"\n folios_libs_totales = {folios_libs_totales}")

        """
        Restauro estatus de liberacion de los folios liberados
        """
        query_libs = {
            'form_id': {'$in': [
                self.FORMA_LIBERACION_FIBRA_TELNOR,
                self.FORMA_LIBERACION_FIBRA_OCCIDENTE,
                self.FORMA_LIBERACION_COBRE_OCCIDENTE,
            ]},
            'deleted_at': {'$exists': False},
            'folio': {'$in': folios_libs_totales}
        }
        resp_update_libs = self.cr.update_many(query_libs, {'$set': {'answers.f2361400a010000000000005': 'liberado'}})

        print("Matched count:", resp_update_libs.matched_count)   # Cuántos documentos coincidieron
        print("Modified count:", resp_update_libs.modified_count) # Cuántos fueron modificados

        """
        Borro las liberaciones que se crearon hoy
        """
        query_libs['created_at'] = {'$gte': self.str_to_date('2026-03-10 00:00:00', format_to_date='%Y-%m-%d %H:%M:%S')}
        folios_os_in_libs = []

        
        # Obtener los folios de OS

        # records_libs = self.cr.find(query_libs, {'folio': 1})
        # for rec_lib in records_libs:
        #     folios_os_in_libs.append( rec_lib['folio'][:8] )
        # print(f"\n folios_os_in_libs ({len(folios_os_in_libs)}) = {folios_os_in_libs}")

        
        # Borrar las liberaciones

        # records_deleted = self.cr.delete_many(query_libs)
        # print(f"Eliminados: {records_deleted.deleted_count}")

if __name__ == '__main__':
    lkf_obj = ParticularAction(settings, sys_argv=sys.argv)
    # lkf_obj.review_os_no_liberadas()

    from pci_get_connection_db import CollectionConnection

    lkf_obj.delete_ocs_and_libs()