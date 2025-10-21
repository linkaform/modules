# coding: utf-8
import sys, simplejson, json
import unicodedata
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos
from datetime import datetime

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
    def clean_text(self, texto):
        """
        Limpia texto: minúsculas, espacios y puntos por guiones bajos, elimina acentos
        """
        if not isinstance(texto, str):
            return ""
        
        texto = texto.lower()                # Minúsculas
        texto = texto.replace(" ", "_")      # Espacios → guiones bajos
        texto = texto.replace(".", "_")      # Puntos → guiones bajos
        
        # Eliminar acentos
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
        
        return texto

    def get_user_catalogs(self):
        soter_catalogs = [
            self.LISTA_INCIDENCIAS_CAT_ID,
            self.SUB_CATEGORIAS_INCIDENCIAS_ID,
            self.CATEGORIAS_INCIDENCIAS_ID,
            self.AREAS_DE_LAS_UBICACIONES_CAT_ID,
            self.UBICACIONES_CAT_ID,
            self.CONFIGURACION_RECORRIDOS_ID,
            self.USUARIOS_ID,
            self.CONF_AREA_EMPLEADOS_CAT_ID,
            self.TIPO_DE_EQUIPO_ID,
            self.LISTA_FALLAS_CAT_ID,
            self.CONF_AREA_EMPLEADOS_AP_CAT_ID,
            self.VISITA_AUTORIZADA_CAT_ID,
            self.ESTADO_ID,
            self.PROVEEDORES_CAT_ID,
            self.LOCKERS_CAT_ID,
            self.TIPO_ARTICULOS_PERDIDOS_CAT_ID,
        ]
        dbs = {}
        try:
            fields_invertido = {v: k for k, v in self.f.items()}
            for catalog_id in soter_catalogs:
                item = {}
                version = "00.00"
                info_catalog = self.lkf_api.get_catalog_id_fields(catalog_id)
                catalog_name = self.clean_text(info_catalog.get('catalog', {}).get('name', ''))
                catalog_fields = info_catalog.get('catalog', {}).get('fields', [])
                catalog_updated_at = info_catalog.get('catalog', {}).get('updated_at', '')
                
                field_items = {}
                for field in catalog_fields:
                    if not field.get('field_type') in ['catalog']:
                        field_items.update({
                            field.get('field_id'): fields_invertido.get(field.get('field_id'), self.clean_text(field.get('label', '')))
                        })
                
                if catalog_updated_at:
                    date_part = catalog_updated_at[:10]
                    dt = datetime.strptime(date_part, '%Y-%m-%d')
                    version = f"{dt.year % 100:02d}.{dt.month:02d}"
                
                item = {
                    'db_name': f'catalog_records_{info_catalog.get("catalog", {}).get("catalog_id", 0)}',
                    'field_name': field_items,
                    'version': version,
                    'host': '',
                    'filter': ''
                }
                dbs[catalog_name] = item
        except Exception as e:
            return {'status_code': 400, 'msg': 'error', 'data': str(e)}
        return {'status_code': 200, 'msg': 'success', 'data': dbs}

    def sync_incidence_to_lkf(self, id, rev):
        #TODO: Dinamizar id del usuario para su db
        self.cr_db = self.lkf_api.couch.set_db('incidencias_10')

        status = {}
        record = self.get_couch_record(id, rev)
        record = dict(record)
        record_id = record.pop('_id', None)
        
        #! Evaluar otra forma de limpiar los campos o si seran necesarios posteriormente
        record.pop('_rev', None)
        record.pop('_revs_info', None)
        record.pop('fotos', None)
        record.pop('status', None)
        record.pop('type', None)
        
        if isinstance(record, dict) and 'status_code' in record:
            return record
        elif isinstance(record, dict) and 'folio' in record:
            folio = record.pop('folio', None)
            response = self.update_incidence(record, folio)
        else:
            response = acceso_obj.create_incidence(record)

        record = self.cr_db.get(record_id)
        if response.get('status_code') in [200, 201, 202]:
            folio = response.get('json', {}).get('folio', '')
            record['status'] = 'synced'
            record['folio'] = folio
            self.cr_db.save(record)
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record synced successfully', 'data': {}}
        else:
            record['status'] = 'error'
            self.cr_db.save(record)
            status = {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        return status

    def get_couch_record(self, _id, _rev=None):
        if not _id:
            return {'status_code': 400, 'type': 'error', 'msg': 'ID is required', 'data': {}}
        
        record = self.cr_db.get(_id, revs_info=True)
        if not record:
            return {'status_code': 404, 'type': 'error', 'msg': 'Record not found', 'data': {}}

        current_rev = record.rev
        all_revs = [r['rev'] for r in record['_revs_info'] if r['status'] == 'available']

        if _rev == current_rev:
            print('✅ Revisión actual encontrada')
            record['status'] = 'recived'
            self.cr_db.save(record)
            return record
        elif _rev in all_revs:
            print('⚠️ Revisión vieja')
            return {'status_code': 461, 'type': 'error', 'msg': 'Old revision found', 'data': {}}
        else:
            print('🕓 Revisión aún no propagada')
            return {'status_code': 462, 'type': 'error', 'msg': 'Revision not yet propagated', 'data': {}}

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data = acceso_obj.data.get('data', {})
    option = data.get("option", 'get_user_catalogs')
    _id = data.get("_id", None)
    _rev = data.get("_rev", None)

    response = {}
    if option == 'get_user_catalogs':
        response = acceso_obj.get_user_catalogs()
    elif option == 'sync_incidence_to_lkf':
        #! Hay que revisar que hacer con el record_type si solo sera una funcion para todos los apartados de Accesos
        #! O si seran varias funciones cada una con su tipo
        response = acceso_obj.sync_incidence_to_lkf(id=_id, rev=_rev)
    elif option == 'test':
        breakpoint()

    sys.stdout.write(simplejson.dumps(response, indent=3))