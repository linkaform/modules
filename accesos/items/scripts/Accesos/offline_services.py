# coding: utf-8
import os
import sys, simplejson, json
import time
import tempfile
import unicodedata
from bson.objectid import ObjectId
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
        
        centralized_fields = {
            'categoria':'686807d46e41614d708f6fc9',
            'icono_categoria':'686807d46e41614d708f6fca',
            'sub_categoria': '686807a7ee7705c5c8eb181a',
            'icono_sub_categoria': '686807a7ee7705c5c8eb181b',
            'incidencia': '663973809fa65cafa759eb97',
            'area': '663e5d44f5b8a7ce8211ed0f',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'address': '663a7e0fe48382c5b1230902',
            'address_complement': '663a7f79e48382c5b123090a',
            'address_email': '663a7ee1e48382c5b1230907',
            'tipo_de_area': '663e5e68f5b8a7ce8211ed18',
            'num_de_habitacion': '680977786d022b9bff6e3645',
            'piso': '680977786d022b9bff6e3646',
            'address_name': '663a7e0fe48382c5b1230901',
            'address_phone': '663a7ee1e48382c5b1230906',
            'address_geolocation': '663e5c8cf5b8a7ce8211ed0c',
            'area_city': '6654187fc85ce22aaf8bb070',
            'area_status': '663e5e4bf5b8a7ce8211ed15',
            'area_state': '663e5e4bf5b8a7ce8211ed14',
            'area_qr_code': '663e5e4bf5b8a7ce8211ed13',
            'area_tag_id': '6762f7b0922cc2a2f57d4044',
            'area_foto': '6763096aa99cee046ba766ad',
            'ubicacion_nombre_comercial': '667468e3e577b8b98c852aaa',
            'address_type': '663a7f67e48382c5b1230908',
            'ubicacion_country': '663a7ca6e48382c5b12308fa',
            'nombre_del_recorrido': '6645050d873fc2d733961eba',
            'usuario_id': '638a9a99616398d2e392a9f5',
            'usuario_nombre_completo': '638a9a7767c332f5d459fc81',
            'usuario_username': '6759e4a7a9a6e13c7b26da33',
            'usuario_email': '638a9a7767c332f5d459fc82',
            'usuario_phone': '67be0c43a31e5161c47f2bba',
            'empleado_nombre_completo': '62c5ff407febce07043024dd',
            'empleado_departamento': '663bc4ed8a6b120eab4d7f1e',
            'empleado_puesto': '663bc4c79b8046ce89e97cf4',
            'empleado_rfc': '663bcbe2274189281359eb71',
            'empleado_estatus_disponibilidad': '663bcbe2274189281359eb78',
            'tipo_de_equipo': '6639a9d9d38959539f59eb9f',
            'falla_concepto': '66397bae9e8b08289a59ec86',
            'falla_subconcepto': '679124a8483c5220455bcb99',
            'empleado_apoyo_nombre_completo': '663bd36eb19b7fb7d9e97ccb',
            'empleado_apoyo_id': '663bd466b19b7fb7d9e97cdc',
            'empleado_apoyo_estatus_disponibilidad': '663bd5c0b6e749213859eb72',
            'area_salida': '663fb45992f2c5afcfe97ca8',
            'visita_autorizada_nombre_completo': '5ea0693a0c12d5a8e43d37df',
            'visita_autorizada_curp': '5ea0897550b8dfe1f4d83a9f',
            'visita_autorizada_email': '5ea069562f8250acf7d83aca',
            'visita_autorizada_telefono': '663ec042713049de31e97c93',
            'visita_autorizada_foto': '5ea35de83ab7dad56c66e045',
            'visita_autorizada_identificacion': '65ce34985fa9df3dbf9dd2d0',
            'visita_autorizada_razon_social': '65fc814fb170488cf4d44c51',
            'visita_autorizada_email_contratista': '65fc814fb170488cf4d44c53',
            'visita_autorizada_estatus': '5ea1bd280ae8bad095055e61',
            'estados_state': '663a7dd6e48382c5b12308ff',
            'estados_state_code': '663a7dd6e48382c5b1230900',
            'proveedores_nombre_comercial': '667468e3e577b8b98c852aaa',
            'proveedores_razon_social': '6687f2f37b2c023e187d6252',
            'proveedores_rfc': '667468e3e577b8b98c852aab',
            'proveedores_email_contacto': '66bfd647cd15883ed163e9b5',
            'proveedores_telefono': '66bfd666cd15883ed163e9b6',
            'proveedores_web': '66bfd66ecd15883ed163e9b7',
            'lockers_id': '66480101786e8cdb66e70124',
            'lockers_tipo': '66ccfec6acaa16b31e5593a3',
            'lockers_estatus': '663961d5390b9ec511e97ca5',
            'objetos_categoria': '66ce23efc5c4d148311adf86',
            'objetos_descripcion': '66ce23efc5c4d148311adf87',
            'objetos_nombre': '66ce2441d63bb7a3871adeaf',
        }
        # self.f.update(centralized_fields)
        # forced_values = set(centralized_fields.values())
        # for k, v in list(self.f.items()):
        #     if v in forced_values and k not in centralized_fields:
        #         del self.f[k]
        # self.f.update(centralized_fields)
        
        self.incidence_filter = {
            'reporta_incidencia': "",
            'fecha_hora_incidencia':"",
            'ubicacion_incidencia':"",
            'area_incidencia': "",
            'incidencia':"",
            'comentario_incidencia': "",
            'tipo_dano_incidencia': "",
            'dano_incidencia':"",
            'evidencia_incidencia': [],
            'documento_incidencia':[],
            'prioridad_incidencia':"",
            'notificacion_incidencia':"",
            'datos_deposito_incidencia': [],
            'tags':[],
            'categoria':"",
            'sub_categoria':"",
            'incidente':"",
            'nombre_completo_persona_extraviada':"",
            'edad':"",
            'color_piel':"",
            'color_cabello':"",
            'estatura_aproximada':"",
            'descripcion_fisica_vestimenta':"",
            'nombre_completo_responsable':"",
            'parentesco':"",
            'num_doc_identidad':"",
            'telefono':"",
            'info_coincide_con_videos':"",
            'responsable_que_entrega':"",
            'responsable_que_recibe':"",
            'afectacion_patrimonial_incidencia':[],
            'personas_involucradas_incidencia': [],
            'acciones_tomadas_incidencia':[],
            'seguimientos_incidencia':[],
            'valor_estimado':"",
            'pertenencias_sustraidas':"",
            'placas':"",
            'tipo':"",
            'marca':"",
            'modelo':"",
            'color':"",
        }
        
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
    
    def get_folio_incidencia(self, record_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_INCIDENCIAS,
                "_id": ObjectId(record_id)
            }},
            {"$limit": 1},
            {"$sort": {"created_at": -1}},
            {"$project": {
                "_id": 0,
                "folio": "$folio"
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = self.unlist(response)
        return format_response

    def sync_incidence_to_lkf(self, id, rev):
        status = {}
        record = self.get_couch_record(id, rev)
        record = dict(record)
        record_id = record.pop('_id', None)
        folio = self.get_folio_incidencia(record_id)
        payload = {k: record[k] for k in self.incidence_filter.keys() if k in record}
            
        if isinstance(record, dict) and 'status_code' in record:
            return record
        elif isinstance(record, dict) and folio:
            folio = folio.get('folio', '')
            response = self.update_incidence(payload, folio)
        else:
            payload.update({'record_id': record_id})
            response = acceso_obj.create_incidence(payload)

        record = self.cr_db.get(record_id)
        if response.get('status_code') in [200, 201, 202]:
            record['status'] = 'synced'
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

        media = []
        if _rev == current_rev:
            attachments = record.get("_attachments", {})
            print('✅ Revisión actual encontrada')
            # for name in attachments:
            #     attachment = self.cr_db.get_attachment(_id, name)
            #     data = attachment.read()
            #     upload_image = self.upload_image_from_couchdb(data, name, self.BITACORA_INCIDENCIAS, self.f['evidencia_incidencia'])
            #     media.append(upload_image)
            record['status'] = 'recived'
            record['evidencia_incidencia'] = media
            self.cr_db.save(record)
            return record
        elif _rev in all_revs:
            print('⚠️ Revisión vieja')
            return {'status_code': 461, 'type': 'error', 'msg': 'Old revision found', 'data': {}}
        else:
            print('🕓 Revisión aún no propagada')
            return {'status_code': 462, 'type': 'error', 'msg': 'Revision not yet propagated', 'data': {}}

    def upload_image_from_couchdb(self, image_data, attachment_name, id_forma_seleccionada, id_field):
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, attachment_name)

        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(image_data)

        rb_file = open(temp_file_path, 'rb')
        dir_file = {'File': rb_file}

        try:
            upload_data = {'form_id': id_forma_seleccionada, 'field_id': id_field}
            upload_url = self.lkf_api.post_upload_file(data=upload_data, up_file=dir_file)
            rb_file.close()
        except Exception as e:
            rb_file.close()
            os.remove(temp_file_path)
            print("Error al subir el archivo:", e)
            return {"error": "Fallo al subir el archivo"}

        try:
            file_url = upload_url['data']['file']
            update_file = {'file_name': attachment_name, 'file_url': file_url}
        except KeyError:
            print('No se pudo obtener la URL del archivo')
            update_file = {"error": "Fallo al obtener la URL del archivo"}
        finally:
            os.remove(temp_file_path)
        return update_file
    
    def assign_user_inbox(self, data):
        status = {}
        lat = 0.0
        long = 0.0
        if len(self.geolocation) > 1:
            lat = self.geolocation[0]
            long = self.geolocation[1]
        epoc_today = int(time.time())
        format_check_areas = self.get_area_images(data.get(self.f['grupo_areas_visitadas'], []))
        inbox_record = {
            "_id": self.record_id,
            "type": "rondin",
            "kind": "inbox",
            "status": "new",
            "created_at": epoc_today,
            "updated_at": epoc_today,
            "created_by_id": self.user_id,
            "created_by_name": self.user_name,
            "geolocation": {
                "lat": lat,
                "long": long
            },
            "record": {
                "user_name": self.user_name,
                "nombre_rondin": data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.mf['nombre_del_recorrido'], ''),
                "ubicacion_rondin": data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], ''),
                "fecha_programada": data.get(self.f['fecha_programacion'], ''),
                "check_areas": format_check_areas,
            }
        }
        try:
            result = self.cr_db.save(inbox_record)
            if result:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Inbox assigned successfully', 'data': {}}
        except Exception as e:
            status = {'status_code': 400, 'type': 'error', 'msg': str(e), 'data': {}}
        return status

    def get_area_images(self, areas):
        location = self.answers.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], '')
        format_areas = [area.get(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['area'], '') for area in areas]
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": 121677, #TODO: Modularizar id
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.Location.f['area']}": {"$in": format_areas}
            }},
            {"$project": {
                "_id": 0,
                "tag_id": f"$answers.{self.f['area_tag_id']}",
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "area": f"$answers.{self.Location.f['area']}",
                "tipo_de_area": f"$answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_de_area']}",
                "foto_del_area": f"$answers.{self.f['area_foto']}",
            }}
        ]
        res = self.cr.aggregate(query)
        format_res = list(res)
        return format_res

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data_rondin = acceso_obj.current_record
    acceso_obj.user_name = data_rondin.get('created_by_name', '')
    acceso_obj.user_id = data_rondin.get('created_by_id', 0)
    acceso_obj.user_email = data_rondin.get('created_by_email', '')
    acceso_obj.geolocation = data_rondin.get('geolocation', [])
    acceso_obj.cr_db = acceso_obj.lkf_api.couch.set_db(f'clave_{acceso_obj.user_id}')
    script_attr = acceso_obj.data
    data = acceso_obj.data.get('data', {})
    option = data.get("option", script_attr.get('option', 'sync_incidence_to_lkf'))
    _id = data.get("_id", None)
    _rev = data.get("_rev", None)

    response = {}
    if option == 'get_user_catalogs':
        response = acceso_obj.get_user_catalogs()
    elif option == 'sync_incidence_to_lkf':
        response = acceso_obj.sync_incidence_to_lkf(id=_id, rev=_rev)
    elif option == 'assign_user_inbox':
        response = acceso_obj.assign_user_inbox(data=acceso_obj.answers)

    sys.stdout.write(simplejson.dumps(response, indent=3))