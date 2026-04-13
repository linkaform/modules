# coding: utf-8
import sys, simplejson, json, pytz, os
import tempfile, unicodedata, threading

from datetime import datetime
from bson.objectid import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed
from hmac import new
import time, random


from accesos_utils import Accesos
from account_settings import *
from linkaform_api import settings

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
        
        self.check_area_filter = {
            "tag_id": "",
            "ubicacion": "",
            "area": "",
            "tipo_de_area": "",
            "foto_del_area": [],
            "evidencia_incidencia": [],
            "documento_incidencia": [],
            "incidencias": [],
            "comentario_check_area": "",
            "status_check_area": "",
        }
        
        self.f.update({
            'bitacora_rondin_url': '690cefdca2dff2f469da17e0',
            'cantidad_areas_inspeccionadas': '68a7b68a22ac030a67b7f8f8',
            'checked_at': '68a7b68a22ac030a67b7f8f8',
            'form_name':'5d810a982628de5556500d55',
            'form_id':'5d810a982628de5556500d56',
        })
        
        self.IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.heic'}

    def delete_duplicate_areas(self, areas_list):
        res = []
        check_ids = []
        for area in areas_list:
            url = area.get(self.f['url_registro_rondin'])
            if not url:
                res.append(area)
                continue
            elif url not in check_ids:
                res.append(area)
                check_ids.append(url)
        return res

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

    def clean_db(self, data, batch_size=300):
        """
        Borra todos los registros con status 'received' en batches
        """
        total_deleted = 0

        while True:
            result = self.cr_db.find({
                "selector": {
                    "status": "received"
                },
                "limit": batch_size,
                "fields": ["_id", "_rev"]
            })

            docs = list(result)  # 🔥 FIX AQUÍ

            if not docs:
                print("No hay más registros para borrar")
                break

            to_delete = [
                {
                    "_id": doc["_id"],
                    "_rev": doc["_rev"],
                    "_deleted": True
                }
                for doc in docs
            ]

            response = self.cr_db.update(to_delete)

            total_deleted += len(to_delete)

            print(f"Batch eliminado: {len(to_delete)} | Total: {total_deleted}")

        return total_deleted

    def complete_rondines(self, records):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            if record.get('status_user') == 'completed':
                good_items.append(_id)
                record['inbox'] = False
                record['status'] = 'received'
                record['updated_at'] = self.today_str( date_format='datetime')
                self.cr_db.save(record)
        
        answers[self.f['estatus_del_recorrido']] = 'realizado'
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines completed successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status
    
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
            self.PASE_ENTRADA_ID,
            self.ACTIVOS_FIJOS_CAT_ID,
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

    def get_couch_record(self, _id=None, _rev=None):
        if not _id:
            return {'status_code': 400, 'type': 'error', 'msg': 'ID is required', 'data': {}}
        
        max_retries = 3
        wait_time = 2

        for attempt in range(max_retries):
            # self.cr_db = self.get_couch_user_db(db_name)
            record = self.cr_db.get(_id, revs_info=True)
            if not record:
                return {'status_code': 404, 'type': 'error', 'msg': 'Record not found', 'data': {}}

            current_rev = record.rev
            all_revs = [r['rev'] for r in record['_revs_info'] if r['status'] == 'available']

            if _rev == current_rev:
                attachments = record.get("_attachments", {})
                print('===> Revisión actual encontrada')
                return record
            elif _rev in all_revs:
                print(f'===> Revisión vieja, ultima revision registrada: {current_rev}')
                return {'status_code': 461, 'type': 'error', 'msg': 'Old revision found', 'data': {}}
            else:
                print(f'===> Revisión aún no propagada (Intento {attempt + 1}/{max_retries})')
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                else:
                    return {'status_code': 462, 'type': 'error', 'msg': 'Revision not yet propagated', 'data': {}}

    def upload_file_from_couchdb(self, image_data, attachment_name, id_forma_seleccionada, id_field):
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
    
    def build_area_inspection_map(self, data):
        """
        Construye:
        {
            'areas': {'nombre area': form_id},
            'inspection_ids': {
                form_id: [schema_preguntas]
            }
        }
        """
        result = {
            'areas': {},
            'inspection_ids': {}
        }
        for item in data:
            area_name = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID,{}).get(self.f['nombre_area'])
            for _, value in item.items():
                form_id = self.unlist(value.get(self.f['form_id'] ))
                if form_id:
                    result['areas'][area_name] = form_id
                if form_id not in result['inspection_ids']:
                    result['inspection_ids'][form_id] = self.get_form_question_schema(form_id)
        return result

    def get_form_question_schema(self, form_id):
        """
        Obtiene las pregutnas del formulario. Solo regiresa las pregutnas aceptadas por el rondin
        Args:
            form_id: el id de la forma a buscar y regresar con esuquema de pregutnas 
        Regresa el esquema de preguntas de una forma de Linkaform:
        [
            {
                'pregunta': '...',
                'tipo': 'radio|checkbox|decimal|integer|text',
                'opciones': [...],
                'required': True|False
            }
        ]
        """
        res = self.lkf_api.get_form_id_fields(form_id)
        if not res:
            return []

        form_info = res[0] if isinstance(res, list) else res
        fields = form_info.get('fields', [])

        questions = []
        for field in fields:
            options = field.get('options', [])
            question_schema = {
                'pregunta': field.get('label', ''),
                'tipo': field.get('field_type',''),
                'opciones': [opt.get('label') for opt in options if opt.get('label')],
                'required': field.get('required', False)
            }
            questions.append(question_schema)

        return questions


    def assign_user_inbox(self, data):
        """
        Asigna registro a usuario
        """
        user_id_to_assign = self.unlist(data.get(self.USUARIOS_OBJ_ID, {}).get(self.mf['id_usuario'], ''))
        if not user_id_to_assign:
            self.LKFException('No se encontro id de usuario en el registro a asignar')
        db_name = f'clave_{user_id_to_assign}'
        #sete la base de datos del usuario
        self.cr_db = self.get_couch_user_db(db_name)
        record = self.cr_db.get(self.record_id)
        #test borrar esto
        # if record:
        if False:
            return {'status_code': 202, 'type': 'success', 'msg': 'Ya existe el registro', 'data': {}}

        user_name_to_assign = data.get(self.USUARIOS_OBJ_ID, {}).get(self.mf['nombre_usuario'], '')
        nombre_recorrido = data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.mf['nombre_del_recorrido'], '')
        ubicacion_recorrido = data.get(self.CONFIGURACION_RECORRIDOS_OBJ_ID, {}).get(self.Location.f['location'], '')
        
        recorrido_info = self.get_info_recorrido(nombre_recorrido, ubicacion_recorrido)
        print('recorrido_info',recorrido_info)
        print('data',data)
        
        #! Revisar timezone en bitacora si sera necesario
        # user_data = self.lkf_api.get_user_by_id(self.user_id)
        # user_timezone = user_data.get('timezone', 'America/Mexico_City')

        status = {}
        lat = 0.0
        long = 0.0
        if len(self.geolocation) > 1:
            lat = self.geolocation[0]
            long = self.geolocation[1]
        epoc_today = int(time.time())
        #obtiene las areas bien formateadas
        format_check_areas = self.get_area_images(data.get(self.f['grupo_areas_visitadas'], []))
        inpections_by_area = self.build_area_inspection_map(data.get(self.f['grupo_areas_visitadas'], []))

        for i in format_check_areas:
            form_id = inpections_by_area['areas'].get(i['area'])
            i['inspeccion'] = inpections_by_area['inspection_ids'].get(form_id, {})
            i['checked'] = False
            i['checked_at'] = ''
            i['check_area_id'] = ''

        inbox_record = {
            "_id": self.record_id,
            "type": "rondin",
            "inbox": True,
            "status": "synced",
            "folio": getattr(self, 'folio', None),
            "status_user": "new",
            "created_at": epoc_today,
            "updated_at": self.today_str( date_format='datetime'),
            "created_by_id": user_id_to_assign,
            "created_by_name": user_name_to_assign,
            "geolocation": {
                "lat": lat,
                "long": long
            },
            "record": {
                "user_name": user_name_to_assign,
                "nombre_rondin": nombre_recorrido,
                "ubicacion_rondin": ubicacion_recorrido,
                "tipo_rondin": data.get(self.f['tipo_rondin'], 'qr'),
                "duracion_estimada": recorrido_info.get('duracion_estimada', ''),
                "fecha_programada": data.get(self.f['fecha_programacion'], ''),
                "fecha_inicio": "",
                "fecha_finalizacion": "",
                "fecha_pausa": "",
                "fecha_reanudacion": "",
                "ultimo_check_area_id": "",
                "check_areas": format_check_areas,
            }
        }
        try:
            result = self.cr_db.save(inbox_record)
            if result:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Inbox assigned successfully', 'data': {
                    'assigned_user_id': user_id_to_assign,
                    'assigned_user': user_name_to_assign,
                    'bitacora_record_id': self.record_id,
                    'bitacora_ubicacion': ubicacion_recorrido,
                    'bitacora_nombre_rondin': nombre_recorrido,
                    'bitacora_fecha_programada': data.get(self.f['fecha_programacion'], ''),
                }}
        except Exception as e:
            status = {'status_code': 400, 'type': 'error', 'msg': str(e), 'data': {}}
        return status
    
    def get_info_recorrido(self, nombre_recorrido, ubicacion_recorrido):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
                f"answers.{self.f['nombre_del_recorrido']}": nombre_recorrido,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": ubicacion_recorrido
            }},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "duracion_estimada": f"$answers.{self.f['duracion_estimada']}",
            }}
        ]
        res = self.cr.aggregate(query)
        format_res = {}
        if res:
            res = list(res)
            if res:
                format_res = self.unlist(res)
        return format_res
    
    #### Check de area >>>

    def sync_check_area_to_lkf(self, complete_record):
        """
        Sincroniza un registro de couchdb a linkaform
        """
        status = {}
        rondin_id = complete_record.get('rondin_id', '')
        record_id = complete_record.get('_id', None)
        record = complete_record.get('record', {})
        attachments_result = self.do_attachments(complete_record)
        complete_record = attachments_result['updated_record']
        if isinstance(record, dict) and 'status_code' in record:
            #Para que es esto, preguntar a Paco????
            print('neta aqui?????')
            return record
        # else:
        #     payload.update({
        #         'record_id': record_id,
        #         'rondin_id': rondin_id
        #     })
        response = self.create_check_area(complete_record)
        if response.get('status_code') in [200, 201, 202, 208,]:
            complete_record['status'] = 'received'
            #TODO delete backward compatibility
            complete_record['folio'] = response.get('json', {}).get('folio', '')
            self.cr_db.save(complete_record)
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record received successfully', 'data': {}}
        else:
            if response.get('status_code') == 400:
                last_error = response.get('json',{})
            else:
                last_error = response.get('json',{}).get('error', 'sync_check_area_to_lkf: Error creating record.')
            if response.get('status_code') == 400 and response.get('json',{}).get('code',0) == 8:
                print('TODO DESCOMENTARIAR PARA QUE SE MARQUE COMO RECIVIDO')
                complete_record['status'] = 'received'
                complete_record['last_error'] = last_error

                status = {'status_code': 208, 'type': 'success', 'msg': 'El id del registro no es único', 'data': {}}
            else:
                status = {'status_code': response.get('status_code',400), 'type': 'error', 'msg': last_error, 'data': {}}
                complete_record['status'] = 'error'
                complete_record['last_error'] = last_error
            res = self.cr_db.save(complete_record)
            print('status', status)
        return status
    

    def find_check_area_in_rondines(self, check_area_id):
        rondines = self.cr_db.find({
            "selector": {
                "type": "rondin",
                "record.check_areas": {
                    "$elemMatch": {
                        "check_area_id": check_area_id
                    }
                }
            },
            "limit": 1
        })

        rondin = next(iter(rondines), None)
        if not rondin:
            return None

        check_areas = rondin.get('record', {}).get('check_areas', [])
        for item in check_areas:
            if item.get('check_area_id') == check_area_id:
                return {
                    'rondin_id': rondin.get('_id'),
                    'rondin': rondin,
                    'check_area': item
                }

        return None

    def process_single_check_for_rondin(self, rec):
        """
        Procesa un check_area y regresa info util para agruparla por rondin_id.
        """
        _id = rec.get('_id')
        rondin_id = rec.get('rondin_id') or rec.get('record', {}).get('rondin_id', '')

        res = self.sync_check_area_to_lkf(complete_record=rec)

        check_info = {
            "check_id": _id,
            "status_code": res.get('status_code'),
            "ok": res.get('status_code') in [200, 201, 202, 208],
            "record": rec.get('record', {}),
            "folio": rec.get('folio', ''),
            "type": rec.get('type'),
        }

        if not rondin_id:
            check_data = self.find_check_area_in_rondines(_id)
            rondin_id = check_data.get('rondin_id')
        return {
            "rondin_id": rondin_id,
            "check": check_info,
            "raw_result": res
        }

    def process_check_area_stage(self, check_records):
        """
        Procesa todos los checks en paralelo.
        Cuando terminan, los agrupa por rondin_id.
        """
        checks_by_rondin = {}
        if not check_records:
            return checks_by_rondin

        # Tetsting purposes...
        # self.process_single_check_for_rondin(check_records[0])
        # breakpoint()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(self.process_single_check_for_rondin, rec): rec
                for rec in check_records
            }

            for future in as_completed(futures):
                rec = futures[future]

                try:
                    result = future.result()
                    rondin_id = result.get('rondin_id') or 'unknown'

                    checks_by_rondin.setdefault(rondin_id, []).append(result['check'])

                    with self.results_lock:
                        self.results["result"].append(result["raw_result"])
                        if result["check"]["ok"]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": result["raw_result"].get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rec.get('_id'),
                            "error": str(e)
                        })

        return checks_by_rondin

    #### Check de area <<<

    ### Rondines >>>

    # def sync_rondin_to_lkf(self, data):
    #     """
    #     Syncroniza un rondin a linkaform. 
    #     Obtiene todos lo checks de ubicacion de este rondin que existan.
    #     """
    #     status = {}
    #     rondin_id = data.get('_id', '')
    #     rondin_name = data.get('record', {}).get('nombre_rondin', '')
    #     # record_id = record.pop('_id', None)
    #     record = data.get('record', {})
        
    #     if isinstance(record, dict) and 'status_code' in record:
    #         return record
        
    #     #! Se obtienen los IDs con checked true en el registro de la bitacora en CouchDB
    #     check_ids = [ObjectId(i.get('check_area_id')) if i.get('checked') and i.get('status_check') == 'completed' else None for i in record.get('check_areas', [])]
    #     print('check_ids',check_ids)
    #     breakpoint()
    #     #! Se buscan los checks que ya existen en Linkaform
    #     checks_in_lkf = self.search_checks_in_lkf(check_ids)
    #     #! Se filtran los checks que no existen en Linkaform
    #     checks_not_in_lkf = []
    #     for i in record.get('check_areas', []):
    #         if i.get('checked') and i.get('check_area_id') not in checks_in_lkf:
    #             checks_not_in_lkf.append(i.get('check_area_id'))
    #     #! Se obtienen los detalles de los checks que no existen en Linkaform
    #     checks_details = self.cr_db.find({
    #         "selector": {
    #             "_id": {"$in": checks_not_in_lkf}
    #         }
    #     })

    #     payloads = self.process_checks(checks_details, rondin_id, rondin_name)
    #     #! Si hay payloads, se crean los checks en Linkaform
    #     if payloads:
    #         self.create_checks_in_lkf(payloads)

    #     #! 1. Obtener la bitacora del rondin en Linkaform y obtener las areas ya revisadas
    #     bitacora_in_lkf = self.get_bitacora_by_id(rondin_id)
    #     if not bitacora_in_lkf:
    #         self.LKFException({'title':'Error', 'msg': 'No se encontro la bitacora del rondin en Linkaform.'})

    #     #! Procesar attachments de incidencias del rondin
    #     incidencias_rondin = record.get('incidencia_rondin', [])

    #     attachments_result = self.do_attachments(data)
    #     data = attachments_result['updated_record']
    #     if len(attachments_result.get('uploaded')):
    #         self.cr_db.save(data)

    #     checks_in_lkf = []
    #     for item in bitacora_in_lkf.get('areas_del_rondin', []):
    #         if item.get('fecha_hora_inspeccion_area'):
    #             checks_in_lkf.append(item.get('incidente_area'))
            
    #     #! 2. Obtener la bitacora del rondin en CouchDB y obtener las areas ya revisadas
    #     bitacora_in_couch = data.get('record', {})
    #     checks_in_couch = bitacora_in_couch.get('check_areas', [])
    #     format_checks_in_couch = []
    #     for item in checks_in_couch:
    #         #! 2.1 Se compara si el check area ya existe en la bitacora de Linkaform
    #         if (item.get('checked') and not item.get('area') in checks_in_lkf and item.get('status_check') == 'completed')  \
    #                 or data.get('status_rondin') == 'completed':
    #             format_checks_in_couch.append(item.get('check_area_id'))

    #     new_checks = self.cr_db.find({
    #         "selector": {
    #             "_id": {"$in": format_checks_in_couch}
    #         },
    #         "limit": 1000
    #     })
    #     new_areas = {}
    #     for check in new_checks:
    #         new_areas[check.get('record', {}).get('area')] = check.get('record', {})
    #         new_areas[check.get('record', {}).get('area')].update({
    #             'timezone': check.get('timezone', ''),
    #             'fecha_check': check.get('created_at', ''),
    #             'record_id': check.get('_id', '')
    #         })
    #     new_incidencias = bitacora_in_couch.get('incidencia_rondin', [])
        
    #     if not isinstance(data, dict):
    #         data = self.cr_db.get(rondin_id)

    #     bitacora_response = self.update_bitacora_with_retry(bitacora_in_lkf, data, new_incidencias, new_areas)
    #     aux = self.cr_db.get(rondin_id)
    #     if bitacora_response and bitacora_response.get('status_code') in [200, 201, 202]:
    #         if aux.get('status_rondin') == 'completed':
    #             bitacora_in_lkf = self.get_bitacora_by_id(rondin_id)
    #             checks_in_lkf = [i.get('incidente_area') for i in bitacora_in_lkf.get('areas_del_rondin', []) if i.get('fecha_hora_inspeccion_area')]
    #             checks_in_couch = [i.get('area') for i in aux.get('record', {}).get('check_areas', []) if i.get('checked')]
    #             if len(checks_in_couch) != len(checks_in_lkf):
    #                 aux['status'] = 'received'
    #                 aux['inbox'] = False
                
    #                 self.cr_db.save(aux)
    #                 return {'status_code': 200, 'type': 'success', 'msg': 'Record synced successfully', 'data': {}}

    #             aux['status'] = 'received'
    #             aux['inbox'] = False

    #             self.cr_db.save(aux)
    #         status = {'status_code': 200, 'type': 'success', 'msg': 'Record synced successfully', 'data': {}}
    #     else:
    #         print('Revisando status de inbox para ver si es necesario guardar como error: ', aux )
    #         if aux['status'] != 'error':
    #             aux['status'] = 'error'
    #             # self.cr_db.save(aux)
    #         status = {'status_code': 400, 'type': 'error', 'msg': bitacora_response, 'data': {}}
    #     return status

    def sync_rondin_to_lkf(self, rondin_id, checks_for_rondin=[], rondin_record={}):
        """
        Sincroniza la bitácora del rondín hacia Linkaform ya sea usando checks ya procesados. O 
        Actualizar o cerrar el rondin en linkaform.
        No debe reconstruir estado desde CouchDB.
        """
        status = {}

        bitacora_in_lkf = self.get_bitacora_by_id(rondin_id)
        if not bitacora_in_lkf:
            return {
                'status_code': 404,
                'type': 'error',
                'msg': f'No se encontró bitácora en LKF para rondin_id={rondin_id}',
                'data': {}
            }

        new_areas = {}
        new_incidencias = []
        for item in checks_for_rondin:
            if not item.get('ok'):
                continue

            check = item.get('record', {})
            area_name = check.get('area')
            if not area_name:
                continue

            new_areas[area_name] = dict(check)
            new_areas[area_name].update({
                'timezone': check.get('timezone') or  self.user.get('timezone'), 
                'fecha_check': item.get('checked_at', ''),
                'record_id': item.get('check_id', '')
            })

            for incidencia in check.get('incidencias', []):
                new_item = dict(incidencia)
                new_item.update({
                    'area': area_name,
                    'fecha_incidencia': check.get('checked_at', '') or check.get('fecha_hora_incidencia', '')
                })
                new_incidencias.append(new_item)

        data = rondin_record or {
            '_id': rondin_id,
            'record': {},
            'status_user': ''
        }

        bitacora_response = self.update_bitacora_with_retry(
            bitacora_in_lkf,
            data,
            new_incidencias,
            new_areas
        )
        # solo si existe documento rondin y se sincronizó bien, marcar received
        if rondin_record and bitacora_response.get('status_code') in [200, 201, 202]:
            try:
                rondin_record['status'] = 'received'
                self.cr_db.save(rondin_record)
            except Exception as e:
                return {
                    'status_code': 409,
                    'type': 'error',
                    'msg': f'Bitácora actualizada pero no se pudo marcar rondín como received: {e}',
                    'data': {'bitacora_response': bitacora_response}
                }

        if bitacora_response.get('status_code') in [200, 201, 202]:
            status = {'status_code': 200, 'type': 'success', 'msg': 'Rondín actualizado correctamente', 'data': {}}
        else:
            status = {'status_code': 400, 'type': 'error', 'msg': bitacora_response, 'data': {}}

        return status

    # def process_single_rondin_with_checks(self, rec, checks_by_rondin):
    #     rondin_id = rec.get('_id')
    #     related_checks = checks_by_rondin.get(rondin_id, [])

    #     # aquí puedes meterlos al rec o mandarlos como argumento
    #     rec['processed_checks'] = related_checks

    #     res = self.sync_rondin_to_lkf(data=rec)

    #     return {
    #         "rondin_id": rondin_id,
    #         "status_code": res.get('status_code'),
    #         "ok": res.get('status_code') in [200, 201, 202, 208],
    #         "raw_result": res
    #     }

    def process_rondin_stage(self, rondin_records, checks_by_rondin):
        rondin_results = []

        if not rondin_records:
            return rondin_results

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}

            for rec in rondin_records:
                rondin_id = rec.get('_id')
                related_checks = checks_by_rondin.pop(rondin_id, [])
                futures[executor.submit(
                    self.sync_rondin_to_lkf,
                    rondin_id,
                    related_checks,
                    rec
                )] = rec

            for future in as_completed(futures):
                rec = futures[future]
                try:
                    result = future.result()
                    rondin_results.append(result)

                    with self.results_lock:
                        self.results["result"].append(result)
                        if result.get("status_code") in [200, 201, 202, 208]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": result.get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rec.get('_id'),
                            "error": str(e)
                        })

        return rondin_results

    def update_rondines_from_checks(self, checks_by_rondin):
        """
        Actualiza el Rondin, segun los check que se estan sincronizando. Este caso aplica para cuando un rondin esta pausado o en progreso
        Pero ya cuenta con checks de ubicacion, el rondin se va auto rellenando.
        """
        results = []

        if not checks_by_rondin:
            return results

        #TEST puposes
        # for rondin_id, checks in checks_by_rondin.items():
        #     self.sync_rondin_to_lkf(rondin_id, checks, None)
        # print(stop)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.sync_rondin_to_lkf, rondin_id, checks, None): rondin_id
                for rondin_id, checks in checks_by_rondin.items()
            }

            for future in as_completed(futures):
                rondin_id = futures[future]
                try:
                    result = future.result()
                    results.append({
                        'rondin_id': rondin_id,
                        'result': result
                    })

                    with self.results_lock:
                        self.results["result"].append(result)
                        if result.get("status_code") in [200, 201, 202, 208]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rondin_id,
                                "error": result.get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rondin_id,
                            "error": str(e)
                        })

        return results
    ### Rondines <<<

    def get_bitacora_by_id(self, record_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                "_id": ObjectId(record_id)
            }},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "answers": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = self.unlist(response)
        return format_response

    def parse_date_for_sorting(self, date_str):
        if not date_str or not date_str.strip():
            return datetime.max
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except:
            return datetime.max

    def format_incidencias_to_bitacora(self, bitacora_in_lkf, new_incidencias, new_areas):
        """
        Formate las incidencias para injectarlas al registro de bitacora de rondines
        Si el registro de Bitacora de rondines tiene incidencias existente las toma enc uenta
        """
        incidencias_list = []
        incidencias_existentes = bitacora_in_lkf.get('bitacora_rondin_incidencias', [])
        
        for incidencia in new_incidencias:
            fecha_incidencia = incidencia.get('fecha_incidencia')
            fecha_str = ""
            if fecha_incidencia:
                try:
                    s = fecha_incidencia.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    fecha_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    fecha_str = ""
            
            ya_existe = False
            for inc_existente in incidencias_existentes:
                if (inc_existente.get('tipo_de_incidencia') == incidencia.get('incidente') and
                    inc_existente.get('categoria') == incidencia.get('categoria') and
                    inc_existente.get('nombre_area_salida') == incidencia.get('area') and
                    inc_existente.get('fecha_hora_incidente_bitacora') == fecha_str):
                    ya_existe = True
                    break
            
            if not ya_existe:
                new_item = {
                    self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID: {
                        self.f['nombre_area_salida']: incidencia.get('area'),
                    },
                    self.f['fecha_hora_incidente_bitacora']: fecha_str,
                    self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                        self.f['categoria']: incidencia.get('categoria', ''),
                        self.f['sub_categoria']: incidencia.get('sub_categoria', ''),
                        self.f['incidente']: incidencia.get('incidente', ''),
                    },
                    self.f['incidente_open']: incidencia.get('otro_incidente', ''),
                    self.f['comentario_incidente_bitacora']: incidencia.get('comentario', ''),
                    self.f['incidente_accion']: incidencia.get('accion', ''),
                    self.f['incidente_evidencia']: [i for i in incidencia.get('evidencia', []) if i.get('file_url', '')],
                    self.f['incidente_documento']: [i for i in incidencia.get('documento', []) if i.get('file_url', '')],
                }
                incidencias_list.append(new_item)
        
        for item in new_areas.values():
            ts = item.get('fecha_check')
            fecha_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
            area = item.get('area', '')
            
            for incidencia in item.get('incidencias', []):
                ya_existe = False
                for inc_existente in incidencias_existentes:
                    if (inc_existente.get('tipo_de_incidencia') == incidencia.get('incidente') and
                        inc_existente.get('categoria') == incidencia.get('categoria') and
                        inc_existente.get('nombre_area_salida') == area and
                        inc_existente.get('fecha_hora_incidente_bitacora') == fecha_str):
                        ya_existe = True
                        break
                
                if not ya_existe:
                    new_item = {
                        self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID: {
                            self.f['nombre_area_salida']: area,
                        },
                        self.f['fecha_hora_incidente_bitacora']: fecha_str,
                        self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                            self.f['categoria']: incidencia.get('categoria', ''),
                            self.f['sub_categoria']: incidencia.get('sub_categoria', ''),
                            self.f['incidente']: incidencia.get('incidente', ''),
                        },
                        self.f['incidente_open']: incidencia.get('otro_incidente', ''),
                        self.f['comentario_incidente_bitacora']: incidencia.get('comentario', ''),
                        self.f['incidente_accion']: incidencia.get('accion', ''),
                        self.f['incidente_evidencia']: [i for i in incidencia.get('evidencia', []) if i.get('file_url', '')],
                        self.f['incidente_documento']: [i for i in incidencia.get('documento', []) if i.get('file_url', '')],
                    }
                    incidencias_list.append(new_item)
        
        for incidencia in incidencias_existentes:
            new_item = {
                self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID: {
                    self.f['nombre_area_salida']: incidencia.get('nombre_area_salida', ''),
                },
                self.f['fecha_hora_incidente_bitacora']: incidencia.get('fecha_hora_incidente_bitacora', ''),
                self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                    self.f['categoria']: incidencia.get('categoria', ''),
                    self.f['sub_categoria']: incidencia.get('sub_categoria', ''),
                    self.f['incidente']: incidencia.get('tipo_de_incidencia', ''),
                },
                self.f['incidente_open']: incidencia.get('incidente_open', ''),
                self.f['comentario_incidente_bitacora']: incidencia.get('comentario_incidente_bitacora', ''),
                self.f['incidente_accion']: incidencia.get('incidente_accion', ''),
                self.f['incidente_evidencia']: [i for i in incidencia.get('incidente_evidencia', []) if i.get('file_url', '')],
                self.f['incidente_documento']: [i for i in incidencia.get('incidente_documento', []) if i.get('file_url', '')],
            }
            incidencias_list.append(new_item)
        return incidencias_list

    def bitacora_set_area_format(self, nombre_area, check):
        ts = check.get('checked_at')
        timezone_str = check.get('timezone') or self.user.get('timezone')
        fecha_str = ""
        if ts:
            try:
                target_tz = pytz.timezone(timezone_str)
                if isinstance(ts, str):
                    dt_aware = target_tz.localize(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"))
                else:
                    dt_aware = datetime.fromtimestamp(ts, tz=target_tz)
                fecha_str = dt_aware.strftime("%Y-%m-%d %H:%M:%S")
            except (pytz.exceptions.UnknownTimeZoneError, Exception):
                fecha_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        res = {
                'incidente_area': nombre_area,
                'fecha_hora_inspeccion_area': fecha_str,
                'foto_evidencia_area_rondin': check.get('evidencia_incidencia', []),
                'comentario_area_rondin': check.get('comentario_check_area', ''),
                'url_registro_rondin': f"https://app.linkaform.com/#/records/detail/{check.get('record_id', '')}",
                }
        return res

    def create_check_area(self, data):
        """
        Registra Area, realiza check de area
        """
        # metadata = self.lkf_api.get_metadata(form_id=self.CHECK_UBICACIONES)
        record = data.get('record',{})
        answers = {}
        metadata = self.lkf_api.get_metadata(form_id=self.CHECK_UBICACIONES) #TODO: Modularizar id
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de check area",
                    "Action": "create_check_area",
                    "File": "accesos/app.py"
                }
            },
        })
        metadata.update({"id": data.id})
        if isinstance(data.get('geolocation'), dict):
            metadata.update({'geolocation': [data.get('geolocation').get('long'), data.get('geolocation').get('lat')]})

        metadata['start_date'] = record.get('checked_at', data.get('created_at', metadata['start_timestamp']))
        metadata['start_timestamp'] = self.get_epoch(metadata['start_date'] )
        metadata['end_timestamp'] = self.get_epoch(data.get('updated_at', metadata['end_timestamp']))
        metadata['timezone'] = data.get('timezone') or  metadata.get('timezone') or self.user.get('timezone')

        if data.get('rondin_id'):
            rondin_id = data.pop('rondin_id')
            answers[self.f['bitacora_rondin_url']] = f"https://app.linkaform.com/#/records/detail/{rondin_id}"

        if data.get('rondin_name'):
            rondin_name = data.pop('rondin_name')
            answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = {
                self.mf['nombre_del_recorrido']: rondin_name
            }
        #---Define Answers
        answers[self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID]={}
        answers[self.f['check_status']] = "continuar_siguiente_punto_de_inspección"
        for key, value in record.items():
            if key == 'tag_id':
                answers[self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({
                    self.f['area_tag_id']: value,
                    self.Location.f['location']: [record.get('ubicacion', '')],
                    self.Location.f['area']: [record.get('area', '')],
                    self.f['tipo_de_area']: [record.get('tipo_de_area', '')],
                    self.f['area_foto']: [record.get('foto_del_area', '')],
                })
            elif key == 'evidencia_incidencia':
                answers[self.f['foto_evidencia_area']] = value
            elif key == 'documento_incidencia':
                answers[self.f['documento_check']] = value
            elif key == 'incidencias':
                incidencias = record.get('incidencias', [])
                if incidencias:
                    incidencias_list = []
                    for incidencia in incidencias:
                        item = {}
                        if incidencia.get('categoria'):
                            item = {self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                                self.f['categoria']: incidencia.get('categoria', ''),
                                self.f['sub_categoria']: incidencia.get('sub_categoria', ''),
                                self.f['incidente']: incidencia.get('incidente', ''),
                            }}
                        item.update({
                            self.f['incidente_open']: incidencia.get('otro_incidente', ''),
                            self.f['incidente_comentario']: incidencia.get('comentario', ''),
                            self.f['incidente_accion']: incidencia.get('accion', ''),
                            self.f['incidente_evidencia']: incidencia.get('evidencia', ''),
                            self.f['incidente_documento']: incidencia.get('documento', ''),
                        })
                        incidencias_list.append(item)
                    answers[self.f['grupo_incidencias_check']] = incidencias_list
            elif key == 'comentario_check_area':
                answers[self.f['comentario_check_area']] = value
            elif key == 'status_check_area':
                answers[self.f['check_status']] = value
            else:
                continue
            
        
        metadata.update({'answers':answers})
        print('ANSWERS=', simplejson.dumps(metadata, indent=3))
        res = self.lkf_api.post_forms_answers(metadata)
        return res
    
    def delete_rondines(self, records):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            good_items.append(_id)
            self.cr_db.delete(record)
        
        answers[self.f['estatus_del_recorrido']] = 'cancelado'
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines deleted successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status
  
    def get_user_data(self, user_id):
        query = [{"$match": {
            "deleted_at": {"$exists": False},
            "form_id": 129958, #TODO: Modularizar id
            f"answers.{self.mf['id_usuario']}": user_id
        }},
        {"$limit": 1},
        {"$sort": {"created_at": -1}},
        {"$project": {
            "_id": 0,
            "id": f"$answers.{self.mf['id_usuario']}",
            "name": f"$answers.{self.mf['nombre_usuario']}",
            "email": f"$answers.{self.mf['email_visita_a']}",
        }}]
        reponse = self.format_cr(self.cr.aggregate(query))
        format_response = self.unlist(reponse)
        return format_response
    
    def reasignar_rondines(self, records, user_to_assign):
        status = {}
        answers = {}
        bad_items = []
        good_items = []
        
        user_id = user_to_assign.get('id', 0)
        name = user_to_assign.get('name', '')
        user_data = self.get_user_data(user_id=user_id)
        email = user_data.get('email', '')
        
        if not records:
            return {'status_code': 400, 'type': 'error', 'msg': 'No records provided', 'data': {}}
        
        db_name = f'clave_{self.user_id}'
        # self.cr_db = self.get_couch_user_db(db_name)
        for item in records:
            _id = item.get('_id', None)
            _rev = item.get('_rev', None)
            
            if not _id or not _rev:
                bad_items.append(item)
                continue
            
            record = self.get_couch_record(_id=_id, _rev=_rev)
            
            if record.get('status_code') in [400, 404, 461, 462]:
                bad_items.append(item)
                continue
            
            if record:
                good_items.append(_id)
                record['inbox'] = False
                record['status_user'] = 'deleted'
                record['status'] = 'received'
                self.cr_db.save(record)
        
        answers[self.USUARIOS_OBJ_ID] = {
            self.mf['nombre_usuario']: name,
            self.mf['id_usuario']: [user_id],
            self.mf['email_visita_a']: [email],
        }
        if good_items:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=good_items)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                status = {'status_code': 200, 'type': 'success', 'msg': 'Rondines assigned successfully', 'data': {}}
            else: 
                status = {'status_code': 400, 'type': 'error', 'msg': res, 'data': {}}
        if bad_items:
            status.update({'data': {'bad_items': bad_items, 'good_items': good_items}})
        return status

    def get_active_guards(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                f"answers.{self.f['fecha_inicio_turno']}": {"$exists": True},
                f"answers.{self.f['fecha_cierre_turno']}": {"$exists": False},
            }},
            {"$project": {
                "_id": 0,
                "created_by_id": 1,
                "created_by_name": 1,
                "created_by_email": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            for item in response:
                new_item = {
                    'guard_id': item.get('created_by_id', 0),
                    'guard_name': item.get('created_by_name', ''),
                    'guard_email': item.get('created_by_email', ''),
                }
                format_response.append(new_item)
        response = {'status_code': 200, 'type': 'success', 'msg': 'Active guards retrieved successfully', 'data': format_response}
        return response
    
    # def search_checks_in_lkf(self, id_list):
    #     query = [
    #         {"$match": {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.CHECK_UBICACIONES,
    #             "_id": {"$in": id_list}
    #         }},
    #         {"$project": {
    #             "_id": 1,
    #         }}
    #     ]
    #     response = self.format_cr(self.cr.aggregate(query))
    #     format_response = []

    #     if response:
    #         format_response = [item.get('_id') for item in response]
    #     return format_response
    
    ### revisar si esto no esta repitdio >>>
    def _process_single_check_record(self, record):
        record_id = record.get('record_id', None)

        # Filter file lists to ensure file_url exists
        file_keys = ['foto_del_area', 'evidencia_incidencia', 'documento_incidencia']
        for key in file_keys:
            if key in record and isinstance(record[key], list):
                record[key] = [
                    item for item in record[key] 
                    if item.get('file_url')
                ]

        if 'incidencias' in record and isinstance(record['incidencias'], list):
            for incidencia in record['incidencias']:
                incidencia_file_keys = ['evidencia', 'documento']
                for key in incidencia_file_keys:
                    if key in incidencia and isinstance(incidencia[key], list):
                        incidencia[key] = [
                            item for item in incidencia[key] 
                            if item.get('file_url')
                        ]

        response = {}
        try:
            response = self.create_check_area(record)
        except Exception as e:
            self.LKFException({'title': 'Error inesperado', 'msg': str(e)})
        
        if record_id:
            record = self.cr_db.get(record_id)
            if record:
                if response.get('status_code') in [200, 201, 202]:
                    record['status'] = 'received'
                    record['folio'] = response.get('json', {}).get('folio', '')
                    self.cr_db.save(record)
                else:
                    record['status'] = 'error'
                    record['last_error'] = response['json']['error']
                    self.cr_db.save(record)

    def create_checks_in_lkf(self, records):
        with ThreadPoolExecutor(max_workers=15) as executor:
            executor.map(self._process_single_check_record, records)

    ### revisar si esto no esta repitdio <<<

    def _process_attachment_upload(self, check_id, name, existing_urls, field_id=None):
        # Check if already uploaded
        """
        revisa si y ya existe la url, si no sube la foto a linkaform
        """
        current_url = existing_urls.get(name, '')
        if current_url and current_url.startswith('http'):
            return None

        attachment = self.cr_db.get_attachment(check_id, name)
        data = attachment.read()
        field_id = None
        if not field_id:
            if name.endswith('.png') or name.endswith('.jpg') or name.endswith('.jpeg'):
                field_id = self.f['foto_evidencia_area']
            else:
                field_id = self.f['documento_check']
        upload_image = self.upload_file_from_couchdb(data, name, self.CHECK_UBICACIONES, field_id)
        return upload_image
    
    def update_bitacora(self, bitacora_in_lkf, data, new_incidencias, new_areas):
        """
        Actualiza la bitacora de Rondines
        """
        answers={}
        res = {}
        areas_list = []
        conf_recorrido = {}
        estatus_bitacora_in_couch = data.get('status_user', '')
        
        incidencias_list = self.format_incidencias_to_bitacora(bitacora_in_lkf, new_incidencias, new_areas)
        answers[self.f['bitacora_rondin_incidencias']] = incidencias_list
        
        if 'areas_del_rondin' not in bitacora_in_lkf:
            bitacora_in_lkf['areas_del_rondin'] = []
        
        # Va a iterar las areas acutales si existe el nombre de la area en el rondin
        # quiere decir que ya previamente se habia inspeccionado, se actualzia el area
        # y se quita de las areas nuevas
        print('updating bitacora')
        for item in bitacora_in_lkf.get('areas_del_rondin', []):
            nombre_area = item.get('incidente_area')
            check = new_areas.pop(nombre_area, None)
            if check:
                item.update(self.bitacora_set_area_format(nombre_area, check))

        # Despues de quitar las areas existentes, agrega las areas nuevas
        for nombre_area, check in new_areas.items():
            new_item = self.bitacora_set_area_format(nombre_area, check)
            print('new_item=', new_item)
            bitacora_in_lkf['areas_del_rondin'].append(new_item)

        for key, value in bitacora_in_lkf.items():
            if key == 'new_user_complete_name':
                answers[self.USUARIOS_OBJ_ID] = {
                    self.f['new_user_complete_name']: value,
                    self.f['new_user_id']: [self.user_id],
                    self.f['new_user_email']: [self.user_email]
                }
            elif key == 'fecha_programacion':
                answers[self.f['fecha_programacion']] = value
            elif key == 'fecha_inicio_rondin':
                answers[self.f['fecha_inicio_rondin']] = value
            elif key == 'fecha_fin_rondin':
                answers[self.f['fecha_fin_rondin']] = value
            elif key == 'estatus_del_recorrido' and value:
                answers[self.f['estatus_del_recorrido']] = value
            elif key == 'incidente_location':
                conf_recorrido.update({
                    self.f['ubicacion_recorrido']: value
                })
            elif key == 'nombre_del_recorrido':
                conf_recorrido.update({
                    self.f['nombre_del_recorrido']: value
                })
            elif key == 'estatus_del_recorrido':
                answers[self.f['estatus_del_recorrido']] = value
            elif key == 'areas_del_rondin':
                for item in value:
                    areas_list.append({
                        self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                            self.f['nombre_area']: item.get('incidente_area', ''),
                        },
                        self.f['fecha_hora_inspeccion_area']: item.get('fecha_hora_inspeccion_area', ''),
                        self.f['foto_evidencia_area_rondin']: item.get('foto_evidencia_area_rondin', []),
                        self.f['comentario_area_rondin']: item.get('comentario_area_rondin', ''),
                        self.f['url_registro_rondin']: item.get('url_registro_rondin', '')
                    })
                    
        all_areas_sorted = sorted(
            areas_list,
            key=lambda x: self.parse_date_for_sorting(x.get(self.f['fecha_hora_inspeccion_area'], ''))
        )
        answers[self.f['areas_del_rondin']] = self.delete_duplicate_areas(all_areas_sorted)
        answers[self.f['estatus_del_recorrido']] = 'en_proceso' if estatus_bitacora_in_couch != 'completed' else 'realizado'
        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = conf_recorrido
        if not answers.get(self.f['fecha_inicio_rondin']):
            answers[self.f['fecha_inicio_rondin']] = data.get('record', {}).get('fecha_inicio', '')

        comentarios_in_couch = data.get('record', {}).get('comentarios_rondin', [])
        comentarios_in_lkf = bitacora_in_lkf.get('grupo_comentarios_generales', [])
        comentarios_existentes = set()
        comentarios_finales = []
        
        for comentario in comentarios_in_lkf:
            fecha = comentario.get('grupo_comentarios_generales_fecha', '')
            texto = comentario.get('grupo_comentarios_generales_texto', '')
            comentarios_existentes.add((fecha, texto))
        
        for comentario in comentarios_in_lkf:
            nuevo_comentario = {
            self.f['grupo_comentarios_generales_fecha']: comentario.get('grupo_comentarios_generales_fecha', ''),
            self.f['grupo_comentarios_generales_texto']: comentario.get('grupo_comentarios_generales_texto', '')
            }
            comentarios_finales.append(nuevo_comentario)
        
        for comentario in comentarios_in_couch:
            fecha = comentario.get('fecha', '')
            texto = comentario.get('texto', '')
            
            if (fecha, texto) not in comentarios_existentes:
                nuevo_comentario = {
                    self.f['grupo_comentarios_generales_fecha']: fecha,
                    self.f['grupo_comentarios_generales_texto']: texto
                }
                comentarios_finales.append(nuevo_comentario)
        
        answers[self.f['grupo_comentarios_generales']] = comentarios_finales
        if answers:
            metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_RONDINES)
            metadata.update(self.get_record_by_folio(bitacora_in_lkf.get('folio'), self.BITACORA_RONDINES, select_columns={'_id': 1}, limit=1))

            metadata.update({
                'properties': {
                    "device_properties": {
                        "system": "Addons",
                        "process":"Actualizacion de Bitacora", 
                        "accion":'rondines_cache', 
                        "folio": bitacora_in_lkf.get('folio'), 
                        "archive": "rondines_cache.py"
                    }
                },
                'answers': answers,
                '_id': bitacora_in_lkf.get('_id')
            })
            res = self.net.patch_forms_answers(metadata)
        return res
        
    def update_bitacora_with_retry(self, bitacora_in_lkf, data, new_incidencias, new_areas, max_retries=5, base_wait=2):
        """
        Reintenta update_bitacora en caso de error 208 (registro ocupado).
        - base_wait: espera inicial en segundos antes del primer intento
        - Backoff exponencial + jitter en cada reintento
        """
        for attempt in range(max_retries):
            # Espera antes de cada intento (incluyendo el primero)
            wait = base_wait * (2 ** attempt) + random.uniform(0, 1)
            print(f'Esperando {wait:.1f}s antes del intento {attempt + 1}/{max_retries}...')
            time.sleep(wait)

            response = self.update_bitacora(bitacora_in_lkf, data, new_incidencias, new_areas)

            if response.get('status_code') == 208:
                print(f'Registro ocupado (208), reintentando...')
                continue

            # Cualquier otra respuesta (éxito o error diferente) se retorna directo
            return response

        return {'status_code': 408, 'type': 'error', 'msg': 'Max retries exceeded after 208 conflicts', 'data': {}}
    
    def sync_incidence_to_lkf(self, record):
        status = {}
        record_id = record.pop('_id', None)
        record = record.get('record', {})
        folio = self.get_folio_incidencia(record_id)
        payload = {k: record[k] for k in self.incidence_filter.keys() if k in record}
            
        if isinstance(record, dict) and 'status_code' in record:
            return record
        elif isinstance(record, dict) and folio:
            folio = folio.get('folio', '')
            response = self.update_incidence(payload, folio)
        else:
            payload.update({'record_id': record_id})
            response = self.create_incidence(payload)

        record = self.cr_db.get(record_id)
        if response.get('status_code') in [200, 201, 202]:
            record['status'] = 'synced'
            record['updated_at'] = self.today_str( date_format='datetime')
            self.cr_db.save(record)
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record synced successfully', 'data': {}}
        else:
            record['last_error'] = record.get('json',{}).get('error','Error 1250 al sincronizar incidencia')
            record['status'] = 'error'
            record['updated_at'] = self.today_str( date_format='datetime')
            self.cr_db.save(record)
            status = {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        return status

    def _handle_result(self, res, _id):
        """Registra el resultado de una operación y retorna True/False."""
        if res.get('status_code') not in (200, 201, 202, 208):
            with self.results_lock:
                self.results["failed"] += 1
                self.results["errors"].append({"id": _id, "error": res.get('msg')})
            return False
        with self.results_lock:
            self.results["success"] += 1
        return True

    def is_valid_url(self, value):
        return isinstance(value, str) and value.startswith('http')

    def get_extension(self, filename):
        return os.path.splitext(filename or '')[1].lower()

    def infer_field_id(self, filename):
        ext = self.get_extension(filename)
        if ext in self.IMAGE_EXTENSIONS:
            return self.f['foto_evidencia_area']
        return self.f['documento_check']

    def find_pending_media_nodes(self, node, found=None, path='record'):
        """
        Recorre recursivamente dicts/lists y encuentra diccionarios que:
        - tengan file_path
        - no tengan file_url válido
        Regresa una lista con referencias al nodo original.
        """
        if found is None:
            found = []

        if isinstance(node, dict):
            has_file_path = bool(node.get('file_path'))
            has_valid_url = self.is_valid_url(node.get('file_url', ''))

            if has_file_path and not has_valid_url:
                found.append({
                    'node': node,
                    'path': path
                })

            for key, value in node.items():
                self.find_pending_media_nodes(value, found, f'{path}.{key}')

        elif isinstance(node, list):
            for idx, item in enumerate(node):
                self.find_pending_media_nodes(item, found, f'{path}[{idx}]')
        return found

    def _process_attachment_upload_universal(self, doc_id, media_node):
        """
        media_node es el diccionario original dentro de record.
        Si encuentra attachment en CouchDB, lo sube a LKF y actualiza el mismo dict.
        """
        attachment_name = media_node.get('name') or media_node.get('file_name')
        file_url = media_node.get('file_url', '')
        file_path = media_node.get('file_path', '')

        if not attachment_name:
            return {
                'success': False,
                'error': 'No se encontró name ni file_name',
                'node': media_node
            }

        if self.is_valid_url(file_url):
            return {
                'success': True,
                'skipped': True,
                'reason': 'Ya tenía file_url',
                'node': media_node
            }

        # try:
        if True:
            attachment = self.cr_db.get_attachment(doc_id, attachment_name)
            if not attachment:
                return {
                    'success': False,
                    'error': f'No se encontró attachment en CouchDB: {attachment_name}',
                    'node': media_node
                }

            data = attachment.read()
            field_id = self.infer_field_id(attachment_name)

            upload_result = self.upload_file_from_couchdb(
                data,
                attachment_name,
                self.CHECK_UBICACIONES,
                field_id
            )

            if upload_result.get('error'):
                return {
                    'success': False,
                    'error': upload_result['error'],
                    'node': media_node
                }

            # Actualiza el mismo nodo dentro de record
            media_node['file_name'] = upload_result.get('file_name', attachment_name)
            media_node['file_url'] = upload_result.get('file_url', '')
            return {
                'success': True,
                'file_name': media_node['file_name'],
                'file_url': media_node['file_url'],
                'file_path': file_path,
                'node': media_node
            }

        # except Exception as e:
        #     return {
        #         'success': False,
        #         'error': str(e),
        #         'node': media_node
        #     }

    def do_attachments(self, record):
        """
        Sube todos los attachments que se le pasen a Linkaform utilizando hilos
        """
        media = []
        pending_nodes = self.find_pending_media_nodes(record.get('record'))
        if not pending_nodes:
            return {
                'updated_record': record,
                'uploaded': [],
                'errors': [],
                'total_found': 0
            }
        uploaded = []
        errors = []

        max_workers = 30
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._process_attachment_upload_universal, record['_id'], item['node'])
                for item in pending_nodes
            ]

            for future in as_completed(futures):
                result = future.result()
                if result.get('success'):
                    if not result.get('skipped'):
                        uploaded.append(result)
                else:
                    errors.append(result)

        print('pending_nodes', pending_nodes)
        return {
            'updated_record': record,
            'uploaded': uploaded,
            'errors': errors,
            'total_found': len(pending_nodes)
        }

    def get_area_model(self, record):
        """
        Traduce un record de CouchDB al formato answers de LKF.
        record['record'] contiene los datos del área capturada en la app.
        """
        attachments_result = self.do_attachments(record)
        if attachments_result.get('updated_record'):
            record = attachments_result['updated_record']

        data = record.get('record', {})
        rec_id = record['_id']

        nombre_area =  data.get('incidente_area')
        area_catalogo =  data.get('area_catalogo')
        if not nombre_area and not area_catalogo:
            return {"error":"Nombre de Area Requerido"}

        answers = {}
        # Catálogo de ubicación 
        answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID] = {
            self.f['location'] : data.get('incidente_location', ''),
        }
        if area_catalogo:
            answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({self.f['nombre_area'] : area_catalogo})

        # Nombre de Area
        answers[self.configuracion_area['nombre_nueva_area']] = nombre_area

        # Catalogo tipo de area
        answers[self.TIPO_AREA_OBJ_ID] = {
            self.f['tipo_de_area'] : data.get('tipo_area', ''),
            }

        existing_urls = {} #obtener las existntes

        #foto del area
        answers[self.configuracion_area['foto_area']] = data.get('area_foto',[])

        #TagId
        answers[self.configuracion_area['tag_id']] = data.get('area_tag_id', '')

        # Comentario del área
        answers[self.configuracion_area['comentarios']] = data.get('comentario', '')

        return answers

    def config_area(self, record):
        if not record:
            return {'status_code': 400, 'type': 'error', 'msg': 'No record provided', 'data': {}}

        _id = record.id
        _rev = record.rev

        if not _id or not _rev:
            return {'status_code': 400, 'type': 'error', 'msg': 'Missing _id or _rev', 'data': {}}

        answers = self.get_area_model(record)
        
        print('answers', simplejson.dumps(answers, indent=3))

        metadata = self.lkf_api.get_metadata(form_id=self.CONFIGURACION_AREA_FORM)
        metadata.update({'answers': answers})

        res = self.lkf_api.post_forms_answers(metadata)

        if res.get('status_code') in (200, 201, 202):
            self.cr_db.delete(record)
            res = {'status_code': 200, 'type': 'success', 'msg': 'Area synced', 'data': {}}
        else:
            record['status'] = 'error'
            if res['status_code'] == 400:
                last_error = res.get('json',{})
            else:
                last_error = res.get('json',{}).get('error','Error al crear la configuracon del area')
            if isinstance(last_error, dict):
                record['status'] = 'error'
                last_error = last_error.get('exception', last_error)

            res = {'status_code': 400, 'type': 'error', 'msg': last_error, 'data': {}}
            record['updated_at'] = self.today_str( date_format='datetime')
            record['last_error'] = last_error
            self.cr_db.save(record)

        return res

    # def process_record(self, rec):
    #     """
    #     Procesa registro segun su tipo
    #     """
    #     # try:
    #     if True:
    #         _id = rec.id
    #         _rev = rec.rev
    #         r_type = rec.get('type')
    #         r_status = rec.get('status')

    #         print(f"Processing record {_id}  type={r_type}, status={r_status}")
    #         if r_type == 'area':
    #             res = self.config_area(rec)
    #             print('Sync Area res=', res)
    #         elif r_type == 'rondin':
    #             print('todo...sync rondin')
    #             res = self.sync_rondin_to_lkf(data=rec)
    #         elif r_type == 'check_area':
    #             print('Syncing Area ...')
    #             res = acceso_obj.sync_check_area_to_lkf(complete_record=rec)
    #         else:
    #             print(f"Unknown type '{r_type}' for record {_id}, skipping.")
    #             res = {'status_code': 400, 'msg': f"Unknown type '{r_type}'"}

    #         return self._handle_result(res, _id)


        # except Exception as e:
        #     with self.results_lock:
        #         self.results["failed"] += 1
        #         self.results["errors"].append({"id": rec.id, "error": str(e)})
        #     print(f"Error processing record {rec.id}: {e}")
        #     return False

    def group_records_by_type(self, records):
        """
        Regresa un diccionario con los registros agrupados por type.
        Ej:
        {
            "rondin": [rec1, rec2],
            "check_area": [rec3],
            "area": [rec4, rec5],
            "unknown": [rec6]
        }
        """
        grouped = {}

        for rec in records:
            r_type = rec.get('type') or 'unknown'
            if r_type not in grouped:
                grouped[r_type] = []
            grouped[r_type].append(rec)

        return grouped

    # def sync_records(self, app_records=[]):
    #     """
    #     Obtiene todos los registros a syncronizar, busca todo lo que este pendiente de procesar
    #     osea todo lo que NO sea ni recivido ni nuevo o que no tenga un error que el usuario tenga q corregir
    #     """
    #     records = self.cr_db.find({
    #         "selector": {
    #             # "rondin_id": "69a1e79fbb4c1f4e2c9714e0",
    #             # "_id": "69c3365ed87df8d7625941eb",
    #             "$or": [
    #                 {"status": {"$exists": False}},
    #                 {"status": {"$nin": ["received", "new", "user_error"]}}
    #             ],
                
    #             "$or": [
    #                 {"status_user": "completed"},
    #                 {"status_user": {"$exists": False}}
    #             ]
    #         },
    #         "limit": 1000
    #     })
    #     record_list = list(records)
    #     grouped_records = self.group_records_by_type(record_list)
    #     print('grouped_records', {
    #       k: len(v) for k, v in grouped_records.items()
    #     })
    #     breakpoint()
    #     # record_list = [record_list[0],]
    #     print('BORRAR ESTO LIMITO A 1 REGISTRO POR PRUEBAS')
    #     if not record_list:
    #         print("No records to sync")
    #         return
    #     self.results = {
    #         "success": 0,
    #         "failed": 0,
    #         "errors": [],
    #         "result": []
    #     }
    #     self.results_lock = threading.Lock()

    #     print('BORRAR TESTIN..................')
    #     # rec = record_list[0]
    #     # self.process_record(rec)


    #     with ThreadPoolExecutor(max_workers=100) as executor:
    #         futures = {executor.submit(self.process_record, rec): rec for rec in record_list}

    #         for future in as_completed(futures):
    #             rec = futures[future]
    #             if future.exception():
    #                 print(f"Unhandled exception for record {rec.id}: {future.exception()}")
    #                 with self.results_lock:
    #                     self.results["failed"] += 1
    #                     self.results["errors"].append({
    #                         "id": rec.id,
    #                         "error": str(future.exception())
    #                     })
    #             else:
    #                 self.results["result"].append(future.result())

    #     # FIX: self.results, no results
    #     print(f"\nSync complete — Success: {self.results['success']} | Failed: {self.results['failed']}")
    #     if self.results["errors"]:
    #         print("Errors:")
    #         for err in self.results["errors"]:
    #             print(f"  - {err['id']}: {err['error']}")

    #     return self.results



    ### SYNC process >>>

    def process_stage_in_parallel(self, records, handler, max_workers=10):
        """
        Procesa en paralelo los tipos de registros segun su tipo 
        """
        stage_results = []

        if not records:
            return stage_results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(handler, rec): rec
                for rec in records
            }

            for future in as_completed(futures):
                rec = futures[future]
                try:
                    result = future.result()
                    stage_results.append(result)

                    with self.results_lock:
                        self.results["result"].append(result)
                        if result.get("status_code") in [200, 201, 202, 208]:
                            self.results["success"] += 1
                        else:
                            self.results["failed"] += 1
                            self.results["errors"].append({
                                "id": rec.get('_id'),
                                "error": result.get("msg")
                            })

                except Exception as e:
                    with self.results_lock:
                        self.results["failed"] += 1
                        self.results["errors"].append({
                            "id": rec.get('_id'),
                            "error": str(e)
                        })

        return stage_results

    def sync_records(self, app_records=[]):
        record_list = []
        records = self.cr_db.find({
            "selector": {
                "status_user": "completed",
                "status": "synced"
            },
            "limit": 1000
        })
        record_list += list(records)

        #TODO DELETE una vez que ya se hayan migrado todas las apps
        # backward compatibility: checks viejos
        records_check = self.cr_db.find({
            "selector": {
                 "status_check": "completed",
                 "status": "synced"
            },
            # "limit": 1000
            "limit": 1
        })
        record_list += list(records_check)

        #TODO DELETE una vez que ya se hayan migrado todas las apps
        # backward compatibility: checks viejos
        records_rondin = self.cr_db.find({
            "selector": {
                 "status_rondin": "completed",
                 "status": "synced"
            },
            "limit": 1000
        })
        record_list += list(records_rondin)

        # quitar duplicados
        unique_records = {}
        for rec in record_list:
            unique_records[rec.get('_id')] = rec
        record_list = list(unique_records.values())

        if not record_list:
            print("No records to sync")
            return

        self.results = {
            "success": 0,
            "failed": 0,
            "errors": [],
            "result": []
        }
        self.results_lock = threading.Lock()

        grouped_records = self.group_records_by_type(record_list)

        area_results = self.process_stage_in_parallel(
            grouped_records.get('area', []),
            self.process_area_record,
            max_workers=10
        )

        #todo
        if  grouped_records.get('incidencia'):
            response = acceso_obj.sync_incidence_to_lkf(record=record)

        # 1. primero checks
        checks_by_rondin = self.process_check_area_stage(
            grouped_records.get('check_area', [])
        )
        print('ya hizo los checks', checks_by_rondin)
        # checks_by_rondin = independent_results.get('check_area')
        # 2. luego rondines explícitos
        rondin_results = self.process_rondin_stage(
            grouped_records.get('rondin', []),
            checks_by_rondin
        )

        # 3. lo que sobró en checks_by_rondin son rondines que no venían como registro
        missing_rondin_results = self.update_rondines_from_checks(
            checks_by_rondin
        )

        return {
            "results": self.results,
            "rondin_results": rondin_results,
            "missing_rondin_results": missing_rondin_results,
        }

    def process_area_record(self, rec):
        return self.config_area(rec)    

    def process_checks(self, checks_details, rondin_id, rondin_name):
        #! Se crean los payloads para crear los checks en Linkaform
        payloads = []
        for i in checks_details:
            check_evidencias = i.get('record', {}).get('evidencia_incidencia', [])
            check_documentos = i.get('record', {}).get('documento_incidencia', [])
            check_incidencias = i.get('record', {}).get('incidencias', [])

            # Build a map of file_name -> file_url to check existing URLs
            existing_urls = {}
            for item in check_evidencias + check_documentos:
                existing_urls[item.get('file_name')] = item.get('file_url', '')
            for inc in check_incidencias:
                for item in inc.get('evidencia', []) + inc.get('documento', []):
                    existing_urls[item.get('file_name')] = item.get('file_url', '')

            attachments = i.get('_attachments', {})
            media = self.do_attachments(attachments)
            if attachments:
                for m in media:
                    m_name = m.get('file_name')
                    m_url = m.get('file_url')
                    
                    for item in check_evidencias:
                        if item.get('file_name') == m_name:
                            item['file_url'] = m_url
                            
                    for item in check_documentos:
                        if item.get('file_name') == m_name:
                            item['file_url'] = m_url
                            
                    for incidencia in check_incidencias:
                        for item in incidencia.get('evidencia', []):
                            if item.get('file_name') == m_name:
                                item['file_url'] = m_url
                        for item in incidencia.get('documento', []):
                            if item.get('file_name') == m_name:
                                item['file_url'] = m_url

                i['updated_at'] = self.today_str( date_format='datetime')
                self.cr_db.save(i)

            record = i.get('record', {})
            payload = {k: record[k] for k in self.check_area_filter.keys() if k in record}
            payload.update({
                'record_id': i.get('_id'),
                'rondin_id': rondin_id,
                'rondin_name': rondin_name
            })
            payloads.append(payload)
        return payloads
        


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data_rondin = acceso_obj.current_record
    acceso_obj.user_name = data_rondin.get('created_by_name', '')
    acceso_obj.user_id = data_rondin.get('created_by_id', acceso_obj.user.get('user_id', 0))
    acceso_obj.user_email = data_rondin.get('created_by_email', acceso_obj.user.get('email', ''))
    acceso_obj.geolocation = data_rondin.get('geolocation', [])
    script_attr = acceso_obj.data
    data = acceso_obj.data.get('data', {})
    option = data.get("option", script_attr.get('option', ''))
    _id = data.get("_id", None)
    _rev = data.get("_rev", None)
    records = data.get("records", [])
    user_to_assign = data.get("user_to_assign", {})
    response = {}
    db_name = f'clave_{acceso_obj.user_id}'
    acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
    if option == 'get_user_catalogs':
        response = acceso_obj.get_user_catalogs()
    # elif option == 'sync_to_lkf':
    #     db_name = f'clave_{acceso_obj.user_id}'
    #     acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
    #     record = acceso_obj.get_couch_record(_id=_id, _rev=_rev)
    #     record = dict(record)
    #     type_sync = record.get('type', '')
    #     if type_sync == 'incidencia':
    #         response = acceso_obj.sync_incidence_to_lkf(record=record)
        # elif type_sync == 'check_area':
        #     response = acceso_obj.sync_check_area_to_lkf(complete_record=record)
        # elif type_sync == 'rondin':
        #     response = acceso_obj.sync_rondin_to_lkf(data=record)
        # elif type_sync == 'error':
        #     response = record
        # else:
        #     response = {'status_code': 400, 'type': 'error', 'msg': 'Unknown error', 'data': {
        #         'type_sync': type_sync,
        #         'record': record,
        #         'db_name': db_name,
        #     }}
    elif option == 'assign_user_inbox':
        response = acceso_obj.assign_user_inbox(data=acceso_obj.answers)
    elif option == 'complete_rondines':
        response = acceso_obj.complete_rondines(records=records)
    elif option == 'delete_rondines':
        response = acceso_obj.delete_rondines(records=records)
    elif option == 'reasignar_rondines':
        response = acceso_obj.reasignar_rondines(records=records, user_to_assign=user_to_assign)
    elif option == 'get_active_guards':
        response = acceso_obj.get_active_guards()
    elif option in ('sync','synced','rondin','check_area','sync_to_lkf'):
        #todo quitar sync_to_lkf meterlo al new worflos
        if  option == 'sync_to_lkf':
            db_name = f'clave_{acceso_obj.user_id}'
            acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
            record = acceso_obj.get_couch_record(_id=_id, _rev=_rev)
            record = dict(record)
            type_sync = record.get('type', '')
            if type_sync == 'incidencia':
                response = acceso_obj.sync_incidence_to_lkf(record=record)
            else:
                response = acceso_obj.sync_records(records)
    elif option == 'clean_db':
        response = acceso_obj.clean_db(data)
    else:
        response = {'status_code': 400, 'type': 'error', 'msg': 'Invalid option', 'data': {}}

    sys.stdout.write(simplejson.dumps(response))