# coding: utf-8
from datetime import date
import sys, simplejson, pytz
from tkinter import N
from bson import ObjectId
from linkaform_api import settings
from account_settings import *
from datetime import datetime
import calendar
import sys, os

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

        self.CONFIGURACION_RECORRIDOS_FORM = self.lkm.form_id('configuracion_de_recorridos','id')

        self.f.update({
            'rondin_area': '663e5d44f5b8a7ce8211ed0f',
            'foto_area': '6763096aa99cee046ba766ad',
            'porcentaje_de_areas_inspeccionadas': '689a7ecfbf2b4be31039388e',
            'cantidad_areas_inspeccionadas': '68a7b68a22ac030a67b7f8f8',
            'usuarios_invitados': '69df1816f0a5742e4f94d1e3',
            'recorridos':f"{self.CONFIGURACION_RECORRIDOS_OBJ_ID}",
            'asignado_a':f"{self.USUARIOS_OBJ_ID}",
            'tipo_rondin':'69b9b98d2a02f4a0dd35f5c1',
            'fecha_hora_programada_inicio':'6760a8e68cef14ecd7f8b6fe',
            'fecha_hora_inicio':'6818ea068a7f3446f1bae3b3',
            'cantidad_areas_inspeccionadas':'68a7b68a22ac030a67b7f8f8',
            'porcentaje_avance':'689a7ecfbf2b4be31039388e',
            'estatus_recorrido':'6639b2744bb44059fc59eb62',
            'areas':'66462aa5d4a4af2eea07e0d1',
            'duracion_rondin':'6639b47565d8e5c06fe97cf3',
            'motivo_cancelacion':'6639b6180bb793945af2742d',
            'comentario_general':'69149dcec7b3ec9f2b9395b2',
            'comentarios_generales':'6927a0cdc03f0f8e5355437a',
            'url_rondin':'690cefdca2dff2f469da17e0',
            'nombre_emp':'638a9a7767c332f5d459fc81',
            'area':f"{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.mf['nombre_area_salida']}",
        })


    def _extract_record_id_from_url(self, registro_padre_value):
        """
        registro_padre puede ser URL completa:
        https://host/#/records/detail/6612abc123...
        o directo el _id. Retorna solo el ID.
        """
        if not registro_padre_value:
            return None
        if '/' in str(registro_padre_value):
            return registro_padre_value.rstrip('/').split('/')[-1]
        return registro_padre_value

    def _get_child_records(self, registro_padre):
        """
        Busca en MongoDB todos los hijos que apunten a este parent_id en registro_padre.
        """
        query = {
            'form_id': self.BITACORA_RONDINES,
            'deleted_at': {'$exists': False},
            f'answers.{self.rondin_keys["registro_padre"]}': registro_padre,
        }
        return list(self.cr.find(query))

    def rondin_asignado_a(self, asignado_a):
        """
        Crea grupo repetitivo de personas asignadas a un rondin.
        args:
            asignado_a (str): 'responsable_en_turno' o nombre de un empleado
        return:
            lista con elementos para el grupo asignado a del rondin
        """
        employee = {}
        visita_set = {}

        if not asignado_a or asignado_a == 'responsable_en_turno':
            # Usa el empleado del usuario actual (igual que access_pass_vista_a)
            employee = self.Employee.get_employee_data(
                user_id=self.user['user_id'], get_one=True
            )
            self.employee = employee
            visita_set = self.visita_a_set_format(employee)
            return [visita_set] if visita_set else []

        # Es un nombre de persona específica
        employee = self.Employee.get_employee_data(name=asignado_a, get_one=True)
        self.employee = employee
        visita_set = self.visita_a_set_format(employee)

        if visita_set and self.employee:
            return [visita_set]
        else:
            # Fallback: inserta solo el nombre si no encuentra en catálogo
            return [{
                self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                    self.mf['nombre_empleado']: asignado_a
                }
            }]

    def claim_rondin(self, record_id):
        """
        El usuario actual reclama este rondin.
        1. Determina si es padre o hijo
        2. Obtiene todos los registros relacionados (padre + hermanos, o hijos)
        3. Borra el inbox de CouchDB de los otros usuarios
        """
        # --- 1. Obtener el registro actual desde MongoDB ---
        record = self.get_record_by_id(record_id)
        if not record:
            return False, "Registro no encontrado"

        answers = record.get('answers', {})
        registro_padre = answers.get(self.rondin_keys['registro_padre'])

        # --- 2. Determinar familia de registros ---
        if registro_padre:
            # Es un hijo → buscar padre y todos los hermanos
            parent_id = self._extract_record_id_from_url(registro_padre)
            siblings = self._get_child_records(registro_padre)
            related_records = [r for r in siblings if str(r['_id']) != str(record_id)]
            parent_record = self.get_record_by_id(parent_id)
            if parent_record:
                related_records.append(parent_record)
        else:
            # Es padre → buscar todos sus hijos
            children = self._get_child_records(record_id)
            related_records = [r for r in children if str(r['_id']) != str(record_id)]

        # --- 3. Bloqueo atómico ---
        STATUS_FIELD = f'answers.{self.mf["estatus_del_recorrido"]}'
        
        related_ids = [r['_id'] for r in related_records]
        all_ids = related_ids + [record['_id']]

        if related_ids:
            # TODO utilizar session de mongo para que sea una transaccion ACID
            # Paso 1: Obtener exactamente cuáles están en 'programado'
            programados = list(self.cr.find(
                {'_id': {'$in': all_ids}, STATUS_FIELD: 'programado'},
                {'_id': 1}  # solo necesitamos el _id
            ))
            programados_ids = [r['_id'] for r in programados]

            if len(programados_ids) != len(all_ids):
                return False, "El rondin ya fue reclamado por otro usuario"

            #Paso 2: update_many solo sobre los que YO encontré en 'programado'
            result = self.cr.update_many(
                {'_id': {'$in': programados_ids}, STATUS_FIELD: 'programado'},
                {'$set': {STATUS_FIELD: 'reclamado'}}
            )


        # --- 4. El registro reclamado pasa a en_proceso ---
        self.cr.update_one(
            {'_id': ObjectId(record['_id'])},
            {'$set': {STATUS_FIELD: 'en_proceso'}}
        )


        if related_records:
            related_ids = [str(r['_id']) for r in related_records]
            # related_ids.append(str(record_id))  # incluir el que se está reclamando
            self.lkf_api.patch_multi_record(
                answers={self.rondin_keys['status']: 'reclamado'},
                form_id=self.BITACORA_RONDINES,
                record_id=related_ids,
            )
        
        self.lkf_api.patch_multi_record(
            answers={self.mf['estatus_del_recorrido']: 'en_proceso'},
            form_id=self.BITACORA_RONDINES,
            record_id=[str(record['_id'])],
        )


        return True, {'claimed': record_id, 'unassinged_records': len(related_ids)}

    def delete_claimed_record(self):
        #TODO HAY QUE LLEVAR ESTO A UNA OPCION DE PARA QUE EL WORKFLOW LO BORRE
        usuario_obj = self.answers.get(self.USUARIOS_OBJ_ID, {})

        user_id = self.unlist(usuario_obj.get(self.mf['id_usuario'], []))

        if not user_id:
            return True

        rel_record_id = str(self.current_record['_id'])
        try:
            db_name = f"clave_{user_id}"
            couch_db = self.get_couch_user_db(db_name)
            couch_record = couch_db.get(rel_record_id)
            if couch_record:
                couch_db.delete(couch_record)
        except Exception as e:
            errors.append({'user_id': user_id, 'record_id': rel_record_id, 'error': str(e)})
        return True

    def create_rondin(self, rondin_data: dict = {}):
        """Crea un rondin con los datos proporcionados.
        Args:
            rondin_data (dict): Un diccionario con los datos del rondin.
        Returns:
            response: La respuesta de la API de Linkaform al crear el rondin.
        """
        #! Data hardcoded for testing purposes
        #! Ejemplo rondin de recurrencia diaria
        # rondin_data = {
        no_use_data = {
            #! ======== PRIMERA PAGINA - INFORMACION GENERAL
            'nombre_rondin': 'Ejemplo rondin Nov 8',
            'duracion_estimada': '30 minutos',
            'ubicacion': 'Planta Monterrey',
            'areas': [
                'Almacén de inventario',
                'Sala de Juntas Planta Baja',
                'Caseta 6 Poniente',
                'Antenas',
            ],
            'grupo_asignado': '',
            #! ======== SEGUNDA PAGINA - FECHA Y HORA DE INICIO
            'fecha_hora_programada': '2025-11-05 15:00:00',
            #! ======== SEGUNDA PAGINA - ANTICIPACION
            'programar_anticipacion': 'no', # 'si' o 'no'
            'cuanto_tiempo_de_anticipacion': '', # numero decimal
            'cuanto_tiempo_de_anticipacion_expresado_en': '', # 'minutos', 'horas', 'dias', 'semanas', 'mes'
            #! ======== SEGUNDA PAGINA - TIEMPO PARA EJECUTAR LA TAREA
            'tiempo_para_ejecutar_tarea': 30,
            'tiempo_para_ejecutar_tarea_expresado_en': 'minutos',
            #! ======== SEGUNDA PAGINA - RECURRENCIA
            'la_tarea_es_de': 'cuenta_con_una_recurrencia', # 'es_de_única_ocación'
            'se_repite_cada': 'diario', # 'hora', 'diario', 'semana', 'mes', 'año', 'configurable'
            'sucede_cada': 'igual_que_la_primer_fecha',
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE
            'sucede_recurrencia': [], # 'minuto', 'hora', 'dia_de_la_semana', 'dia_del_mes', 'mes'
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE - MINUTO
            'en_que_minuto_sucede': '', # numero decimal
            'cada_cuantos_minutos_se_repite': '', # numero decimal
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE - HORA
            'en_que_hora_sucede': '', # numero decimal
            'cada_cuantas_horas_se_repite': '', # numero decimal
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE - DIA DE LA SEMANA
            'que_dias_de_la_semana': [], # ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE - DIA DEL MES
            'que_dia_del_mes': '', # numero decimal que no deberia ser decimal
            'cada_cuantos_dias_se_repite': '', # numero decimal que no deberia ser decimal
            #! ======== SEGUNDA PAGINA - RECURRENCIA CONFIGURABLE - MES
            'en_que_mes': '', # 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
            'cada_cuantos_meses_se_repite': '', # numero decimal que no deberia ser decimal
            #! ======== SEGUNDA PAGINA - EN QUE SEMANA SUCEDE
            'en_que_semana_sucede': 'todas_las_semanas', # 'primera_semana_del_mes', 'segunda_semana_del_mes', 'tercera_semana_del_mes', 'cuarta_semana_del_mes', 'quinta_semana_del_mes'
            #! ======== SEGUNDA PAGINA - FECHA FINAL DE RECURRENCIA SOLO SI ES DE UNICA OCASION
            'la_recurrencia_cuenta_con_fecha_final': 'no', # 'si' o 'no'
            'fecha_final_recurrencia': '', # 'YYYY-MM-DD HH:MM:SS'
            #! ======== SEGUNDA PAGINA - ACCION DE RECURRENCIA
            'accion_recurrencia': 'programar' # 'programar', 'pausar' o 'eliminar'
        }
        answers = {}
        tipo_asignacion = rondin_data.get('tipo_asignacion', 'responsable_en_turno')
        ubicacion_result = self.get_ubicacion_geolocation(location=rondin_data.get('ubicacion', ''))
        rondin_data['ubicacion'] = ubicacion_result if ubicacion_result else rondin_data.get('ubicacion', '')
        areas_result = self.get_areas_details(areas_list=rondin_data.get('areas', []))
        rondin_data['areas'] = areas_result if areas_result else rondin_data.get('areas', [])
        for key, value in rondin_data.items():
            if key == 'ubicacion':
                if isinstance(value, dict):
                    answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                        self.Location.f['location']: value.get('location', ''),
                        self.f['address_geolocation']: value.get('geolocation', [])
                    }
                else:
                    # Llegó como string directo (geolocation no encontró nada)
                    answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                        self.Location.f['location']: value,
                        self.f['address_geolocation']: []
                    }
            elif key == 'area':
                if value:
                    answers[self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID] = {
                        self.mf['nombre_area_salida']: value
                    }
            elif key == 'grupo_asignado':
                answers[self.GRUPOS_CAT_OBJ_ID] = {
                    self.rondin_keys[key]: value
                }
            elif key == 'areas':
                areas_list = []
                for area in value:
                    if isinstance(area, dict):
                        area_dict = {
                            self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.Location.f['area']: area.get('area', ''),
                                self.f['geolocalizacion_area_ubicacion']: [{
                                    'latitude': area.get('latitude', 0),
                                    'longitude': area.get('longitude', 0)
                                }],
                                self.f['foto_area']: area.get('image', []),
                                self.f['area_tag_id']: [area.get('tag_id', [])]
                            }
                        }
                    else:
                        # Llegó como string directo (get_areas_details no encontró nada)
                        area_dict = {
                            self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.Location.f['area']: area,
                                self.f['geolocalizacion_area_ubicacion']: [],
                                self.f['foto_area']: [],
                                self.f['area_tag_id']: []
                            }
                        }
                    areas_list.append(area_dict)
                answers[self.rondin_keys[key]] = areas_list
            elif key == "cron_id":
                answers[self.rondin_keys['cron_id']] = valor
            elif key == 'sucede_recurrencia' and ('dia_del_mes' in value or 'mes' in value):
                actual_day = datetime.now().day
                answers[self.rondin_keys['que_dia_del_mes']] = int(actual_day)
                answers[self.rondin_keys[key]] = value
            elif value == '':
                pass
            elif key == 'tipo_rondin':
                answers[self.rondin_keys[key]] = value.lower()
            elif key == 'roles':
                answers[self.f['grupo_roles']] = [
                    {self.ROL_CATALOG_OBJ_ID: {self.f['rol']: rol}}
                    for rol in value
                ]
            elif key == 'tipo_asignacion':
                answers[self.rondin_keys['tipo_asignacion']] = value
            elif key == 'asignado_a':
                if not value:
                    pass
                elif tipo_asignacion == 'grupo':
                    print("aqui andamos")
                    grupo_asignado = value[0] if isinstance(value, list) else value
                    answers[self.GRUPOS_CAT_OBJ_ID] = {
                        self.rondin_keys['grupo_asignado']: grupo_asignado,
                    }
                elif tipo_asignacion == 'persona_especifica':
                    nombre = value[0] if isinstance(value, list) else value
                    answers[self.rondin_keys['grupo_asignado_a']] = self.rondin_asignado_a(nombre)
                else:
                    # responsable_en_turno
                    answers[self.rondin_keys['grupo_asignado_a']] = self.rondin_asignado_a(value)
            else:
                answers[self.rondin_keys[key]] = value
        print('creando rondin...', simplejson.dumps(answers, indent=4))
        response = self.create_register(
            module='Accesos',
            process='Creacion de un rondin',
            action='rondines',
            file='accesos/app.py',
            form_id=self.CONFIGURACION_RECORRIDOS_FORM,
            answers=answers
        )
        return response

    def create_register(self, module: str, process: str, action: str, file: str, form_id: int, answers: dict):
        """Crea un registro en Linkaform con los metadatos y respuestas proporcionadas.

        Args:
            module (str): El nombre del módulo que está ejecutando la acción.
            process (str): El nombre del proceso que se está ejecutando.
            action (str): El nombre del script que se está ejecutando.
            file (str): La ruta del archivo donde se encuentra el app del modulo utilizado(Ej. jit/app.py).
            form_id (str): El ID de la forma en Linkaform.
            answers (dict): El diccionario de respuestas ya formateado.

        Returns:
            response: La respuesta de la API de Linkaform al crear el registro.
        """
        metadata = self.lkf_api.get_metadata(form_id=form_id)

        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": module,
                    "Process": process,
                    "Action": action,
                    "File": file
                }
            },
        })

        metadata.update({'answers':answers})
        response = self.lkf_api.post_forms_answers(metadata)
        return response

    def create_incidencia_by_rondin(self, data):
        # data = {
        #     'reporta_incidencia': "Emiliano Zapata",
        #     'fecha_hora_incidencia': "2025-10-24 13:07:16",
        #     'ubicacion_incidencia':"Planta Monterrey",
        #     'area_incidencia': "Recursos eléctricos",
        #     'categoria': "Intrusión y seguridad",
        #     'sub_categoria':"Alteración del orden",
        #     # "tipo_incidencia": "Otro incidente",
        #     'comentario_incidencia': "comentario random",
        #     'evidencia_incidencia': [],
        #     'documento_incidencia':[],
        #     'acciones_tomadas_incidencia':[],
        #     "prioridad_incidencia": "leve",
        #     "notificacion_incidencia": "no",
        # }
        status = {}
        response = self.create_incidence(data)
        if response.get('status_code') in [200, 201, 202]:
            id_incidencia= response.get("json",{}).get("id")
            link= "https://app.linkaform.com/#/records/detail/"+id_incidencia
            print("link",link)
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record created successfully', 'data': {}}
        else:
            status = {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        return status

    def delete_rondin(self, folio: str):
        """Elimina un rondin por su folio.
        Args:
            folio (str): El folio del rondin a eliminar.
        Returns:
            dict: Un diccionario con el estado de la operación.
        Raises:
            Exception: Si el folio no es proporcionado.
        """
        if not folio:
            raise Exception("Folio is required to delete a rondin.")

        response = 404
        print('TODO, ELIMINAR REVISAR CONFIGURACION DE RONDINES, primero eliminar de airflow...')
        answers = {
            self.rondin_keys['accion_recurrencia']: 'eliminar'
        }
        response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CONFIGURACION_RECORRIDOS_FORM, folios=[folio,])
        if response.get('status_code') == 202:
            response =response
        else:
            response = response
        return response

    def detail_response(self, status_code: int):
        """Devuelve un mensaje detallado según el código de estado HTTP.
        Args:
            status_code (int): El código de estado HTTP devuelto por la API.
        Returns:
            dict: Un diccionario con el estado y el mensaje correspondiente.
        """
        if status_code in [200, 201, 202]:
            return {"status": "success", "message": "Operation completed successfully."}
        elif status_code in [400, 404]:
            return {"status": "error", "message": "Bad request or resource not found."}
        elif status_code in [500, 502, 503]:
            return {"status": "error", "message": "Server error, please try again later."}
        else:
            return {"status": "error", "message": "Unexpected error occurred."}

    def edit_areas_rondin(self, areas, folio, record_id):
        metadata = self.lkf_api.get_metadata(form_id=self.CONFIGURACION_RECORRIDOS_FORM)
        metadata.update(self.get_record_by_folio(record_id, self.CONFIGURACION_RECORRIDOS_FORM, select_columns={'_id':1}, limit=1))

        full_rondin =self. get_rondin_by_id(record_id)
        print(simplejson.dumps(full_rondin, indent=4))

        answers = {}
        answers[self.rondin_keys['grupo_asignado_rondin']] = {}

        for key, value in full_rondin.items():
            if key == 'nombre_del_rondin':
                print("21323", key)
                answers.update({f"{self.rondin_keys['nombre_rondin']}":value})
            elif key == 'ubicacion':
                answers[self.Location.UBICACIONES_CAT_OBJ_ID]={
                    self.rondin_keys['ubicacion']: full_rondin.get('ubicacion', ''),
                    self.f['address_geolocation']: [full_rondin.get('ubicacion_geolocation',{})]
                }
            elif key == 'grupo_asignado_rondin':
                answers[key].append({
                    self.rondin_keys['grupo_asignado']: full_rondin.get('grupo_asignado', ""),
                    self.rondin_keys['id_grupo']: full_rondin.get('id_grupo', ""),
                })

            else:
                if key in self.rondin_keys:
                    answers.update({
                        f"{self.rondin_keys[key]}": value
                    })

        if areas:
                areas_list = []
                for a in areas:
                    obj = {f"{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}":{
                            self.f['rondin_area'] : a.get('rondin_area', ''),
                            self.f['foto_area']:a.get('foto_area', ''),
                            self.f['geolocalizacion_area_ubicacion'] :a.get('geolocalizacion_area_ubicacion', ''),
                            self.f['area_tag_id'] :a.get('area_tag_id', ''),
                        }}
                    areas_list.append(obj)
        answers.update({self.rondin_keys['areas']:areas_list})

        metadata.update({
            'properties': {
                "device_properties":{
                    "system": "Addons",
                    "process":"Actualizacion de Areas Rondin",
                    "accion":'edit_areas_rondin',
                    "folio": folio,
                    "archive": "rondines.py"
                }
            },
            'answers': answers,
            '_id': record_id
        })
        res= self.net.patch_forms_answers(metadata)
        return res

    def format_rondin_by_id(self, data):
        fotos_de_areas = []
        puntos_de_control = []

        for item in data.get('areas', []):
            foto_area_data = item.get('foto_area', [])
            foto_url = ""
            if foto_area_data:
                primer_elemento = foto_area_data[0]
                if isinstance(primer_elemento, list) and len(primer_elemento) > 0:
                    foto_url = primer_elemento[0].get('file_url', '')
                elif isinstance(primer_elemento, dict):
                    foto_url = primer_elemento.get('file_url', '')

            area_id = item.get('area_tag_id', [])[0] if len(item.get('area_tag_id', [])) > 0 else ""
            nombre = item.get('rondin_area', '')
            geo_list = item.get('geolocalizacion_area_ubicacion', [])
            geo = geo_list[0] if geo_list else {}

            if foto_url:
                fotos_de_areas.append({
                    "id": area_id,
                    "nombre_area": nombre,
                    "foto_area": foto_area_data,
                    "geolocation_area": geo,
                })

            puntos_de_control.append({
                "id": area_id,
                "nombre_area": nombre,
                "geolocation_area": geo,
                "foto_area": foto_area_data,
            })

        data.update({
            "recurrencia": data.get('recurrencia').replace('_', ' ').title() if data.get('recurrencia') else 'No Recurrente',
            "estatus_rondin": data.get('estatus_rondin').replace('_', ' ').title() if data.get('estatus_rondin') else 'No Especificado',
            "ubicacion_geolocation": (data.get('ubicacion_geolocation') or [{}])[0],
            "images_data": fotos_de_areas,
            "map_data": puntos_de_control,
        })
        return data

    def format_incidencias_rondines(self, data, area):
        format_data = []
        for item in data:
            incidencias = item.get('incidencias_rondin', [])
            for index, incidencia in enumerate(incidencias):
                if area:
                    incidencia_area = incidencia.get('nombre_area_salida', '')
                    if incidencia_area != area:
                        continue

                format_item = {
                    "id": item.get('_id',''),
                    "folio": item.get('folio',''),
                    "ref_number": index,
                    "ubicacion_incidente": item.get('ubicacion', ''),
                    "area_incidente": incidencia.get('nombre_area_salida', ''),
                    "nombre_del_recorrido": item.get('nombre_recorrido', ''),
                    "fecha_hora_incidente": incidencia.get('fecha_hora_incidente_bitacora', ''),
                    "categoria": incidencia.get('categoria', 'General'),
                    "subcategoria": incidencia.get('sub_categoria', 'General'),
                    "incidente": incidencia.get('incidencia', incidencia.get('incidente_open', '')),
                    "accion_tomada": incidencia.get('incidente_accion', ''),
                    "comentarios": incidencia.get('comentario_incidente_bitacora', ''),
                    "evidencias": incidencia.get('incidente_evidencia', []),
                    "documentos": incidencia.get('incidente_documento', []),
                    "link": incidencia.get('link', ""),
                }
                format_data.append(format_item)
        return format_data

    def format_rondines_images(self, data):
        format_data = []
        for index, item in enumerate(data):
            format_item = {
                "id": item.get('_id',''),
                "folio": item.get('folio',''),
                "ref_number": index,
                "ubicacion": item.get('ubicacion', ''),
                "nombre_recorrido": item.get('nombre_recorrido', ''),
                "nombre_area": item.get('rondin_area', ''),
                "fecha_y_hora_check": item.get('fecha_hora_inspeccion_area', ''),
                "comentario_check": item.get('comentario_area_rondin', ''),
                "url_check": item.get('url_registro_rondin', ''),
                "fotos_check": item.get('foto_evidencia_area_rondin', []),
            }
            format_data.append(format_item)
        return format_data

    def format_bitacora_rondines(self, data):
        if hasattr(self, 'timezone') and self.timezone:
            try:
                tz = pytz.timezone(self.timezone)
                now = datetime.now(tz)
            except Exception:
                now = datetime.now()
        else:
            now = datetime.now()
        current_year = now.year
        current_month = now.month
        days_in_month = calendar.monthrange(current_year, current_month)[1]

        format_data = []

        for item in data:
            hora_agrupada = item.get('hora_agrupada', '')
            categorias_raw = item.get('categorias', [])

            categorias_formateadas = []

            # Procesar cada categoría (recorrido)
            for categoria in categorias_raw:
                nombre_recorrido = categoria.get('nombre_recorrido', '')
                bitacora_rondines = categoria.get('bitacora_rondines', [])

                areas_recorrido = []
                if bitacora_rondines:
                    primera_bitacora = bitacora_rondines[0]
                    areas_del_rondin = primera_bitacora.get('areas_del_rondin', [])
                    areas_recorrido = [
                        {'rondin_area': area.get('rondin_area', ''), 'area_tag_id': area.get('area_tag_id', [])}
                        for area in areas_del_rondin
                    ]

                hora_valida = ''
                if bitacora_rondines:
                    fecha_programacion = bitacora_rondines[0].get('fecha_programacion', '')
                    if fecha_programacion:
                        try:
                            hora_valida = str(datetime.strptime(fecha_programacion, '%Y-%m-%d %H:%M:%S').hour)
                        except Exception:
                            try:
                                hora_valida = str(datetime.strptime(fecha_programacion, '%Y-%m-%d %H:%M').hour)
                            except Exception:
                                pass

                areas_formateadas = []

                for area in areas_recorrido:
                    nombre_area = area.get('rondin_area', '')
                    area_tag_id = area.get('area_tag_id', [])
                    area_tag = area_tag_id[0] if area_tag_id else ''

                    estados = []
                    for dia in range(1, days_in_month + 1):
                        estado = self._get_estado_area_dia(
                            bitacora_rondines,
                            area_tag,
                            nombre_area,
                            dia,
                            current_year,
                            current_month,
                            hora_valida
                        )

                        g_id = ""
                        for bitacora in bitacora_rondines:
                            areas_del_rondin = bitacora.get('areas_del_rondin', [])
                            fecha_inicio = bitacora.get('fecha_inicio_rondin', '')

                            if not fecha_inicio:
                                continue

                            try:
                                fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')
                            except Exception:
                                try:
                                    fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M')
                                except Exception:
                                    continue

                            if fecha_bitacora.year != current_year or fecha_bitacora.month != current_month or fecha_bitacora.day != dia:
                                continue

                            for area_check in areas_del_rondin:
                                area_nombre = area_check.get('rondin_area', '')
                                if area_nombre != nombre_area:
                                    continue

                                fecha_check = area_check.get('fecha_hora_inspeccion_area', '')
                                if not fecha_check:
                                    continue

                                url = area_check.get('url_registro_rondin', '')
                                if url:
                                    g_id_part = url.split('detail/')[-1]
                                    g_id = g_id_part.split('?')[0].split('#')[0].strip('/')
                                    break

                            if g_id:
                                break

                        estados.append({
                            "dia": dia,
                            "estado": estado,
                            "record_id": g_id if estado not in ["none", "no_inspeccionada", "no_aplica"] else "",
                        })

                    areas_formateadas.append({
                        "nombre": nombre_area,
                        "estados": estados
                    })

                resumen_estados = []
                for dia in range(1, days_in_month + 1):
                    estado_bitacora, bitacora_id = self._get_estado_bitacora_dia(
                        bitacora_rondines,
                        dia,
                        current_year,
                        current_month,
                        hora_valida
                    )

                    resumen_estados.append({
                        "dia": dia,
                        "estado": estado_bitacora,
                        "record_id": bitacora_id if estado_bitacora not in ["none", "no_aplica"] else "",
                    })

                # Agregar esta categoría al array
                categorias_formateadas.append({
                    "titulo": nombre_recorrido,
                    "areas": areas_formateadas,
                    "resumen": resumen_estados
                })

            # Agregar el item con todas sus categorías
            format_data.append({
                "hora": hora_agrupada,
                "categorias": categorias_formateadas
            })

        return format_data

    def format_bitacoras_mes(self, bitacoras_data, nombre_recorrido):
        if hasattr(self, 'timezone') and self.timezone:
            try:
                tz = pytz.timezone(self.timezone)
                now = datetime.now(tz)
            except Exception:
                now = datetime.now()
        else:
            now = datetime.now()
        current_year = now.year
        current_month = now.month
        days_in_month = calendar.monthrange(current_year, current_month)[1]

        bitacoras_por_dia = {}
        for bitacora in bitacoras_data:
            created_at = bitacora.get('created_at')
            if isinstance(created_at, str):
                try:
                    fecha_bitacora = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        fecha_bitacora = datetime.strptime(created_at.split()[0], '%Y-%m-%d')
                    except Exception:
                        continue
            elif isinstance(created_at, datetime):
                fecha_bitacora = created_at
            else:
                continue

            dia = fecha_bitacora.day
            estatus = bitacora.get('estatus_del_recorrido', '')
            incidencias = bitacora.get('bitacora_rondin_incidencias', [])


            estado = self._mapear_estado_bitacora(estatus, incidencias)

            if dia not in bitacoras_por_dia or bitacoras_por_dia[dia]['created_at'] < created_at:
                bitacoras_por_dia[dia] = {
                    'estado': estado,
                    'created_at': created_at,
                    'record_id': str(bitacora.get('_id', ''))
                }

        estados = []
        for dia in range(1, days_in_month + 1):
            if dia in bitacoras_por_dia:
                estado = bitacoras_por_dia[dia]['estado']
                record_id = bitacoras_por_dia[dia]['record_id']
            else:
                fecha_dia = datetime(current_year, current_month, dia)
                if fecha_dia.date() < now.date():
                    estado = "no_inspeccionada"
                else:
                    estado = "none"
                record_id = ""

            estados.append({
                "dia": dia,
                "estado": estado,
                "record_id": record_id
            })

        hoy = now.day
        estado_dia_actual = estados[hoy - 1] if hoy <= days_in_month else estados[-1]

        format_data = {
            "recorrido": {
                "nombre": nombre_recorrido,
                "estados": estados
            },
            "estadoDia": estado_dia_actual
        }

        return format_data

    def format_check_by_id(self, data: dict, record_id: str):
        """
        Formatea los detalles de un check por su ID de registro.
        Args:
            data (dict): Datos del check.
        Returns:
            dict: Un diccionario con los detalles formateados del check.
        """
        incidencias_area = []
        for incidencia in data.get('incidencias', []):
            nombre_area_incidencia = incidencia.get('nombre_area_salida', '')
            if nombre_area_incidencia != data.get('rondin_area', ''):
                continue
            incidencia_formateada = {
                "fecha_hora_incidente": incidencia.get('fecha_hora_incidente_bitacora', ''),
                "categoria": incidencia.get('categoria', 'General'),
                "subcategoria": incidencia.get('sub_categoria', 'General'),
                "incidente": incidencia.get('incidencia', incidencia.get('incidente_open', '')),
                "accion_tomada": incidencia.get('incidente_accion', ''),
                "comentarios": incidencia.get('comentario_incidente_bitacora', ''),
                "evidencias": incidencia.get('incidente_evidencia', []),
                "documentos": incidencia.get('incidente_documento', []),
            }
            incidencias_area.append(incidencia_formateada)

        checks_mes = self.get_rondin_checks(data.get('rondin_area', ''), data.get('ubicacion', ''), data.get('nombre_recorrido', ''), record_id)

        format_data = {
            'area': data.get('rondin_area', ''),
            'checks_mes': checks_mes,
            'fotos': [{'file_name': item.get('file_name', ''),'file_url': item.get('file_url', '')} for item in data.get('foto_evidencia_area_rondin', [])],
            'hora_de_check': data.get('fecha_hora_inspeccion_area', ''),
            'ubicacion': data.get('ubicacion', ''),
            'tiempo_traslado': data.get('duracion_traslado_area', ''),
            'comentarios': data.get('comentario_area_rondin', ''),
            'incidencias': incidencias_area,
        }
        return format_data

    def format_rondin_checks(self, checks_data, rec_id):
        """
        Formatea los checks del mes en el formato requerido por el frontend.

        Args:
            checks_data (list): Lista de checks del mes con sus incidencias
            area_nombre (str): Nombre del área

        Returns:
            dict: Datos formateados con estructura de estados por día
        """
        # Obtener el mes y año actuales
        if hasattr(self, 'timezone') and self.timezone:
            try:
                tz = pytz.timezone(self.timezone)
                now = datetime.now(tz)
            except Exception:
                now = datetime.now()
        else:
            now = datetime.now()
        current_year = now.year
        current_month = now.month
        days_in_month = calendar.monthrange(current_year, current_month)[1]

        # Crear diccionario para mapear días a lista de checks
        checks_por_dia = {}
        for check in checks_data:
            created_at = check.get('created_at')
            if isinstance(created_at, str):
                try:
                    # Intentar parsear con hora y minuto primero
                    fecha_check = datetime.strptime(created_at, '%Y-%m-%d %H:%M')
                except ValueError:
                    try:
                        # Fallback al formato solo fecha
                        fecha_check = datetime.strptime(created_at, '%Y-%m-%d')
                    except ValueError:
                        continue
            elif isinstance(created_at, datetime):
                fecha_check = created_at
            else:
                continue

            dia = fecha_check.day
            check_area = check.get('check_area', {})
            incidencias = check.get('incidencias', [])

            # Determinar estado del check
            estado = self._get_estado_check(incidencias, self.unlist(check_area.get('rondin_area', '')))

            # Guardar el check
            if dia not in checks_por_dia:
                checks_por_dia[dia] = []

            # Si created_at es datetime, formatearlo a string para consistencia en la respuesta
            created_at_str = created_at
            if isinstance(created_at, datetime):
                created_at_str = created_at.strftime('%Y-%m-%d %H:%M')

            checks_por_dia[dia].append({
                'estado': estado,
                'created_at': created_at_str,
                'record_id': str(check.get('_id', ''))
            })

        # Crear lista de estados para todos los días del mes
        estados = []
        for dia in range(1, days_in_month + 1):
            if dia in checks_por_dia:
                # Ordenar por fecha de creación
                checks_dia = sorted(checks_por_dia[dia], key=lambda x: x['created_at'])
                # El estado principal del día es el del último check
                ultimo_check = self.unlist([check for check in checks_dia if check.get('record_id') == rec_id]) or checks_dia[-1]
                estado = ultimo_check['estado']
                record_id = ultimo_check['record_id']
                registros = checks_dia
            else:
                # Determinar si es día pasado, presente o futuro
                fecha_dia = datetime(current_year, current_month, dia)
                if fecha_dia.date() < now.date():
                    estado = "no_inspeccionada"
                else:
                    estado = "none"
                record_id = ""
                registros = []

            estados.append({
                "dia": dia,
                "estado": estado,
                "record_id": record_id,
                "registros": registros
            })

        # Obtener el estado del día actual
        hoy = now.day
        estado_dia_actual = estados[hoy - 1] if hoy <= days_in_month else estados[-1]

        format_data = {
            "area": {
                "nombre": self.unlist(checks_data[0].get('rondin_area', '')),
                "estados": estados
            },
            "estadoDia": estado_dia_actual
        }

        return format_data

    def get_average_rondin_duration(self, location: str, rondin_name: str):
        query = [
            {"$match": {
                "form_id": self.BITACORA_RONDINES,
                "deleted_at": {"$exists": False},
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.f['nombre_del_recorrido']}": rondin_name,
                f"answers.{self.f['duracion_rondin']}": {"$exists": True}
            }},
            {"$group": {
                "_id": None,
                "average_duration": {
                    "$avg": {
                        "$ifNull": [f"$answers.{self.f['duracion_rondin']}", 0]
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "average_duration": {"$round": ["$average_duration", 2]}
            }}
        ]

        response = self.format_cr(self.cr.aggregate(query))
        format_response = 0
        if response:
            format_response = self.unlist(response).get('average_duration', 0)
        return format_response

    def format_bitacora_record(self, record, area_details=False):
            areas = record.get("areas", [])
            if not isinstance(areas, list):
                areas = [areas] if areas else []
            areas_formateadas = []
            areas_default_images = {}
            areas_config = self.unlist(record.get('recorrido_config', []))
            if areas_config:
                areas_config = areas_config.get('areas_config', {})
                areas_default_images = {i.get('rondin_area', ''): i.get('foto_area', {}) for i in areas_config if i.get('foto_area')}
            for area in areas:
                area_con_contexto = {
                    **area,
                    "ubicacion": record.get("ubicacion", ""),
                    "nombre_recorrido": record.get("nombre_recorrido", ""),
                    "incidencias": record.get("incidencias", []),
                }
                record_id = str(area.get("url_registro_rondin", ""))
                detalle = self.format_check_by_id(area_con_contexto, record_id)
                if area_details:
                    detalle = self.get_area_images([detalle], location=record.get("ubicacion", ""))
                    detalle = detalle[0] if detalle else detalle
                areas_formateadas.append({
                    "area": area.get("rondin_area", ""),
                    "foto_default_area": self.unlist(areas_default_images.get(area.get('rondin_area', ''))),
                    "detalle": detalle,
                })

            incidencias = record.get("incidencias", [])
            if not isinstance(incidencias, list):
                incidencias = [incidencias] if incidencias else []

            incidencias_formateadas = []
            for inc in incidencias:
                incidencias_formateadas.append({
                    "categoria": inc.get("categoria", ""),
                    "subcategoria": inc.get("sub_categoria", ""),
                    "incidente": inc.get("incidencia", ""),
                    "area_incidente": inc.get("nombre_area_salida", ""),
                    "fecha_hora_incidente": inc.get("fecha_hora_incidente_bitacora", ""),
                    "accion_tomada": inc.get("incidente_accion", ""),
                    "comentarios": inc.get("comentario_incidente_bitacora", ""),
                    "evidencias": inc.get("incidente_evidencia", []),
                    "documentos": inc.get("incidente_documento", []),
                })

            recorrido_config = record.get("recorrido_config", [])
            areas_config = recorrido_config[0].get("areas_config", []) if recorrido_config else []

            map_data = []
            for area_conf in areas_config:
                nombre = area_conf.get("rondin_area", "")
                tag_ids = area_conf.get("area_tag_id", [])
                geo_list = area_conf.get("geolocalizacion_area_ubicacion", [])
                foto_area = area_conf.get("foto_area", [])
                geo = geo_list[0] if geo_list else {}
                area_id = tag_ids[0] if tag_ids else nombre
                map_data.append({
                    "id": area_id,
                    "nombre_area": nombre,
                    "geolocation_area": {
                        "latitude": geo.get("latitude", 0),
                        "longitude": geo.get("longitude", 0),
                    },
                    "foto_area": foto_area,
                })

            images_data = []
            for area in areas_formateadas:
                fotos = area.get("detalle", {}).get("fotos", [])
                for foto in fotos:
                    images_data.append({
                        "id": area.get("area", ""),
                        "nombre_area": area.get("area", ""),
                        "foto_area": foto.get("file_url", ""),
                    })

            format_checks_data = []
            checks_data = record.get('checks_data', [])
            for check in checks_data:
                new_item = {}
                new_item['fecha_check'] = check.get('fecha_hora_inspeccion_area', '')
                new_item['evidencias_check'] = check.get('foto_evidencia_area', [])
                new_item['comentarios_check'] = check.get('comentario_check_area', '')
                new_item['incidencias_check'] = check.get('grupo_incidencias_check', '')
                format_checks_data.append(new_item)

            return {
                "id": str(record.get("_id", "")),
                "folio": record.get("folio", ""),
                "created_at": str(record.get("created_at", "")),
                "updated_at": str(record.get("updated_at", "")),
                "ubicacion": record.get("ubicacion", ""),
                "nombre_recorrido": record.get("nombre_recorrido", ""),
                "asignado_a": record.get("asignado_a", ""),
                "tipo_rondin": record.get("tipo_rondin", ""),
                "fecha_hora_programada_inicio": record.get("fecha_hora_programada_inicio", ""),
                "fecha_hora_inicio": record.get("fecha_hora_inicio", ""),
                "fecha_hora_fin": record.get("fecha_hora_fin", ""),
                "estatus_recorrido": record.get("estatus_recorrido", ""),
                "duracion_rondin": record.get("duracion_rondin", ""),
                "motivo_cancelacion": record.get("motivo_cancelacion", ""),
                "comentario_general": record.get("comentario_general", ""),
                "comentarios_generales": record.get("comentarios_generales", []),
                "porcentaje_avance": record.get("porcentaje_avance", 0),
                "cantidad_areas_inspeccionadas": record.get("cantidad_areas_inspeccionadas", 0),
                "total_checks": len(areas),
                "areas": areas_formateadas,
                "incidencias": incidencias_formateadas,
                "images_data": images_data,
                "map_data": map_data,
                "checks_data": format_checks_data
            }

    def get_bitacora(self, date_from=None, date_to=None, area_details=False, limit: int = 15, offset: int = 0, ubicacion: str = "", nombre_rondin: str = ""):
        from datetime import datetime
        año = datetime.now().year

        match_filters = {
            "deleted_at": {"$exists": False},
            "form_id": self.BITACORA_RONDINES,
        }

        if date_from and date_to:
            match_filters["created_at"] = {
                "$gte": date_from,
                "$lte": date_to
            }
        elif date_from:
            match_filters["created_at"] = {"$gte": date_from}
        elif date_to:
            match_filters["created_at"] = {"$lte": date_to}
        else:
            match_filters["$expr"] = {
                "$eq": [{"$year": "$created_at"}, año]
            }

        if ubicacion:
            match_filters[f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}"] = ubicacion
        if nombre_rondin:
            match_filters[f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}"] = nombre_rondin

        query = [
            {"$match": match_filters},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "created_at": 1,
                "updated_at": 1,
                "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                "recorrido_id": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}._id",  # ← id del recorrido
                "asignado_a": f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['nombre_usuario']}",
                "tipo_rondin": f"$answers.{self.f['tipo_rondin']}",
                "fecha_hora_programada_inicio": f"$answers.{self.f['fecha_hora_programada_inicio']}",
                "fecha_hora_inicio": f"$answers.{self.f['fecha_hora_inicio']}",
                "fecha_hora_fin": f"$answers.{self.f['fecha_hora_fin']}",
                "estatus_recorrido": f"$answers.{self.f['estatus_recorrido']}",
                "duracion_rondin": f"$answers.{self.f['duracion_rondin']}",
                "motivo_cancelacion": f"$answers.{self.f['motivo_cancelacion']}",
                "comentario_general": f"$answers.{self.f['comentario_general']}",
                "comentarios_generales": f"$answers.{self.f['comentarios_generales']}",
                "porcentaje_avance": f"$answers.{self.f['porcentaje_avance']}",
                "cantidad_areas_inspeccionadas": f"$answers.{self.f['cantidad_areas_inspeccionadas']}",
                "areas": f"$answers.{self.f['areas']}",
                "incidencias": f"$answers.{self.f['bitacora_rondin_incidencias']}",
            }},
            {"$lookup": {
                "from": self.cr.name,
                "let": { "nombre_rec": "$nombre_recorrido", "ubicacion_rec": "$ubicacion" },
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$form_id", self.CONFIGURACION_DE_RECORRIDOS_FORM]},
                                {"$eq": [f"$answers.{self.rondin_keys['nombre_rondin']}", "$$nombre_rec"]},
                                {"$eq": [f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}", "$$ubicacion_rec"]},
                                {"$not": {"$ifNull": ["$deleted_at", False]}}
                            ]
                        }
                    }},
                    {"$project": {
                        "_id": 0,
                        "areas_config": f"$answers.{self.rondin_keys['areas']}",
                    }},
                    {"$limit": 1}
                ],
                "as": "recorrido_config"
            }},
            {"$addFields": {
                "area_record_ids": {
                    "$filter": {
                        "input": {
                            "$map": {
                                "input": {"$ifNull": ["$areas", []]},
                                "as": "area",
                                "in": {
                                    "$convert": {
                                        "input": {
                                            "$arrayElemAt": [
                                                {"$split": [{"$ifNull": [f"$$area.{self.f['url_registro_rondin']}", ""]}, "/"]},
                                                -1
                                            ]
                                        },
                                        "to": "objectId",
                                        "onError": None,
                                        "onNull": None
                                    }
                                }
                            }
                        },
                        "as": "oid",
                        "cond": {"$ne": ["$$oid", None]}
                    }
                }
            }},
            {"$lookup": {
                "from": self.cr.name,
                "let": {"record_ids": "$area_record_ids"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$form_id", self.CHECK_UBICACIONES]},
                                {"$in": ["$_id", "$$record_ids"]}
                            ]
                        }
                    }},
                    {"$project": {
                        "_id": 1,
                        "answers": 1,
                    }}
                ],
                "as": "checks_data"
            }},
            {"$unset": "area_record_ids"},
            {"$sort": {"created_at": -1}},
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        result = [self.format_bitacora_record(record, area_details) for record in response]
        # print("RESPUESTA DEL SERVICIO", simplejson.dumps(result, indent=4))
        return {"data": result, "total": len(result)}

    def get_recorridos(self, date_from=None, date_to=None, area_details=False, limit=20, offset=0):
        """Lista los rondines según los filtros proporcionados.
        Params:
            date_from (str): Fecha de inicio del filtro.
            date_to (str): Fecha de fin del filtro.
            limit (int): Número máximo de rondines a devolver.
            offset (int): Número de rondines a omitir desde el inicio.
        Returns:
            list: Lista de rondines con sus detalles.
        """
        match = {
            "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
            "deleted_at": {"$exists": False},
            f"answers.{self.f['status_cron']}":{'$ne':'eliminado'}
        }

        if date_from:
            match.update({
                "created_at": {"$gte": date_from}
            })
        if date_to:
            match.update({
                "created_at": {"$lte": date_to}
            })

        query = [
            {"$match": match},
            {"$project": {
                "_id": 1,
                "folio":1,
                "accion_recurrencia": f"$answers.{self.rondin_keys['accion_recurrencia']}",
                "areas": f"$answers.{self.rondin_keys['areas']}",
                "areas_name": f"$answers.{self.rondin_keys['areas']}.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area']}",  
                "cantidad_de_puntos": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},
                "cada_cuantas_horas_se_repite": f"$answers.{self.rondin_keys['cada_cuantas_horas_se_repite']}",
                "checkpoints": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},  
                "cron_id": f"$answers.{self.rondin_keys['cron_id']}",
                "dag_id": {"$ifNull": [f"$answers.{self.rondin_keys['dag_id']}", ""]},
                "duracion_estimada": f"$answers.{self.rondin_keys['duracion_estimada']}",  
                "duracion_esperada_rondin": {"$ifNull": [f"$answers.{self.rondin_keys['duracion_estimada']}", "No especificada"]},
                "empleados_asignado": {
                    "$map": {
                        "input": {"$ifNull": [f"$answers.{self.rondin_keys['grupo_asignado_a']}", []]},
                        "as": "emp",
                        "in": f"$$emp.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}"
                    }
                },
                "en_que_mes": f"$answers.{self.rondin_keys['en_que_mes']}",
                "en_que_semana_sucede": f"$answers.{self.rondin_keys['en_que_semana_sucede']}",
                "estatus_rondin": f"$answers.{self.f['status_cron']}",
                "fecha1": f"$answers.{self.rondin_keys['fecha1']}",
                "fecha2": f"$answers.{self.rondin_keys['fecha2']}",
                "fecha_final_rondin": {"$ifNull": [f"$answers.{self.f['fecha_final_recurrencia']}", "Sin fecha final"]},
                "fecha_hora_programada": f"$answers.{self.rondin_keys['fecha_hora_programada']}",
                "fecha_inicio_rondin": f"$answers.{self.f['fecha_primer_evento']}",
                "grupo_asignado": {"$ifNull": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['grupo_asignado']}", None]},
                # "grupo_asignado_rondin": f"$answers.{self.rondin_keys['grupo_asignado_rondin']}",
                "id_grupo": {"$arrayElemAt": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['id_grupo']}", 0]},
                "la_recurrencia_cuenta_con_fecha_final": f"$answers.{self.rondin_keys['la_recurrencia_cuenta_con_fecha_final']}",
                "nombre_del_rondin": f"$answers.{self.rondin_keys['nombre_rondin']}",
                "programar_anticipacion": f"$answers.{self.rondin_keys['programar_anticipacion']}",
                "que_dias_de_la_semana": f"$answers.{self.rondin_keys['que_dias_de_la_semana']}",
                "recurrencia": {"$ifNull": [f"$answers.{self.rondin_keys['la_tarea_es_de']}", 'No Recurrente']},
                "se_repite_cada": f"$answers.{self.rondin_keys['se_repite_cada']}",
                "sucede_cada": f"$answers.{self.rondin_keys['sucede_cada']}",
                "sucede_recurrencia": f"$answers.{self.rondin_keys['sucede_recurrencia']}",
                "tiempo_para_ejecutar_tarea": f"$answers.{self.rondin_keys['tiempo_para_ejecutar_tarea']}",
                "tiempo_para_ejecutar_tarea_expresado_en": f"$answers.{self.rondin_keys['tiempo_para_ejecutar_tarea_expresado_en']}",
                "tipo_asignacion": f"$answers.{self.rondin_keys['tipo_asignacion']}",
                "tipo_rondin": {"$ifNull": [f"$answers.{self.rondin_keys['tipo_rondin']}", "qr"]},
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "ubicacion_area": f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.Location.f['area_salida']}",
                "ubicacion_geolocation": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.f['address_geolocation']}",
                "area": f"$answers.{self.f['area']}", 
                "cada_cuantos_dias_se_repite": f"$answers.{self.rondin_keys['cada_cuantos_dias_se_repite']}", 
                "roles": f"$answers.{self.f['grupo_roles']}", 
            }},
            {"$sort": {"_id": -1}}, 
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []

        if response:
            for item in response:
                data = self.format_rondin_by_id(item)
                location = item.get('ubicacion', '')
                rondin_name = item.get('nombre_del_rondin', '')

                duracion_promedio = self.get_average_rondin_duration(
                    location=location,
                    rondin_name=rondin_name
                )
                roles_raw = item.get('roles', [])
                data['roles'] = [r.get('rol') for r in roles_raw if r.get('rol')]
                data['duracion_promedio'] = duracion_promedio
                if area_details:
                    data['areas'] = self.get_area_images(
                        data.get('areas', []),
                        location=data.get('ubicacion')
                    )

                format_response.append(data)
        # format_response = []
        # if response:
        #     for item in response:
        #         item['recurrencia'] = item['recurrencia'].replace('_', ' ').title() if item.get('recurrencia') else 'No Recurrente'
        #         format_response.append(item)
        #         if area_details:
        #             item['areas']  = self.get_area_images(item['areas'], location=item['ubicacion'])
        print(simplejson.dumps(format_response, indent=4))
        return format_response

    def get_rondin_by_id(self, record_id: str):
        """Obtiene los detalles de un rondin por su ID de registro.
        Args:
            record_id (str): El ID del registro del rondin.
        Returns:
            dict: Un diccionario con los detalles del rondin.
        Raises:
            Exception: Si el ID del registro no es proporcionado.
        """
        if not record_id:
            raise Exception("Record ID is required to get rondin details.")

        query = [
            {"$match": {
                "_id": ObjectId(record_id),
                "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
                "deleted_at": {"$exists": False}
            }},
            {"$project": {
                "_id": 0,
                "folio": 1,
                "accion_recurrencia": f"$answers.{self.rondin_keys['accion_recurrencia']}",
                "areas": f"$answers.{self.rondin_keys['areas']}",
                "cada_cuantas_horas_se_repite": f"$answers.{self.rondin_keys['cada_cuantas_horas_se_repite']}",
                "cantidad_de_puntos": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},
                "cron_id": f"$answers.{self.rondin_keys['cron_id']}",
                "dag_id": f"$answers.{self.rondin_keys['dag_id']}",
                "duracion_esperada_rondin": {"$ifNull": [f"$answers.{self.rondin_keys['duracion_estimada']}", "No especificada"]},
                "empleados_asignado": {
                    "$map": {
                        "input": {"$ifNull": [f"$answers.{self.rondin_keys['grupo_asignado_a']}", []]},
                        "as": "emp",
                        "in": f"$$emp.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}"
                    }
                },                
                "en_que_mes": f"$answers.{self.rondin_keys['en_que_mes']}",
                "en_que_semana_sucede": f"$answers.{self.rondin_keys['en_que_semana_sucede']}",
                "estatus_rondin": f"$answers.{self.f['status_cron']}",
                "fecha1": f"$answers.{self.rondin_keys['fecha1']}",
                "fecha2": f"$answers.{self.rondin_keys['fecha2']}",
                "fecha_final_rondin": {"$ifNull": [f"$answers.{self.f['fecha_final_recurrencia']}", "Sin fecha final"]},
                "fecha_hora_programada": f"$answers.{self.rondin_keys['fecha_hora_programada']}",
                "fecha_inicio_rondin": f"$answers.{self.f['fecha_primer_evento']}",
                "id_grupo": {"$arrayElemAt": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['id_grupo']}", 0]},
                "grupo_asignado": {"$ifNull": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['grupo_asignado']}",None]},
                "la_recurrencia_cuenta_con_fecha_final": f"$answers.{self.rondin_keys['la_recurrencia_cuenta_con_fecha_final']}",
                "nombre_del_rondin": f"$answers.{self.rondin_keys['nombre_rondin']}",
                "programar_anticipacion": f"$answers.{self.rondin_keys['programar_anticipacion']}",
                "que_dias_de_la_semana": f"$answers.{self.rondin_keys['que_dias_de_la_semana']}",
                "recurrencia": {"$ifNull": [f"$answers.{self.rondin_keys['la_tarea_es_de']}", 'No Recurrente']},
                "se_repite_cada": f"$answers.{self.rondin_keys['se_repite_cada']}",
                "sucede_cada": f"$answers.{self.rondin_keys['sucede_cada']}",
                "sucede_recurrencia": f"$answers.{self.rondin_keys['sucede_recurrencia']}",
                "tiempo_para_ejecutar_tarea": f"$answers.{self.rondin_keys['tiempo_para_ejecutar_tarea']}",
                "tiempo_para_ejecutar_tarea_expresado_en": f"$answers.{self.rondin_keys['tiempo_para_ejecutar_tarea_expresado_en']}",
                "tipo_asignacion": f"$answers.{self.rondin_keys['tipo_asignacion']}",
                "tipo_rondin": {"$ifNull": [f"$answers.{self.rondin_keys['tipo_rondin']}", "qr"]},
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "ubicacion_area": f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.Location.f['area_salida']}",
                "ubicacion_geolocation": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.f['address_geolocation']}",
            }},
        ]

        response = self.format_cr(self.cr.aggregate(query))
        response = self.unlist(response)
        format_response = {}
        if response:
            # format_response['tipo_asignacion'] = response.get('tipo_asignacion', '')
            print(simplejson.dumps(response, indent=4))
            format_response = self.format_rondin_by_id(response)
            # print("ALLALALALALALA",simplejson.dumps(response, indent=4))
            location = response.get('ubicacion', '')
            rondin_name = response.get('nombre_del_rondin', '')
            duracion_promedio = self.get_average_rondin_duration(location=location, rondin_name=rondin_name)
            format_response['duracion_promedio'] = duracion_promedio
        # print(simplejson.dumps(format_response, indent=4))
        return format_response

    def get_ubicacion_geolocation(self, location: str):
        """
        Obtiene la geolocalización de una ubicación específica.
        Args:
            location (str): El nombre de la ubicación.
        Returns:
            dict: Un diccionario con la ubicación y su geolocalización.
        """
        query = [
            {"$match": {
                "form_id": self.Location.UBICACIONES,
                "deleted_at": {"$exists": False},
                f"answers.{self.Location.f['location']}": location,
            }},
            {"$project": {
                "_id": 0,
                "location": f"$answers.{self.Location.f['location']}",
                "geolocation": f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.f['address_geolocation']}",
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        response = self.unlist(response)
        return response

    def get_areas_details(self, areas_list: list):
        """
        Obtiene los detalles necesarios de las áreas proporcionadas.
        Args:
            areas_list (list): Lista de áreas.
        Returns:
            list: Lista de áreas con su geolocalización y foto.
        """
        query = [
            {"$match": {
                "form_id": self.Location.AREAS_DE_LAS_UBICACIONES,
                "deleted_at": {"$exists": False},
                f"answers.{self.Location.f['area']}": {"$in": areas_list},
            }},
            {"$project": {
                "_id": 0,
                "folio": 1,
                "area": f"$answers.{self.Location.f['area']}",
                "geolocation": f"$answers.{self.f['geolocalizacion_area_ubicacion']}",
                "image": f"$answers.{self.f['foto_area']}",
                "tag_id": f"$answers.{self.f['area_tag_id']}",
                "tipo_de_area": f"$answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_de_area']}",
                "area_state": f"$answers.{self.Location.f['area_state']}",
                "area_status": f"$answers.{self.Location.f['area_status']}",
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        return response

    def get_catalog_areas(self, ubicacion=""):
        #Obtener areas disponibles para rondin
        if ubicacion:
            query = [
                {"$match": {
                    "form_id": self.AREAS_DE_LAS_UBICACIONES,
                    "deleted_at": {"$exists": False},
                    f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}": ubicacion,
                    f"answers.{self.f['area_tag_id']}": {"$exists": True}
                }},
                {"$project": {
                    "_id": f"$answers.{self.mf['nombre_area']}",
                }}
            ]
            data = self.format_cr(self.cr.aggregate(query))
            data = [item.get('_id') for item in data]
            format_data = list(set(data))
            return format_data
        else:
            raise Exception("Ubicacion is required.")

    def catalago_grupos_recorridos(self):
        catalog_id = self.GRUPOS_CAT_ID
        form_id = self.CONFIGURACION_RECORRIDOS_FORM
        return self.catalogo_view(catalog_id, form_id)

    def catalogo_inspecciones(self): 
        catalog_id = self.CATALOGO_FORMAS_CAT_ID
        form_id = self.CONFIGURACION_RECORRIDOS_FORM
        return self.catalogo_view(catalog_id, form_id)

    def get_catalog_areas_formatted(self, ubicacion=""):
        #Obtener areas disponibles para rondin
        if ubicacion:
            options = {
                'startkey': [ubicacion],
                'endkey': [f"{ubicacion}\n",{}],
                'group_level':2
            }

            catalog_id = self.AREAS_DE_LAS_UBICACIONES_CAT_ID
            form_id = self.CONFIGURACION_RECORRIDOS_FORM
            areas = self.catalogo_view(catalog_id, form_id, options)
            response = self.get_areas_details(areas)
            areas_formateadas = []
            for r in response:
                areas_formateadas.append({
                    "folio": r.get("folio", ""),
                    "rondin_area": r.get("area", ""),
                    "geolocalizacion_area_ubicacion": [
                        {
                            "latitude": r.get("latitude", 0.0),
                            "longitude": r.get("longitude", 0.0)
                        }
                    ],
                    "area_tag_id": [r.get("tag_id", "")],
                    "foto_area": r.get("image", []),
                    "tipo_de_area": r.get("tipo_de_area", ""),
                    "area_state": r.get("area_state", ""),
                    "area_status": r.get("area_status", ""),
                })
            print("RESPONSE",simplejson.dumps(areas_formateadas, indent=3))
            return areas_formateadas
        else:
            raise Exception("Ubicacion is required.")

    def get_incidencias_rondines(self, location=None, area=None, date_from=None, date_to=None, limit=20, offset=0):
        """Lista las incidencias de los rondines según los filtros proporcionados.
        Params:
            date_from (str): Fecha de inicio del filtro.
            date_to (str): Fecha de fin del filtro.
            limit (int): Número máximo de incidencias a devolver.
            offset (int): Número de incidencias a omitir desde el inicio.
        Returns:
            list: Lista de incidencias con sus detalles.
        """
        match = {
            "form_id": self.BITACORA_RONDINES,
            "deleted_at": {"$exists": False},
            f"answers.{self.f['bitacora_rondin_incidencias']}": {
                "$type": "array",
                "$not": {"$size": 0}
            }
        }

        if location:
            match.update({
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location
            })
        if date_from:
            match.update({
                "created_at": {"$gte": date_from}
            })
        if date_to:
            match.update({
                "created_at": {"$lte": date_to}
            })

        query = [
            {"$match": match},
            {"$sort": {"created_at": -1}},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                "incidencias_rondin": f"$answers.{self.f['bitacora_rondin_incidencias']}",
                "link": f"$answers.{self.rondin_keys['link']}",
            }},
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            format_response = self.format_incidencias_rondines(response, area)
        return format_response

    def get_rondines_images(self, location=None, areas=None, date_from=None, date_to=None, limit=20, offset=0):
        """Lista las imágenes de los rondines según los filtros proporcionados.
        Params:
            date_from (str): Fecha de inicio del filtro.
            date_to (str): Fecha de fin del filtro.
            limit (int): Número máximo de imágenes a devolver.
            offset (int): Número de imágenes a omitir desde el inicio.
        Returns:
            list: Lista de imágenes con sus detalles.
        """
        match = {
            "form_id": self.BITACORA_RONDINES,
            "deleted_at": {"$exists": False},
            f"answers.{self.f['areas_del_rondin']}": {
                "$type": "array",
                "$not": {"$size": 0}
            }
        }

        unwind_match = {
            f"answers.{self.f['areas_del_rondin']}.{self.f['foto_evidencia_area_rondin']}": {
                "$exists": True,
                "$not": {"$size": 0}
            }
        }

        if location:
            match.update({
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location
            })
        if areas:
            unwind_match.update({
                f"answers.{self.f['areas_del_rondin']}.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['area']}": {"$in": areas}
            })
        if date_from:
            match.update({
                "created_at": {"$gte": date_from}
            })
        if date_to:
            match.update({
                "created_at": {"$lte": date_to}
            })

        query = [
            {"$match": match},
            {"$sort": {"created_at": -1}},
            {'$unwind': f"$answers.{self.f['areas_del_rondin']}"},
            {"$match": unwind_match},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "areas_recorrido": f"$answers.{self.f['areas_del_rondin']}",
                "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
            }},
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            format_response = self.format_rondines_images(response)
        print("format_response", simplejson.dumps(format_response, indent=4))
        return format_response

    def get_bitacora_rondines(self, location=None, nombre_rondin=None, year=None, month=None):
        year_condition = { "$eq": [ { "$year": "$created_at" }, { "$year": "$$NOW" } ] }
        month_condition = { "$eq": [ { "$month": "$created_at" }, { "$month": "$$NOW" } ] }

        if year:
            year_condition = { "$eq": [ { "$year": "$created_at" }, int(year) ] }
        if month:
            month_condition = { "$eq": [ { "$month": "$created_at" }, int(month) ] }

        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.BITACORA_RONDINES,
            #! TEST PURPOSES
            # f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}": {"$in": ["Recorrido cada 4 horas", "Recorrido 1 vez al Mes"]},
            # f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}": {"$in": ["Recorrido cada 4 horas"]},
            f"answers.{self.f['fecha_programacion']}": {"$type": "string", "$ne": ""},
            "$expr": {
                "$and": [
                    year_condition,
                    month_condition
                ]
            }
        }

        if nombre_rondin:
            match.update({
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}": nombre_rondin,
            })
        if location:
            match.update({
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location,
            })

        query = [
            {"$match": match},
            {"$project": {
                "_id": 1,
                "answers": 1,
                "hora_agrupada": {
                    "$hour": {
                        "$dateFromString": {
                            "dateString": f"$answers.{self.f['fecha_programacion']}",
                            "format": "%Y-%m-%d %H:%M:%S",
                            "onError": None
                        }
                    }
                },
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                "recorridos": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}",
                "bitacora_rondines": "$answers"
            }},
            {"$group": {
                "_id": {
                    "hora": "$hora_agrupada",
                    "nombre_recorrido": "$nombre_recorrido"
                },
                "recorridos": {"$first": "$recorridos"},
                "bitacora_rondines": {
                    "$push": {
                        "_id": "$_id",
                        "answers": "$answers"
                    }
                }
            }},
            {"$group": {
                "_id": "$_id.hora",
                "categorias": {
                    "$push": {
                        "nombre_recorrido": "$_id.nombre_recorrido",
                        "recorridos": "$recorridos",
                        "bitacora_rondines": "$bitacora_rondines"
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "hora_agrupada": {
                    "$concat": [
                        {"$cond": [{"$lt": ["$_id", 10]}, "0", ""]},
                        {"$toString": "$_id"},
                        ":00"
                    ]
                },
                "categorias": 1
            }},
            {"$sort": {"hora_agrupada": 1}}
        ]

        response = self.format_cr(self.cr.aggregate(query))
        format_resp = []
        if response:
            format_resp = self.format_bitacora_rondines(response)
        return format_resp

    def get_check_by_id(self, record_id: str):
        """
        Obtiene los detalles de un check por su ID de registro.
        Args:
            record_id (str): El ID del registro del check.
        Returns:
            dict: Un diccionario con los detalles del check.
        Raises:
            Exception: Si el ID del registro no es proporcionado.
        """
        if not record_id:
            return self.LKFException({'title': 'Advertencia', 'msg': 'El ID del registro no fue proporcionado.'})

        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                "$expr": {
                    "$and": [
                        { "$eq": [ { "$year": "$created_at" }, { "$year": "$$NOW" } ] }, #TODO: Cambiar a mes por parametro
                        { "$eq": [ { "$month": "$created_at" }, { "$month": "$$NOW" } ] }, #TODO: Cambiar a mes por parametro
                        {"$anyElementTrue": {
                            "$map": {
                                "input": {
                                    "$ifNull": [f"$answers.{self.f['areas_del_rondin']}", []]
                                },
                                "as": "check",
                                "in": {
                                    "$regexMatch": {
                                        "input": f"$$check.{self.f['url_registro_rondin']}",
                                        "regex": record_id
                                    }
                                }
                            }
                        }}
                    ]
                }
            }},
            {"$unwind": f"$answers.{self.f['areas_del_rondin']}"},
            {"$match": {
                "$expr": {
                    "$regexMatch": {
                        "input": f"$answers.{self.f['areas_del_rondin']}.{self.f['url_registro_rondin']}",
                        "regex": record_id
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                "area": f"$answers.{self.f['areas_del_rondin']}",
                "incidencias": f"$answers.{self.f['bitacora_rondin_incidencias']}",
            }}
        ]

        response = self.format_cr(self.cr.aggregate(query))
        response = self.unlist(response)
        format_response = {}
        if response:
            format_response = self.format_check_by_id(response, record_id)
        return format_response

    def get_all_checks(self, ubicacion: str = "", nombre_rondin: str = ""):
        from datetime import datetime
        año = datetime.now().year
        match_filters = {
            "deleted_at": {"$exists": False},
            "form_id": self.CHECK_UBICACIONES,
            "$expr": {
                "$eq": [{"$year": "$created_at"}, año]
            }
        }
        if ubicacion:
            match_filters[f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['location']}"] = ubicacion

        query = [
            {"$match": match_filters},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "created_at": 1,
                "updated_at": 1,
                "foto_evidencia_area": f"$answers.{self.f['foto_evidencia_area']}",
                "grupo_incidencias_check": f"$answers.{self.f['grupo_incidencias_check']}",
                "comentario_check_area": f"$answers.{self.f['comentario_check_area']}",
                "check_status": f"$answers.{self.f['check_status']}",
                "fecha_inspeccion_area": f"$answers.{self.f['fecha_inspeccion_area']}",
                "url_rondin": f"$answers.{self.f['url_rondin']}",
                "rondin_area": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['rondin_area']}",
                "tipo_de_area": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['tipo_de_area']}",
                "incidente_location": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['location']}",
                "area_tag_id": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['area_tag_id']}",
                "foto_area": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['foto_area']}",
                # "area_nombre": f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['area_nombre']}",
            }},
            # Extraer ID del final de la URL
            {"$addFields": {
                "rondin_id_str": {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": ["$url_rondin", None]},
                            {"$ne": ["$url_rondin", ""]},
                        ]},
                        "then": {
                            "$let": {
                                "vars": {
                                    "match": {
                                        "$regexFind": {
                                            "input": "$url_rondin",
                                            "regex": r"([a-f0-9]{24})$"
                                        }
                                    }
                                },
                                "in": "$$match.match"
                            }
                        },
                        "else": None
                    }
                }
            }},
            {"$addFields": {
                "rondin_object_id": {
                    "$cond": {
                        "if": {"$ne": ["$rondin_id_str", None]},
                        "then": {"$toObjectId": "$rondin_id_str"},
                        "else": None
                    }
                }
            }},
            # Lookup a BITACORA_RONDINES
            {"$lookup": {
                "from": self.cr.name,
                "let": {"rondin_oid": "$rondin_object_id"},
                "pipeline": [
                    {"$match": {"$expr": {
                        "$and": [
                            {"$eq": ["$_id", "$$rondin_oid"]},
                            {"$ne": ["$$rondin_oid", None]}
                        ]
                    }}},
                    {"$project": {
                        "_id": 1,
                        "folio": 1,
                        "form_id": 1,  # agrega esto para ver qué forma es
                        "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                        "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                        "asignado_a": f"$answers.{self.f['asignado_a']}",
                        "tipo_rondin": f"$answers.{self.f['tipo_rondin']}",
                        "fecha_hora_programada_inicio": f"$answers.{self.f['fecha_hora_programada_inicio']}",
                        "fecha_hora_inicio": f"$answers.{self.f['fecha_hora_inicio']}",
                        "estatus_recorrido": f"$answers.{self.f['estatus_recorrido']}",
                        "duracion_rondin": f"$answers.{self.f['duracion_rondin']}",
                        "comentario_general": f"$answers.{self.f['comentario_general']}",
                        "porcentaje_avance": f"$answers.{self.f['porcentaje_avance']}",
                        "cantidad_areas_inspeccionadas": f"$answers.{self.f['cantidad_areas_inspeccionadas']}",
                    }}
                ],
                "as": "rondin_info"
            }},
            {"$addFields": {
                "rondin": {"$arrayElemAt": ["$rondin_info", 0]}
            }},
            {"$sort": {"created_at": -1}},
            {"$limit": 100}
        ]

        response = self.format_cr(self.cr.aggregate(query))
        result = []

        for record in response:
            rondin = self.unlist(record.get("rondin_info")) or {}
            result.append({
                "id": str(record.get("_id", "")),
                "folio": record.get("folio", ""),
                "created_at": str(record.get("created_at", "")),
                "updated_at": str(record.get("updated_at", "")),
                "url_rondin": record.get("url_rondin", ""),
                # info del area
                # "nombre_area": record.get("nombre_area", []),
                "rondin_area": record.get("rondin_area", []),
                "area_tag_id": record.get("area_tag_id", ""),
                "tipo_de_area": record.get("tipo_de_area", []),
                "incidente_location": record.get("incidente_location", []),
                # check
                "check_status": record.get("check_status", ""),
                "comentario_check_area": record.get("comentario_check_area", ""),
                "foto_evidencia_area": record.get("foto_evidencia_area", []),
                "foto_area": record.get("foto_area", []),
                "fecha_inspeccion_area": record.get("fecha_inspeccion_area", ""),
                "grupo_incidencias_check": record.get("grupo_incidencias_check", []),
                # info de la bitacora del rondin
                "rondin": {
                    "id": str(rondin.get("_id", "")),
                    "folio": rondin.get("folio", ""),
                    "ubicacion": rondin.get("ubicacion", ""),
                    "nombre_recorrido": rondin.get("nombre_recorrido", ""),
                    "asignado_a": rondin.get("nombre_emp", ""),
                    "tipo_rondin": rondin.get("tipo_rondin", ""),
                    "fecha_hora_programada_inicio": rondin.get("fecha_hora_programada_inicio", ""),
                    "fecha_hora_inicio": rondin.get("fecha_hora_inicio", ""),
                    "estatus_recorrido": rondin.get("estatus_recorrido", ""),
                    "duracion_rondin": rondin.get("duracion_rondin", ""),
                    "comentario_general": rondin.get("comentario_general", ""),
                    "porcentaje_avance": rondin.get("porcentaje_avance", ""),
                    "cantidad_areas_inspeccionadas": rondin.get("cantidad_areas_inspeccionadas", ""),
                } if rondin else {}
            })

        print(simplejson.dumps(result, indent=4, default=str))
        return {"data": result, "total": len(result)}

    def get_bitacora_by_id(self, record_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                "_id": ObjectId(record_id),
            }},
            {"$project": {
                "_id": 0,
                "answers": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = {}
        if response:
            response = self.unlist(response)
            bitacoras_mes = self.get_bitacoras_mes(response.get('incidente_location', ''), response.get('nombre_del_recorrido', ''))
            format_response.update({
                "bitacoras_mes": bitacoras_mes,
                "fecha_hora_programada": response.get('fecha_programacion', ''),
                "fecha_inicio": response.get('fecha_inicio_rondin', ''),
                "fecha_fin": response.get('fecha_fin_rondin', ''),
                "duracion": response.get('duracion_rondin', ''),
                "estatus": response.get('estatus_del_recorrido', ''),
                "recurrencia": response.get('fecha_programacion', ''),
                "areas_a_inspeccionar": response.get('areas_del_rondin', []),
                "incidencias": response.get('bitacora_rondin_incidencias', []),
            })
        return format_response

    def get_bitacoras_mes(self, location, nombre_recorrido):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}": nombre_recorrido,
                "$expr": {
                    "$and": [
                        {"$eq": [{"$year": "$created_at"}, {"$year": "$$NOW"}]},
                        {"$eq": [{"$month": "$created_at"}, {"$month": "$$NOW"}]}
                    ]
                }
            }},
            {"$project": {
                "_id": 1,
                "answers": 1,
                "created_at": 1,
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            format_response = self.format_bitacoras_mes(response, nombre_recorrido)
        return format_response

    def get_rondin_checks(self, area, location, nombre_recorrido, record_id):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CHECK_UBICACIONES,
                f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['area']}": area,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}": nombre_recorrido,
                "$expr": {
                    "$and": [
                        {"$eq": [{"$year": "$created_at"}, {"$year": "$$NOW"}]},
                        {"$eq": [{"$month": "$created_at"}, {"$month": "$$NOW"}]}
                    ]
                }
            }},
            {"$project": {
                "_id": 1,
                "answers": 1,
                "created_at": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M",
                        "date": "$created_at",
                        "timezone": "America/Mexico_City"
                    }
                }
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            format_response = self.format_rondin_checks(response, record_id)
        return format_response

    def _get_estado_check(self, incidencias, area_nombre):
        """
        Determina el estado de un check según su información.

        Args:
            check_area (dict): Información del área visitada en el check
            incidencias (list): Lista de incidencias de la bitácora
            area_nombre (str): Nombre del área a evaluar

        Returns:
            str: Estado del check ("finalizado", "incidencias", etc.)
        """
        # Verificar si hay incidencias para esta área
        tiene_incidencias = False
        for incidencia in incidencias:
            nombre_area_incidencia = incidencia.get('nombre_area_salida', '')
            if nombre_area_incidencia == area_nombre:
                tiene_incidencias = True
                break

        if tiene_incidencias:
            return "incidencias"

        # Si no hay incidencias, el check está finalizado
        return "finalizado"

    def _get_estado_bitacora_dia(self, bitacora_rondines, dia, year, month, hora_valida):
        """
        Determina el estado de una bitácora en un día específico.

        Returns:
            tuple: (estado, bitacora_id)
                - "finalizado": Bitácora completada
                - "incidencias": Bitácora con incidencias
                - "no_inspeccionada": Día pasado donde debía haber bitácora pero no se hizo
                - "no_aplica": Día pasado donde no estaba programada ninguna bitácora
                - "none": Día futuro o presente sin bitácora
        """
        for bitacora in bitacora_rondines:
            fecha_inicio = bitacora.get('fecha_inicio_rondin', '')
            estatus_bitacora = bitacora.get('estatus_del_recorrido', '')
            incidencias = bitacora.get('bitacora_rondin_incidencias', [])

            bitacora_id = str(bitacora.get('_id', ''))
            areas_del_rondin = bitacora.get('areas_del_rondin', [])
            for area in areas_del_rondin:
                url = area.get('url_registro_rondin', '')
                if url:
                    break

            if not fecha_inicio:
                continue

            try:
                fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')
            except Exception:
                try:
                    fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M')
                except Exception:
                    continue

            if fecha_bitacora.year != year or fecha_bitacora.month != month or fecha_bitacora.day != dia:
                continue

            if hora_valida:
                try:
                    hora_inicio = fecha_bitacora.hour
                    hora_esperada = int(hora_valida)

                    if not (hora_esperada <= hora_inicio <= hora_esperada + 1):
                        continue
                except Exception:
                    pass

            if incidencias and len(incidencias) > 0:
                return ('incidencias', bitacora_id)

            if estatus_bitacora in ['realizado', 'cerrado']:
                return ("finalizado", bitacora_id)
            elif estatus_bitacora == 'cancelado':
                return ("cancelado", bitacora_id)
            else:
                return ("finalizado", bitacora_id)

        # No se encontró bitácora para este día
        if hasattr(self, 'timezone') and self.timezone:
            try:
                tz = pytz.timezone(self.timezone)
                now = datetime.now(tz)
            except Exception:
                now = datetime.now()
        else:
            now = datetime.now()
        fecha_evaluada = datetime(year, month, dia)

        estaba_programada, bitacora_programada = self._verificar_bitacora_programada(dia, year, month, hora_valida, bitacora_rondines)
        if estaba_programada:
            estatus_bitacora_programada = bitacora_programada.get('estatus_del_recorrido', '')
            record_id = str(bitacora_programada.get('_id', ''))
            if estatus_bitacora_programada == 'cancelado':
                return ("cancelado", record_id)
            elif estatus_bitacora_programada == 'programado':
                return ("programado", record_id)
            elif estatus_bitacora_programada == 'realizado':
                return ("fuera_de_hora", record_id)
            else:
                return ("no_inspeccionada", record_id)

        if fecha_evaluada.date() > now.date():
            return ("none", "")
        elif fecha_evaluada.date() == now.date():
            return ("programado", "")
        else:
            return ("no_aplica", "")

    def _get_estado_area_dia(self, bitacora_rondines, area_tag_id, nombre_area, dia, year, month, hora_valida):
        """
        Determina el estado de un área en un día específico.

        Returns:
            str: Estado del área
                - "finalizado": Área visitada
                - "incidencias": Área con incidencias
                - "no_inspeccionada": Día pasado donde debía visitarse pero no se hizo
                - "no_aplica": Día pasado donde no estaba programada la visita
                - "none": Día futuro o presente sin visita
        """
        for bitacora in bitacora_rondines:
            areas_del_rondin = bitacora.get('areas_del_rondin', [])
            incidencias = bitacora.get('bitacora_rondin_incidencias', [])
            fecha_inicio = bitacora.get('fecha_inicio_rondin', '')

            if not fecha_inicio:
                continue

            try:
                fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')
            except Exception:
                try:
                    fecha_bitacora = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M')
                except Exception:
                    continue

            if fecha_bitacora.year != year or fecha_bitacora.month != month or fecha_bitacora.day != dia:
                continue

            if hora_valida:
                try:
                    hora_inicio = fecha_bitacora.hour
                    hora_esperada = int(hora_valida)

                    if not (hora_esperada <= hora_inicio <= hora_esperada + 1):
                        continue
                except Exception:
                    pass

            # Verificar incidencias para esta área
            for incidencia in incidencias:
                nombre_area_incidencia = incidencia.get('nombre_area_salida', '')

                if nombre_area_incidencia == nombre_area:
                    return "incidencias"

            # Verificar si el área fue visitada
            for area_check in areas_del_rondin:
                area_nombre = area_check.get('rondin_area', '')

                if area_nombre != nombre_area:
                    continue

                fecha_check = area_check.get('fecha_hora_inspeccion_area', '')

                if fecha_check:
                    return "finalizado"

        # No se encontró visita para este día
        if hasattr(self, 'timezone') and self.timezone:
            try:
                tz = pytz.timezone(self.timezone)
                now = datetime.now(tz)
            except Exception:
                now = datetime.now()
        else:
            now = datetime.now()
        fecha_evaluada = datetime(year, month, dia)

        # Si es día pasado, verificar si estaba programada una bitácora
        estaba_programada, bitacora_programada = self._verificar_bitacora_programada(dia, year, month, hora_valida, bitacora_rondines)

        if estaba_programada:
            estatus_bitacora_programada = bitacora_programada.get('estatus_del_recorrido', '')
            if estatus_bitacora_programada == 'cancelado':
                return "cancelado"
            elif estatus_bitacora_programada == 'realizado':
                return "fuera_de_hora"
            else:
                return "no_inspeccionada"

        # Si es día futuro o presente
        if fecha_evaluada.date() > now.date():
            return "none"
        elif fecha_evaluada.date() == now.date():
            return "programado"
        else:
            return "no_aplica"

    def _mapear_estado_bitacora(self, estatus_bitacora, incidencias):
        if incidencias and len(incidencias) > 0:
            return 'incidencias'

        estados_map = {
            'realizado': 'finalizado',
            'cerrado': 'finalizado',
            'completado': 'finalizado',
            'finalizado': 'finalizado',
            'cancelado': 'no_inspeccionada',
            'pendiente': 'none',
            'en_proceso': 'none',
            'programado': 'programado',
        }

        status_normalizado = estatus_bitacora.lower().strip() if estatus_bitacora else ''
        for key, value in estados_map.items():
            if key in status_normalizado:
                return value
        return 'finalizado' if estatus_bitacora else 'none'

    def pause_or_play_rondin(self, record_id, paused=True):
        answers = {
            self.rondin_keys['accion_recurrencia']: 'pausar' if paused else 'programar',
        }
        response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CONFIGURACION_RECORRIDOS_FORM, record_id=[record_id])
        if response.get('status_code') in [200, 201, 202]:
            return {'status_code': 200, 'type': 'success', 'msg': 'Rondin paused successfully', 'data': {}}
        else:
            return {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}

    def run_cron(self, dag_id):
        print('dag_id', dag_id)
        response = self.lkf_api.run_cron(dag_id)
        print('response', response)
        return response
    
    def update_inspeccion(self, folio, rondin_data: dict = {}):
        answers = {}
        existing_record = self.get_rondin_by_id(folio)
        folio = existing_record.get("folio", "")
        existing_areas = existing_record.get("areas", [])
        if existing_areas and isinstance(existing_areas[0], list):
            existing_areas = existing_areas[0]

        inspeccion = rondin_data.get('inspeccion', '')
        prompt_inspeccion = rondin_data.get('prompt_inspeccion', '')
        areas_targets = rondin_data.get('areas', [])

        updated_areas = []
        for i, area_item in enumerate(existing_areas):
            area_nombre = area_item.get('rondin_area', '')
            should_update = (
                not areas_targets or
                areas_targets == ["todas"] or
                area_nombre in areas_targets
            )
            area_dict = {
                self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                    self.Location.f['area']: area_nombre,
                    self.f['area_tag_id']: area_item.get('area_tag_id', []),
                    self.f['foto_area']: area_item.get('foto_area', []),
                    self.f['geolocalizacion_area_ubicacion']: area_item.get('geolocalizacion_area_ubicacion', []),
                },
                self.CATALOGO_FORMAS_OBJ_ID: {
                    self.mf['nombre_forma']: inspeccion if should_update else area_item.get(self.CATALOGO_FORMAS_OBJ_ID, {}).get(self.mf['nombre_forma'], ''),
                    self.rondin_keys['grupo_id']: area_item.get(self.CATALOGO_FORMAS_OBJ_ID, {}).get(self.rondin_keys['grupo_id'], ['129870'])
                },
                self.rondin_keys['prompt_inspeccion']: prompt_inspeccion if should_update else area_item.get(self.rondin_keys['prompt_inspeccion'], '')
            }
            updated_areas.append(area_dict)

        answers[self.rondin_keys["areas"]] = {str(i): area for i, area in enumerate(updated_areas)}

        print('actualizando inspeccion...', simplejson.dumps(answers, indent=4))

        response = self.lkf_api.patch_multi_record(
            answers=answers,
            form_id=self.CONFIGURACION_RECORRIDOS_FORM,
            folios=[folio,]
        )
        return response
    
    def update_rondin(self, folio, rondin_data: dict = {}):
        answers = {}
        for key, value in rondin_data.items():
            if key == 'ubicacion':
                ubicacion_result = self.get_ubicacion_geolocation(location=value)
                ubicacion = ubicacion_result if ubicacion_result else value
                if isinstance(ubicacion, dict):
                    answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                        self.Location.f['location']: ubicacion.get('location', ''),
                        self.f['address_geolocation']: ubicacion.get('geolocation', [])
                    }
                else:
                    answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                        self.Location.f['location']: ubicacion,
                        self.f['address_geolocation']: []
                    }
            elif key == 'area':
                if value:
                    answers[self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID] = {
                        self.mf['nombre_area_salida']: value
                    }
            elif key == 'grupo_asignado':
                answers[self.GRUPOS_CAT_OBJ_ID] = {
                    self.rondin_keys[key]: value
                }
            elif key == 'asignado_a':
                answers[self.rondin_keys['grupo_asignado_a']] = self.rondin_asignado_a(value)
            elif key == 'areas':
                areas_list = []
                for area in value:
                    if isinstance(area, dict):
                        area_dict = {
                            self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.Location.f['area']: area.get('area', ''),
                                self.f['geolocalizacion_area_ubicacion']: [{
                                    'latitude': area.get('latitude', 0),
                                    'longitude': area.get('longitude', 0)
                                }],
                                self.f['foto_area']: area.get('image', []),
                                self.f['area_tag_id']: [area.get('tag_id', [])]
                            },
                            self.CATALOGO_FORMAS_OBJ_ID: {},
                            self.rondin_keys['prompt_inspeccion']: ''
                        }
                    else:
                        area_dict = {
                            self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.Location.f['area']: area,
                                self.f['geolocalizacion_area_ubicacion']: [],
                                self.f['foto_area']: [],
                                self.f['area_tag_id']: []
                            },
                            self.CATALOGO_FORMAS_OBJ_ID: {},
                            self.rondin_keys['prompt_inspeccion']: ''
                        }
                    areas_list.append(area_dict)
                answers[self.rondin_keys["areas"]] = areas_list
           
            elif key == 'sucede_recurrencia' and value and ('dia_del_mes' in value or 'mes' in value):
                actual_day = datetime.now().day
                answers[self.rondin_keys['que_dia_del_mes']] = int(actual_day)
                answers[self.rondin_keys[key]] = value
            elif key == 'tipo_rondin':
                if value:
                    answers[self.rondin_keys[key]] = value.lower()
            elif value == '' or value is None:
                pass
            else:
                answers[self.rondin_keys[key]] = value

        print('actualizando rondin...', simplejson.dumps(answers, indent=4))
        print("grupoooo", folio)

        response = self.lkf_api.patch_multi_record(
            answers=answers,
            form_id=self.CONFIGURACION_RECORRIDOS_FORM,
            folios=[folio]
        )
        return response

    def _verificar_bitacora_programada(self, dia, year, month, hora_valida, bitacora_rondines):
        """
        Verifica si había una bitácora programada para un día específico.

        Args:
            dia (int): Día del mes
            year (int): Año
            month (int): Mes
            hora_valida (str): Hora esperada del recorrido
            bitacora_rondines (list): Lista de bitácoras del recorrido

        Returns:
            bool: True si había bitácora programada, False si no
        """
        # Buscar si existe alguna bitácora con fecha_programacion para ese día
        for bitacora in bitacora_rondines:
            fecha_programacion = bitacora.get('fecha_programacion', '')

            if not fecha_programacion:
                continue

            try:
                fecha_prog = datetime.strptime(fecha_programacion, '%Y-%m-%d %H:%M:%S')
            except Exception:
                try:
                    fecha_prog = datetime.strptime(fecha_programacion, '%Y-%m-%d %H:%M')
                except Exception:
                    continue

            # Verificar si la programación era para este día
            if fecha_prog.year != year or fecha_prog.month != month or fecha_prog.day != dia:
                continue

            # Verificar la hora si es necesario
            if hora_valida:
                try:
                    hora_programada = fecha_prog.hour
                    hora_esperada = int(hora_valida)

                    if hora_esperada <= hora_programada <= hora_esperada + 1:
                        return True, bitacora
                except Exception:
                    pass
            else:
                return True, bitacora

        return False, {}

    def asignar_recorrido(self, folio, asignado_a):
        if not folio:
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono el folio'})
        if not asignado_a:
            return self.LKFException({'title': 'Error', 'msg': 'No se proporciono el asignado_a'})

        answers = {}
        grupo = {}

        for index, nombre in enumerate(asignado_a):
            empleado_set = self.rondin_asignado_a(nombre)
            if empleado_set:
                grupo[(index + 1) * -1] = empleado_set[0]

        if grupo:
            answers[self.rondin_keys['grupo_asignado_a']] = {'0': list(grupo.values())[0]}
        print("answers", simplejson.dumps(answers, indent=4))

        res = self.lkf_api.patch_multi_record(
            answers=answers,
            form_id=self.CONFIGURACION_RECORRIDOS_FORM,
            record_id=[folio]
        )
        print("response", res)
        return res


if __name__ == "__main__":
    class_obj = Accesos(settings, sys_argv=sys.argv, use_api=False)
    class_obj.console_run()
    data = class_obj.data.get('data',{})
    option = data.get("option", '')
    rondin_data = data.get("rondin_data", {})
    date_from = data.get("date_from", None)
    date_to = data.get("date_to", None)
    limit = data.get("limit", 20)
    offset = data.get("offset", 0)
    folio = data.get("folio", '')
    record_id = data.get("record_id", '')
    ubicacion = data.get("ubicacion", None)
    nombre_rondin = data.get("nombre_rondin", None)
    area = data.get("area", None)
    paused = data.get("paused", True)
    areas = data.get("areas", [])
    year = data.get("year", None)
    month = data.get("month", None)
    areas = data.get("areas", [])
    dag_id = data.get("dag_id", [])
    area_details = data.get("area_details", False)
    asignado_a = data.get("asignado_a", [])
    user_to_assign = data.get("user_to_assign", {})
    data_script = class_obj.current_record
    class_obj.timezone = data_script.get('timezone', 'America/Mexico_City')
    tz = pytz.timezone(class_obj.timezone)
    if option == 'create_rondin':
        response = class_obj.create_rondin(rondin_data=rondin_data)
    elif option == 'claim_rondin':
        response = class_obj.claim_rondin(record_id)
    elif option == 'create_incidencia_by_rondin':
        response = class_obj.create_incidencia_by_rondin(data=rondin_data)
    elif option == 'delete_rondin':
        response = class_obj.delete_rondin(folio=folio)
    elif option == 'edit_areas_rondin':
        response = class_obj.edit_areas_rondin(areas=areas, folio=folio, record_id=record_id)
    elif option == 'get_recorridos':
        response = class_obj.get_recorridos(date_from=date_from, date_to=date_to, area_details=area_details, limit=limit, offset=offset)
    elif option == 'get_bitacora':
        response = class_obj.get_bitacora(date_from=date_from, date_to=date_to, area_details=area_details, limit=limit, offset=offset)
    elif option == 'get_catalog_areas':
        response = class_obj.get_catalog_areas(ubicacion=ubicacion)
    elif option == 'get_all_checks':
        response = class_obj.get_all_checks(ubicacion=ubicacion, nombre_rondin=nombre_rondin)
    elif option == 'get_rondin_by_id':
        response = class_obj.get_rondin_by_id(record_id=record_id)
    elif option == 'get_incidencias_rondines':
        response = class_obj.get_incidencias_rondines(location=ubicacion, area=area, date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    elif option == 'get_rondines_images':
        response = class_obj.get_rondines_images(location=ubicacion, areas=areas, date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    elif option == 'get_bitacora_rondines':
        response = class_obj.get_bitacora_rondines(location=ubicacion, nombre_rondin=nombre_rondin, year=year, month=month)
    elif option == 'get_check_by_id':
        response = class_obj.get_check_by_id(record_id=record_id)
    elif option == 'get_bitacora_by_id':
        response = class_obj.get_bitacora_by_id(record_id=record_id)
    elif option == 'get_catalog_areas_formatted':
        response = class_obj.get_catalog_areas_formatted(ubicacion=ubicacion)
    elif option == 'catalago_grupos_recorridos':
        response = class_obj.catalago_grupos_recorridos()
    elif option == 'catalogo_inspecciones':
        response = class_obj.catalogo_inspecciones()
    elif option == 'pause_or_play_rondin':
        response = class_obj.pause_or_play_rondin(record_id=record_id, paused=paused)
    elif option == 'update_rondin':
        response = class_obj.update_rondin(folio=folio,rondin_data=rondin_data)
    elif option == 'update_inspeccion':
        response = class_obj.update_inspeccion(folio=folio,rondin_data=rondin_data)
    elif option == 'assign_rondin':
        response = class_obj.assign_rondin(record_id=record_id, user_to_assign=user_to_assign)
    elif option == 'asignar_recorrido':
        response = class_obj.asignar_recorrido(folio=folio, asignado_a=asignado_a)
    elif option in ('run_rondin','run_cron'):
        response = class_obj.run_cron(dag_id=dag_id)
    else:
        response = {"msg": "Empty"}
    class_obj.HttpResponse({"data": response})