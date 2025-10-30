# coding: utf-8
from datetime import date
import sys, simplejson
from tkinter import N
from bson import ObjectId
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
        self.f.update({
            'rondin_area': '663e5d44f5b8a7ce8211ed0f',
            'foto_area': '6763096aa99cee046ba766ad',
        })
        
        self.rondin_keys = {
            'nombre_rondin': '6645050d873fc2d733961eba',
            'duracion_estimada': '6854459836ea891d9d2be7d9',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'areas': '6645052ef8bc829a5ccafaf5',
            'grupo_areas':'66462aa5d4a4af2eea07e0d1',
            'grupo_asignado': '638a9ab3616398d2e392a9fa',
            'fecha_hora_programada': 'abcde0001000000000010001',
            'programar_anticipacion': 'abcde0002000000000010001',
            'cuanto_tiempo_de_anticipacion': 'abcde0002000000000010004',
            'cuanto_tiempo_de_anticipacion_expresado_en': 'abcde0002000000000010005',
            'tiempo_para_ejecutar_tarea': 'abcde0001000000000010004',
            'tiempo_para_ejecutar_tarea_expresado_en': 'abcde0001000000000010005',
            'la_tarea_es_de': 'abcde0001000000000010006',
            'se_repite_cada': 'abcde0001000000000010007',
            'sucede_cada': 'abcde0001000000000010008',
            'sucede_recurrencia': 'abcde0001000000000010009',
            'en_que_minuto_sucede': 'abcde0001000000000010010',
            'cada_cuantos_minutos_se_repite': 'abcde0001000000000010011',
            'en_que_hora_sucede': 'abcde0001000000000010012',
            'cada_cuantas_horas_se_repite': 'abcde0001000000000010013',
            'que_dias_de_la_semana': 'abcde0001000000000010014',
            'en_que_semana_sucede': 'abcde0001000000000010015',
            'que_dia_del_mes': 'abcde0001000000000010016',
            'cada_cuantos_dias_se_repite': 'abcde0001000000000010017',
            'en_que_mes': 'abcde0001000000000010018',
            'cada_cuantos_meses_se_repite': 'abcde0001000000000010019',
            'la_recurrencia_cuenta_con_fecha_final': '64374e47a208e5c0ff95e9bd',
            'fecha_final_recurrencia': 'abcde0001000000000010099',
        }
        
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

    def create_rondin(self, rondin_data: dict = {}):
        """Crea un rondin con los datos proporcionados.
        Args:
            rondin_data (dict): Un diccionario con los datos del rondin.
        Returns:
            response: La respuesta de la API de Linkaform al crear el rondin.
        """
        #! Data hardcoded for testing purposes

        # rondin_data = {
        #     'nombre_rondin': 'Ejemplo rondin',
        #     'duracion_estimada': '30 minutos',
        #     'ubicacion': 'Planta Monterrey',
        #     'areas': [
        #         'Caseta 6 Poniente',
        #         'Sala de Juntas Planta Baja',
        #         'Recursos eléctricos',
        #         'Almacén de inventario',
        #     ],
        #     'grupo_asignado': 'Guardias',
        #     'fecha_hora_programada': '2025-10-01 12:00:00',
        #     'programar_anticipacion': 'no',
        #     'cuanto_tiempo_de_anticipacion': '',
        #     'cuanto_tiempo_de_anticipacion_expresado_en': '',
        #     'tiempo_para_ejecutar_tarea': 30,
        #     'tiempo_para_ejecutar_tarea_expresado_en': 'minutos',
        #     'la_tarea_es_de': '',
        #     'se_repite_cada': '',
        #     'sucede_cada': 'igual_que_la_primer_fecha',
        #     'sucede_recurrencia': [],
        #     'en_que_minuto_sucede': '',
        #     'cada_cuantos_minutos_se_repite': '',
        #     'en_que_hora_sucede': '',
        #     'cada_cuantas_horas_se_repite': '',
        #     'que_dias_de_la_semana': [],
        #     'en_que_semana_sucede': '',
        #     'que_dia_del_mes': '',
        #     'cada_cuantos_dias_se_repite': '',
        #     'en_que_mes': '',
        #     'cada_cuantos_meses_se_repite': '',
        #     'la_recurrencia_cuenta_con_fecha_final': 'no',
        #     'fecha_final_recurrencia': '',
        # }
        
        answers = {}
        rondin_data['ubicacion'] = self.get_ubicacion_geolocation(location=rondin_data.get('ubicacion', ''))
        rondin_data['areas'] = self.get_areas_details(areas_list=rondin_data.get('areas', []))
        
        for key, value in rondin_data.items():
            if key == 'ubicacion':
                answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                    self.Location.f['location']: value.get('location', ''),
                    self.f['address_geolocation']: value.get('geolocation', [])
                }
            elif key == 'grupo_asignado':
                answers[self.GRUPOS_CAT_OBJ_ID] = {
                    self.rondin_keys[key]: value
                }
            elif key == 'areas':
                areas_list = []
                for area in value:
                    area_dict = {
                        self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                            self.Location.f['area']: area.get('area', ''),
                            self.f['address_geolocation']: area.get('geolocation', []),
                            self.f['foto_area']: area.get('image', [])
                        }
                    }
                    areas_list.append(area_dict)
                answers[self.rondin_keys[key]] = areas_list
            elif value == '':
                pass
            else:
                answers[self.rondin_keys[key]] = value
        print("answers", answers)
        response = self.create_register(
            module='Accesos',
            process='Creacion de un rondin',
            action='rondines',
            file='accesos/app.py',
            form_id=121742, #TODO Modularizar este ID
            answers=answers
        )
        return response

    def update_rondin(self, folio, rondin_data: dict = {}):
        answers = {}
        
        for key, value in rondin_data.items():
            if key == 'ubicacion':
                answers[self.Location.UBICACIONES_CAT_OBJ_ID] = {
                    self.Location.f['location']: value
                }
            elif key == 'grupo_asignado':
                answers[self.GRUPOS_CAT_OBJ_ID] = {
                    self.rondin_keys[key]: value
                }
            elif key == 'areas':
                areas_list = []
                for area in value:
                    area_dict = {
                        self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                            self.Location.f['area']: area
                        }
                    }
                    areas_list.append(area_dict)
                answers[self.rondin_keys["grupo_areas"]] = areas_list
            elif value == '':
                pass
            else:
                answers[self.rondin_keys[key]] = value
        response = self.lkf_api.patch_multi_record( answers = answers, form_id=121742, folios=[folio])
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
        response = self.detail_response(response.get('status_code', 0))
        return response

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
        
        response = self.cr.delete_one({
            'form_id': 121742, # TODO Modularizar este ID
            'folio': folio
        })
        
        if response.deleted_count > 0:
            response = self.detail_response(202)
        else:
            response = self.detail_response(404)
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

    def get_rondines(self, date_from=None, date_to=None, limit=20, offset=0):
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
                "folio": 1,
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "nombre_del_rondin": f"$answers.{self.rondin_keys['nombre_rondin']}",
                "checkpoints": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},
                "recurrencia": {"$ifNull": [f"$answers.{self.rondin_keys['la_tarea_es_de']}", 'No Recurrente']},
                "duracion_estimada": f"$answers.{self.rondin_keys['duracion_estimada']}",
                "asignado_a": {"$ifNull": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['grupo_asignado']}", 'No Asignado']},
                "fecha_hora_programada": f"$answers.{self.rondin_keys['fecha_hora_programada']}",
                "cada_cuantos_dias_se_repite": f"$answers.{self.rondin_keys['cada_cuantos_dias_se_repite']}",
                "areas": f"$answers.{self.rondin_keys['areas']}.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area']}",
            }},
            {"$sort": {"folio": -1}},
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        return response
    
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
                "nombre_del_rondin": f"$answers.{self.rondin_keys['nombre_rondin']}",
                "recurrencia": {"$ifNull": [f"$answers.{self.rondin_keys['la_tarea_es_de']}", 'No Recurrente']},
                "asignado_a": {"$ifNull": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['grupo_asignado']}", 'No Asignado']},
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "ubicacion_geolocation": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.f['address_geolocation']}",
                "estatus_rondin": f"$answers.{self.f['status_cron']}",
                "fecha_inicio_rondin": f"$answers.{self.f['fecha_primer_evento']}",
                "duracion_esperada_rondin": {"$ifNull": [f"$answers.{self.rondin_keys['duracion_estimada']}", "No especificada"]},
                "fecha_final_rondin": f"$answers.{self.f['fecha_final_recurrencia']}",
                "cantidad_de_puntos": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},
                "areas": f"$answers.{self.rondin_keys['areas']}",
            }},
        ]

        response = self.format_cr(self.cr.aggregate(query))
        response = self.unlist(response)
        format_response = {}
        if response:
            format_response = self.format_rondin_by_id(response)
            location = response.get('ubicacion', '')
            rondin_name = response.get('nombre_del_rondin', '')
            duracion_promedio = self.get_average_rondin_duration(location=location, rondin_name=rondin_name)
            format_response['duracion_promedio'] = duracion_promedio
        return format_response
    
    def format_rondin_by_id(self, data):
        fotos_de_areas = []
        puntos_de_control = []
        for item in data.get('areas', []):
            new_item = {
                "id": item.get('area_tag_id', [])[0] if len(item.get('area_tag_id', [])) > 0 else "",
                "nombre_area": item.get('rondin_area', ''),
                "foto_area": item.get('foto_area', [])[0].get('file_url') if len(item.get('foto_area', [])) > 0 else "",
            }
            if new_item.get('foto_area'):
                fotos_de_areas.append(new_item)
            new_item = {
                "id": item.get('area_tag_id', [])[0] if len(item.get('area_tag_id', [])) > 0 else "",
                "nombre_area": item.get('rondin_area', ''),
                "geolocation_area": item.get('geolocalizacion_area_ubicacion', [])[0] if len(item.get('geolocalizacion_area_ubicacion', [])) > 0 else {},
            }
            if new_item.get('geolocation_area'):
                puntos_de_control.append(new_item)
        data.update({
            "recurrencia": data.get('recurrencia').replace('_', ' ').title(),
            "estatus_rondin": data.get('estatus_rondin').replace('_', ' ').title(),
            "ubicacion_geolocation": data.get('ubicacion_geolocation', [])[0] if len(data.get('ubicacion_geolocation', [])) > 0 else {},
            "images_data": fotos_de_areas,
            "map_data": puntos_de_control,
        })
        return data
        
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
                "area": f"$answers.{self.Location.f['area']}",
                "geolocation": f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.f['address_geolocation']}",
                "image": f"$answers.{self.f['foto_area']}"
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        return response

    def get_catalog_areas(self, ubicacion=""):
        #Obtener areas disponibles para rondin
        if ubicacion:
            options = {
                'startkey': [ubicacion],
                'endkey': [f"{ubicacion}\n",{}],
                'group_level':2
            }

            catalog_id = self.AREAS_DE_LAS_UBICACIONES_CAT_ID
            form_id = 121742
            return self.catalogo_view(catalog_id, form_id, options)
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
            }},
            {"$skip": offset},
            {"$limit": limit}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = []
        if response:
            format_response = self.format_incidencias_rondines(response, area)
        return format_response

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
                    "incidente": incidencia.get('tipo_de_incidencia', incidencia.get('incidente_open', '')),
                    "accion_tomada": incidencia.get('incidente_accion', ''),
                    "comentarios": incidencia.get('comentario_incidente_bitacora', ''),
                    "evidencias": incidencia.get('incidente_evidencia', []),
                    "documentos": incidencia.get('incidente_documento', []),
                }
                format_data.append(format_item)
        return format_data
    
    def create_incidencia_by_rondin(self, data):
        data = {
            'reporta_incidencia': "Emiliano Zapata",
            'fecha_hora_incidencia': "2025-10-24 13:07:16",
            'ubicacion_incidencia':"Planta Monterrey",
            'area_incidencia': "Recursos eléctricos",
            'categoria': "Intrusión y seguridad",
            'sub_categoria':"Alteración del orden",
            'incidente':"Drogadicto",
            # "tipo_incidencia": "Otro incidente",
            'comentario_incidencia': "comentario random",
            'evidencia_incidencia': [],
            'documento_incidencia':[],
            'acciones_tomadas_incidencia':[],
            "prioridad_incidencia": "leve",
            "notificacion_incidencia": "no",
        }
        status = {}
        response = self.create_incidence(data)
        if response.get('status_code') in [200, 201, 202]:
            status = {'status_code': 200, 'type': 'success', 'msg': 'Record created successfully', 'data': {}}
        else:
            status = {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        return status
    
    def get_rondines_images(self, location=None, area=None, date_from=None, date_to=None, limit=20, offset=0):
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
            f"answers.{self.f['grupo_areas_visitadas']}": {
                "$type": "array",
                "$not": {"$size": 0}
            }
        }
        
        unwind_match = {
            f"answers.{self.f['grupo_areas_visitadas']}.{self.f['foto_evidencia_area_rondin']}": {
                "$exists": True,
                "$not": {"$size": 0}
            }
        }
        
        if location:
            match.update({
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": location
            })
        if area:
            unwind_match.update({
                f"answers.{self.f['grupo_areas_visitadas']}.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['area']}": area
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
            {'$unwind': f"$answers.{self.f['grupo_areas_visitadas']}"},
            {"$match": unwind_match},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "areas_recorrido": f"$answers.{self.f['grupo_areas_visitadas']}",
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
    area = data.get("area", None)

    if option == 'create_rondin':
        response = class_obj.create_rondin(rondin_data=rondin_data)
    elif option == 'get_rondines':
        response = class_obj.get_rondines(date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    elif option == 'update_rondin':
        response = class_obj.update_rondin(folio=folio,rondin_data=rondin_data)
    elif option == 'delete_rondin':
        response = class_obj.delete_rondin(folio=folio)
    elif option == 'get_catalog_areas':
        response = class_obj.get_catalog_areas(ubicacion=ubicacion)
    elif option == 'get_rondin_by_id':
        response = class_obj.get_rondin_by_id(record_id=record_id)
    elif option == 'get_incidencias_rondines':
        response = class_obj.get_incidencias_rondines(location=ubicacion, area=area, date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    elif option == 'create_incidencia_by_rondin':
        response = class_obj.create_incidencia_by_rondin(data=rondin_data)
    elif option == 'get_rondines_images':
        response = class_obj.get_rondines_images(location=ubicacion, area=area, date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    else:
        response = {"msg": "Empty"}
    class_obj.HttpResponse({"data": response})