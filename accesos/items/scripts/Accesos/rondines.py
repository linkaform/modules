# coding: utf-8
from datetime import date
import sys, simplejson
from tkinter import N
from bson import ObjectId
from linkaform_api import settings
from account_settings import *
from datetime import datetime
import calendar

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
            'accion_recurrencia': 'abcde00010000000a0000001'
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
                            self.f['geolocalizacion_area_ubicacion']: area.get('geolocation', []),
                            self.f['foto_area']: area.get('image', []),
                            self.f['area_tag_id']: [area.get('tag_id', [])]
                        }
                    }
                    areas_list.append(area_dict)
                answers[self.rondin_keys[key]] = areas_list
            elif value == '':
                pass
            else:
                answers[self.rondin_keys[key]] = value
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
        format_response = []
        if response:
            for item in response:
                item['recurrencia'] = item['recurrencia'].replace('_', ' ').title() if item.get('recurrencia') else 'No Recurrente'
                format_response.append(item)
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
            foto_area_data = item.get('foto_area', [])
            foto_url = ""
            if foto_area_data:
                primer_elemento = foto_area_data[0]
                if isinstance(primer_elemento, list) and len(primer_elemento) > 0:
                    foto_url = primer_elemento[0].get('file_url', '')
                elif isinstance(primer_elemento, dict):
                    foto_url = primer_elemento.get('file_url', '')
            
            new_item = {
                "id": item.get('area_tag_id', [])[0] if len(item.get('area_tag_id', [])) > 0 else "",
                "nombre_area": item.get('rondin_area', ''),
                "foto_area": foto_url,
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
            "recurrencia": data.get('recurrencia').replace('_', ' ').title() if data.get('recurrencia') else 'No Recurrente',
            "estatus_rondin": data.get('estatus_rondin').replace('_', ' ').title() if data.get('estatus_rondin') else 'No Especificado',
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
                "image": f"$answers.{self.f['foto_area']}",
                "tag_id": f"$answers.{self.f['area_tag_id']}",
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
        # data = {
        #     'reporta_incidencia': "Emiliano Zapata",
        #     'fecha_hora_incidencia': "2025-10-24 13:07:16",
        #     'ubicacion_incidencia':"Planta Monterrey",
        #     'area_incidencia': "Recursos eléctricos",
        #     'categoria': "Intrusión y seguridad",
        #     'sub_categoria':"Alteración del orden",
        #     'incidente':"Drogadicto",
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
    
    def get_bitacora_rondines(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.CONFIGURACION_DE_RECORRIDOS_FORM,
                # f"answers.{self.mf['nombre_del_recorrido']}": "Recorrido Lunes 6"
            }},
            {"$project": {
                "answers": 1,
                "hora_agrupada": {
                    "$hour": {
                        "$dateFromString": {
                            "dateString": f"$answers.{self.f['fecha_primer_evento']}",
                            "format": "%Y-%m-%d %H:%M:%S",
                            "onError": None
                        }
                    }
                }
            }},
            {"$group": {
                "_id": "$hora_agrupada",
                "recorridos": {
                    "$push": {
                        "hora_original": f"$answers.{self.f['fecha_primer_evento']}",
                        "nombre_del_recorrido": f"$answers.{self.mf['nombre_del_recorrido']}",
                        "areas": f"$answers.{self.f['grupo_de_areas_recorrido']}"
                    }
                }
            }},
            {"$sort": {"_id": 1}},
            {"$lookup": {
                "from": "form_answer",
                "let": {"nombres_recorridos": "$recorridos.nombre_del_recorrido"},
                "pipeline": [
                    {"$match": {
                        "deleted_at": {"$exists": False},
                        "form_id": self.BITACORA_RONDINES,
                        "$expr": {
                            "$and": [
                                {"$in": [
                                    f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                                    "$$nombres_recorridos"
                                ]},
                                {"$eq": [{"$year": "$created_at"}, {"$year": "$$NOW"}]}, #TODO: Cambiar a mes por parametro
                                {"$eq": [{"$month": "$created_at"}, {"$month": "$$NOW"}]} #TODO: Cambiar a mes por parametro
                            ]
                        }
                    }},
                    {"$project": {
                        "_id": 0,
                        "hora": f"$answers.{self.f['fecha_inicio_rondin']}",
                        "areas_visitadas": f"$answers.{self.f['grupo_areas_visitadas']}",
                        "incidencias": f"$answers.{self.f['bitacora_rondin_incidencias']}",
                    }}
                ],
                "as": "bitacora_rondines"
            }},
            {"$project": {
                "_id": 0,
                "hora_agrupada": {"$concat": [
                    {"$cond": [{"$lt": ["$_id", 10]}, "0", ""]},
                    {"$toString": "$_id"},
                    ":00"
                ]},
                "recorridos": 1,
                "bitacora_rondines": 1,
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_resp = []
        if response:
            format_resp = self.format_bitacora_rondines(response)
        return format_resp

    def format_bitacora_rondines(self, data):
        # Obtener el mes y año actuales
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        days_in_month = calendar.monthrange(current_year, current_month)[1]
        
        format_data = []
        
        for item in data:
            hora_agrupada = item.get('hora_agrupada', '')
            recorridos = item.get('recorridos', [])
            bitacora_rondines = item.get('bitacora_rondines', [])
            
            # Crear estructura de categorías (una por cada recorrido)
            categorias = []
            
            for recorrido in recorridos:
                nombre_recorrido = recorrido.get('nombre_del_recorrido', '')
                areas_recorrido = recorrido.get('areas', [])
                hora_valida = recorrido.get('hora_original', '')
                hora_int = None
                if hora_valida:
                    try:
                        hora_int = datetime.strptime(hora_valida, '%Y-%m-%d %H:%M:%S').hour
                    except Exception:
                        try:
                            hora_int = datetime.strptime(hora_valida, '%Y-%m-%d %H:%M').hour
                        except Exception:
                            try:
                                hora_int = int(hora_valida.split()[1].split(':')[0])
                            except Exception:
                                hora_int = None
                hora_valida = str(hora_int) if hora_int is not None else ''
                
                # Crear estructura de áreas para esta categoría
                areas_formateadas = []
                
                for area in areas_recorrido:
                    nombre_area = area.get('rondin_area', '')
                    area_tag_id = area.get('area_tag_id', [])
                    area_tag = area_tag_id[0] if area_tag_id else ''
                    
                    # Inicializar estados para todos los días del mes
                    estados = []
                    for dia in range(1, days_in_month + 1):
                        # Buscar si existe bitácora para este día y área
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
                            areas_visitadas = bitacora.get('areas_visitadas', [])

                            for area_visitada in areas_visitadas:
                                area_nombre = area_visitada.get('rondin_area', '')
                                if area_nombre != nombre_area:
                                    continue
                                
                                fecha_str = area_visitada.get('fecha_hora_inspeccion_area', '')
                                if not fecha_str:
                                    continue
                                
                                url = area_visitada.get('url_registro_rondin', '')
                                if url:
                                    g_id_part = url.split('detail/')[-1]
                                    g_id = g_id_part.split('?')[0].split('#')[0].strip('/')

                        estados.append({
                            "dia": dia,
                            "estado": estado,
                            "record_id": g_id if estado not in ["none", "no_inspeccionada"] else "",
                        })
                    
                    
                    areas_formateadas.append({
                        "nombre": nombre_area,
                        "estados": estados
                    })
                
                categorias.append({
                    "titulo": nombre_recorrido,
                    "areas": areas_formateadas
                })
            
            format_data.append({
                "hora": hora_agrupada,
                "categorias": categorias
            })
        return format_data
    
    def _get_estado_area_dia(self, bitacora_rondines, area_tag_id, nombre_area, dia, year, month, hora_valida):
        """
        Determina si un área fue visitada en un día específico.
        
        Estados:
        - "incidencias": Área con incidencias registradas
        - "finalizado": Área visitada en la hora correcta
        - "no_inspeccionada": Área no visitada en día pasado (ya venció)
        - "none": Área no visitada en día futuro (aún no aplica)
        
        Args:
            bitacora_rondines (list): Lista de bitácoras del recorrido
            area_tag_id (str): ID del tag del área
            nombre_area (str): Nombre del área
            dia (int): Día del mes
            year (int): Año
            month (int): Mes
            hora_valida (str): Hora esperada del recorrido (ej: "8")
        
        Returns:
            str: Estado del área para ese día
        """
        
        # Buscar en las bitácoras si existe visita para este día
        for bitacora in bitacora_rondines:
            areas_visitadas = bitacora.get('areas_visitadas', [])
            incidencias = bitacora.get('incidencias', [])
            hora_bitacora = bitacora.get('hora', '')
            
            # Primero verificar si hay incidencias para esta área en este día
            for incidencia in incidencias:
                nombre_area_incidencia = incidencia.get('nombre_area_salida', '')
                
                # Si el nombre del área coincide
                if nombre_area_incidencia != nombre_area:
                    continue
                
                # Verificar si la fecha de la incidencia corresponde al día buscado
                fecha_incidencia_str = incidencia.get('fecha_hora_incidente_bitacora', '')
                if not fecha_incidencia_str:
                    continue
                
                try:
                    fecha_incidencia = datetime.strptime(fecha_incidencia_str, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        fecha_incidencia = datetime.strptime(fecha_incidencia_str, '%Y-%m-%d %H:%M')
                    except Exception:
                        continue
                
                # Verificar si es el día correcto
                if fecha_incidencia.year != year or fecha_incidencia.month != month or fecha_incidencia.day != dia:
                    continue
                
                # Verificar si la hora está dentro del rango válido (igual que para visitas)
                if hora_valida:
                    try:
                        hora_incidencia = fecha_incidencia.hour
                        hora_esperada = int(hora_valida)
                        
                        # Permitir que la incidencia esté en la hora esperada o hasta 1 hora después
                        if hora_esperada <= hora_incidencia <= hora_esperada + 1:
                            return "incidencias"
                    except Exception:
                        # Si no se puede validar la hora, pero el día coincide, considerar como incidencia
                        return "incidencias"
                else:
                    # Si no hay hora válida especificada, solo verificar el día
                    return "incidencias"
            
            # Si no hay incidencias, verificar si el área fue visitada
            for area_visitada in areas_visitadas:
                area_nombre = area_visitada.get('rondin_area', '')
                
                # Verificar si el nombre del área coincide
                if area_nombre != nombre_area:
                    continue
                
                # Verificar si la fecha corresponde al día buscado y está dentro de la hora válida
                fecha_str = area_visitada.get('fecha_hora_inspeccion_area', '')
                
                # Si no hay fecha_hora_inspeccion_area, usar la hora de la bitácora
                if not fecha_str:
                    fecha_str = hora_bitacora
                
                if not fecha_str:
                    continue
                
                try:
                    fecha_visita = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        fecha_visita = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M')
                    except Exception:
                        continue
                
                # Verificar si es el día correcto
                if fecha_visita.year != year or fecha_visita.month != month or fecha_visita.day != dia:
                    continue
                
                # Verificar si la hora está dentro del rango válido
                # La hora válida puede ser la hora exacta o una hora después (tolerancia de 1 hora)
                if hora_valida:
                    try:
                        hora_check = fecha_visita.hour
                        hora_esperada = int(hora_valida)
                        
                        # Permitir que el check se haya hecho en la hora esperada o hasta 1 hora después
                        if hora_esperada <= hora_check <= hora_esperada + 1:
                            return "finalizado"
                    except Exception:
                        # Si no se puede validar la hora, pero el día coincide, considerar como finalizado
                        return "finalizado"
                else:
                    # Si no hay hora válida especificada, solo verificar el día
                    return "finalizado"
        
        # Si no se encontró visita para este día, determinar si es pasado o futuro
        now = datetime.now()
        fecha_evaluada = datetime(year, month, dia)
        
        # Si la fecha evaluada es anterior a hoy, es "no_inspeccionada"
        if fecha_evaluada.date() < now.date():
            return "no_inspeccionada"
        else:
            # Si es hoy o futuro, es "none"
            return "none"
        
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
                                "input": f"$answers.{self.f['grupo_areas_visitadas']}",
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
            {"$unwind": f"$answers.{self.f['grupo_areas_visitadas']}"},
            {"$match": {
                "$expr": {
                    "$regexMatch": {
                        "input": f"$answers.{self.f['grupo_areas_visitadas']}.{self.f['url_registro_rondin']}",
                        "regex": record_id
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "ubicacion": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}",
                "nombre_recorrido": f"$answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.mf['nombre_del_recorrido']}",
                "area": f"$answers.{self.f['grupo_areas_visitadas']}",
            }}
        ]

        response = self.format_cr(self.cr.aggregate(query))
        response = self.unlist(response)
        format_response = {}
        if response:
            format_response = self.format_check_by_id(response)
        return format_response

    def format_check_by_id(self, data: dict):
        """
        Formatea los detalles de un check por su ID de registro.
        Args:
            data (dict): Datos del check.
        Returns:
            dict: Un diccionario con los detalles formateados del check.
        """
        format_data = {
            'area': data.get('rondin_area', ''),
            'checks_mes': [], #TODO: Agregar info de checks del mes
            'fotos': [{'pic_name': item.get('name', ''),'pic_url': item.get('file_url', '')} for item in data.get('foto_evidencia_area_rondin', [])],
            'hora_de_check': data.get('fecha_hora_inspeccion_area', ''),
            'ubicacion': data.get('ubicacion', ''),
            'tiempo_traslado': data.get('duracion_traslado_area', ''),
            'comentarios': data.get('comentario_area_rondin', ''),
        }
        return format_data
    
    def pause_rondin(self, record_id):
        answers = {
            self.rondin_keys['accion_recurrencia']: 'pausar',
        }
        response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CONFIGURACION_RECORRIDOS_FORM, record_id=[record_id])
        if response.get('status_code') in [200, 201, 202]:
            return {'status_code': 200, 'type': 'success', 'msg': 'Rondin paused successfully', 'data': {}}
        else:
            return {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
        
    def play_rondin(self, record_id):
        answers = {
            self.rondin_keys['accion_recurrencia']: 'programar',
        }
        response = self.lkf_api.patch_multi_record(answers=answers, form_id=self.CONFIGURACION_RECORRIDOS_FORM, record_id=[record_id])
        if response.get('status_code') in [200, 201, 202]:
            return {'status_code': 200, 'type': 'success', 'msg': 'Rondin resumed successfully', 'data': {}}
        else:
            return {'status_code': 400, 'type': 'error', 'msg': response, 'data': {}}
    
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
    elif option == 'get_bitacora_rondines':
        response = class_obj.get_bitacora_rondines()
    elif option == 'get_check_by_id':
        response = class_obj.get_check_by_id(record_id=record_id)
    elif option == 'pause_rondin':
        response = class_obj.pause_rondin(record_id=record_id)
    elif option == 'play_rondin':
        response = class_obj.play_rondin(record_id=record_id)
    else:
        response = {"msg": "Empty"}
    class_obj.HttpResponse({"data": response})