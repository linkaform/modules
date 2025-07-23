# coding: utf-8
from datetime import date
import sys, simplejson
from tkinter import N
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
        self.rondin_keys = {
            'nombre_rondin': '6645050d873fc2d733961eba',
            'duracion_estimada': '6854459836ea891d9d2be7d9',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'areas': '6645052ef8bc829a5ccafaf5',
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
        
    def create_rondin(self, rondin_data: dict = {}):
        rondin_data = {
            'nombre_rondin': 'Ejemplo rondin',
            'duracion_estimada': '30 minutos',
            'ubicacion': 'Planta Monterrey',
            'areas': [
                'Caseta 6 Poniente',
                'Sala de Juntas Planta Baja',
                'Recursos eléctricos',
            ],
            'grupo_asignado': 'Guardias',
            'fecha_hora_programada': '2025-10-01 12:00:00',
            'programar_anticipacion': 'no',
            'cuanto_tiempo_de_anticipacion': '',
            'cuanto_tiempo_de_anticipacion_expresado_en': '',
            'tiempo_para_ejecutar_tarea': 30,
            'tiempo_para_ejecutar_tarea_expresado_en': 'minutos',
            'la_tarea_es_de': '',
            'se_repite_cada': '',
            'sucede_cada': 'igual_que_la_primer_fecha',
            'sucede_recurrencia': [],
            'en_que_minuto_sucede': '',
            'cada_cuantos_minutos_se_repite': '',
            'en_que_hora_sucede': '',
            'cada_cuantas_horas_se_repite': '',
            'que_dias_de_la_semana': [],
            'en_que_semana_sucede': '',
            'que_dia_del_mes': '',
            'cada_cuantos_dias_se_repite': '',
            'en_que_mes': '',
            'cada_cuantos_meses_se_repite': '',
            'la_recurrencia_cuenta_con_fecha_final': 'no',
            'fecha_final_recurrencia': '',
        }
        
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

    def list_rondines(self, date_from=None, date_to=None, limit=20, offset=0):
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
            "form_id": 121742,
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
                "_id": 0,
                "folio": 1,
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                "nombre_del_rondin": f"$answers.{self.rondin_keys['nombre_rondin']}",
                "checkpoints": {"$size": {"$ifNull": [f"$answers.{self.rondin_keys['areas']}", []]}},
                "recurrencia": {"$ifNull": [f"$answers.{self.rondin_keys['la_tarea_es_de']}", 'No Recurrente']},
                "duracion_estimada": f"$answers.{self.rondin_keys['duracion_estimada']}",
                "asignado_a": {"$ifNull": [f"$answers.{self.GRUPOS_CAT_OBJ_ID}.{self.rondin_keys['grupo_asignado']}", 'No Asignado']},
            }},
            {"$sort": {"created_at": -1}},
            {"$skip": offset},
            {"$limit": limit}
        ]
        
        response = self.format_cr(self.cr.aggregate(query))
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
            'form_id': 121742, # ID del formulario de rondines
            'folio': folio
        })
        
        if response.deleted_count > 0:
            response = self.detail_response(202)
        else:
            response = self.detail_response(404)
        return response
    
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

    if option == 'create_rondin':
        response = class_obj.create_rondin(rondin_data=rondin_data)
    elif option == 'list_rondines':
        response = class_obj.list_rondines(date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    elif option == 'delete_rondin':
        response = class_obj.delete_rondin(folio=folio)
    else:
        response = {"msg": "Empty"}
    class_obj.HttpResponse({"data": response})