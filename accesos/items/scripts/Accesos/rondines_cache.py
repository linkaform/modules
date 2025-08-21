# coding: utf-8
import sys, simplejson, json, pytz
import time
from datetime import datetime, timedelta, date
from bson import ObjectId

from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos
import random

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

    #! Utils functions ==========
    def parse_date_for_sorting(self, date_str):
        if not date_str or not date_str.strip():
            return datetime.max
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except:
            return datetime.max
    #! ===========================

    def search_rondin_by_area(self):
        """
        Search for a rondin by location and check_area in form Configuracion de Recorridos.
        """
        query = [
            {'$match': {
                'deleted_at': {'$exists': False},
                'form_id': self.CONFIGURACION_DE_RECORRIDOS_FORM,
                f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": self.location,
                f"answers.{self.f['grupo_de_areas_recorrido']}": {'$exists': True}
            }},
            # {'$unwind': f"$answers.{self.f['grupo_de_areas_recorrido']}"},
            {'$project': {
                '_id': 1,
                # 'match_area': f"$answers.{self.f['grupo_de_areas_recorrido']}.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area']}",
                'nombre_recorrido': f"$answers.{self.f['nombre_del_recorrido']}",
                'ubicacion_recorrido': f"$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_recorrido']}"
            }},
            # {'$match': {
            #     'match_area': self.check_area
            # }},
            # {"$project": {
            #     "_id": 1,
            #     "nombre_recorrido": 1,
            #     "ubicacion_recorrido": 1
            # }}
        ]
        resp = self.cr.aggregate(query)
        format_res = list({item.get('nombre_recorrido') for item in resp})
        return format_res

    def search_active_bitacora_by_rondin(self, recorridos):
        """
        Search for a bitacora by rondin name in form Bitacora Rondines.
        """
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": self.location,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.f['nombre_del_recorrido']}": {"$in": recorridos},
                f"answers.{self.f['estatus_del_recorrido']}": 'en_proceso',
            }},
            {'$sort': {'created_at': -1}},
            {'$limit': 1},
            {'$project': {
                '_id': 1,
                'folio': 1,
                'fecha_programacion': f"$answers.{self.f['fecha_programacion']}",
                'answers': f"$answers"
            }},
        ]
        resp = self.format_cr(self.cr.aggregate(query))
        return resp
    
    def search_closed_bitacora_by_check(self, status=['cerrado', 'realizado'], date_from=None, date_to=None):
        """
        Search for closed bitacora entries by status.
        """
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.f['fecha_inicio_rondin']}": {"$gte": date_from, "$lte": date_to} if date_from and date_to else {},
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.Location.f['location']}": self.location,
                f"answers.{self.f['estatus_del_recorrido']}": {"$in": status},
            }},
            {'$project': {
                '_id': 1,
                'folio': 1,
                'answers': f"$answers"
            }},
        ]
        resp = self.format_cr(self.cr.aggregate(query))
        return resp

    def search_cache(self, search_winner=False, location=None):
        """
        Search active caches, optionally filtered by location.
        """
        match_query = {}
        if search_winner:
            match_query.update({
                'winner': True,
            })
        if location:
            match_query.update({
                'location': location
            })
        
        query = [
            {'$match': match_query}
        ]
        resp = self.cr_cache.aggregate(query)
        format_resp = list(resp)
        return format_resp

    def create_cache(self):
        """
        Create a cache entry for a rondin.
        """
        data = {}
        data.update({
            '_id': ObjectId(self.record_id),
            'location': self.location,
            'folio': self.folio,
            'timestamp': self.timestamp,
            'random': random.random(),
            'check_data': self.answers,
        })
        return self.create(data, collection='rondin_caches')

    def select_winner(self, caches_list, location=None):
        """
        Select a winner rondin from the cache, optionally filtered by location.
        """
        if location:
            caches_list = [cache for cache in caches_list if cache.get('location') == location]
        
        if not caches_list:
            return None
            
        sorted_caches = sorted(caches_list, key=lambda x: (x.get('timestamp', float('inf')), -x.get('random', 0)))
        winner = sorted_caches[0]
        
        update_data = {
            '$set': {
                'winner': True
            }
        }
        
        self.cr_cache.update_one(
            {'_id': winner['_id']}, 
            update_data
        )
        
        return winner
            
    def add_checks_to_winner(self, winner, checks_list):
        """
        Add checks for the current area to the winner.
        """
        for check in checks_list:
            new_check = {
                "area": check['check_data'].get(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['area'], []),
                "record_id": str(check.get('_id', '')),
                "timestamp": check['timestamp'],
                "check_data": check['check_data']
            }
            self.cr_cache.update_one(
                {'_id': winner['_id']},
                {
                    '$addToSet': {
                        'checks': new_check
                    }
                }
            )

    def create_bitacora_from_cache(self, winner, recorridos_names):
        """
        Create a bitacora entry from cache data.
        """
        nombre_del_recorrido = ""
        for name in recorridos_names:
            nombre_del_recorrido = name
        
        metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_RONDINES)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de Rondin",
                    "Action": "check_ubicacion_rondin",
                    "File": "accesos/check_ubicacion_rondin.py"
                }
            },
        })
        answers = {}

        tz = pytz.timezone('America/Mexico_City')

        answers[self.f['fecha_programacion']] = winner.get('timestamp') and datetime.fromtimestamp(winner.get('timestamp'), tz).strftime('%Y-%m-%d %H:%M:%S')
        answers[self.f['fecha_inicio_rondin']] = winner.get('timestamp') and datetime.fromtimestamp(winner.get('timestamp'), tz).strftime('%Y-%m-%d %H:%M:%S')

        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = {
            self.f['ubicacion_recorrido']: self.location,
            self.f['nombre_del_recorrido']: nombre_del_recorrido
        }
        estatus_del_recorrido = 'realizado' if winner.get('check_data', {}).get(self.f['check_status'], '') == 'finalizado' else 'en_proceso'
        answers[self.f['estatus_del_recorrido']] = estatus_del_recorrido
        check_areas_list = []
        for area in winner.get('checks', []):
            format_area = {
                self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                    self.f['nombre_area']: self.unlist(area['check_data'].get(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.Location.f['area'], []))
                },
                self.f['fecha_hora_inspeccion_area']: area.get('timestamp') and datetime.fromtimestamp(area.get('timestamp'), tz).strftime('%Y-%m-%d %H:%M:%S'),
                self.f['foto_evidencia_area_rondin']: area['check_data'].get(self.f['foto_evidencia_area'], []),
                self.f['comentario_area_rondin']: area['check_data'].get(self.f['comentario_check_area'], ''),
                self.f['url_registro_rondin']: f"https://app.linkaform.com/#/records/detail/{area.get('record_id', '')}",
            }
            check_areas_list.append(format_area)
            
        check_areas_list.sort(key=lambda x: self.parse_date_for_sorting(x.get(self.f['fecha_hora_inspeccion_area'], '')))
        answers[self.f['areas_del_rondin']] = check_areas_list

        # for area in areas_recorrido:
        #     if not area.get('incidente_area') == area_rondin:
        #         answers[self.f['areas_del_rondin']].append({
        #             self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
        #                 self.f['nombre_area']: area.get('incidente_area', '')
        #             },
        #         })

        # answers[self.f['bitacora_rondin_incidencias']] = self.answers.get(self.f['grupo_incidencias_check'], [])

        metadata.update({'answers':answers})
        # print(simplejson.dumps(metadata, indent=3))

        res = self.lkf_api.post_forms_answers(metadata)
        return res

    def clear_cache(self, location=None, record_id=None, winner=None):
        """
        Clear collection rondin_caches.
        """
        if location:
            self.cr_cache.delete_many({'location': location})
            return
        if record_id:
            self.cr_cache.delete_one({'_id': ObjectId(record_id)})
            return
        if winner:
            self.cr_cache.delete_one({'winner': True})
            return
        
        self.cr_cache.delete_many({})
        
    def check_areas_in_rondin(self, cache, rondin):
        """
        Recibe: Las answers del check de ubicacion, el area que se hice check de ubicacion y el registro de rondin
        Retorna: La respuesta de la api al hacer el patch de un registro
        Error: La respuesta de la api al hacer el patch de un registro
        """
        tz = pytz.timezone('America/Mexico_City')
        today = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        rondin = self.unlist(rondin)
        rondin_en_progreso = True
        answers={}
        areas_list = []

        # print('rondinnnnnnnnnnnnnnnnn', simplejson.dumps(rondin, indent=3))

        if not rondin.get('fecha_inicio_rondin'):
            rondin['fecha_inicio_rondin'] = self.timestamp and datetime.fromtimestamp(self.timestamp, tz).strftime('%Y-%m-%d %H:%M:%S')

        conf_recorrido = {}
        for key, value in rondin.items():
            if key == 'fecha_programacion':
                answers[self.f['fecha_programacion']] = value
            elif key == 'fecha_inicio_rondin':
                answers[self.f['fecha_inicio_rondin']] = value
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
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            area_name = (
                                item.get('incidente_area') or 
                                item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.f['nombre_area'], '')
                            )
                            if area_name:
                                areas_list.append({
                                    self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                        self.f['nombre_area']: area_name
                                    },
                                    self.f['fecha_hora_inspeccion_area']: item.get('fecha_hora_inspeccion_area', ''),
                                    self.f['foto_evidencia_area_rondin']: item.get('foto_evidencia_area_rondin', []),
                                    self.f['comentario_area_rondin']: item.get('comentario_area_rondin', ''),
                                    self.f['url_registro_rondin']: item.get('url_registro_rondin', '')
                                })
                
                for cache_item in cache:
                    data_cache = cache_item.get('check_data', {})
                    area_name = self.unlist(
                        data_cache.get(self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
                        .get(self.Location.f['area'], '')
                    )
                    
                    if area_name:
                        areas_list.append({
                            self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.f['nombre_area']: area_name
                            },
                            self.f['fecha_hora_inspeccion_area']: cache_item.get('timestamp') and datetime.fromtimestamp(cache_item['timestamp'], tz).strftime('%Y-%m-%d %H:%M:%S'),
                            self.f['foto_evidencia_area_rondin']: data_cache.get(self.f['foto_evidencia_area'], []),
                            self.f['comentario_area_rondin']: data_cache.get(self.f['comentario_check_area'], ''),
                            self.f['url_registro_rondin']: f"https://app.linkaform.com/#/records/detail/{cache_item.get('_id', '')}",
                        })
            
            all_areas_sorted = sorted(
                areas_list,
                key=lambda x: self.parse_date_for_sorting(x.get(self.f['fecha_hora_inspeccion_area'], ''))
            )
            
            answers[self.f['areas_del_rondin']] = all_areas_sorted
        else:
            pass

        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = conf_recorrido
        answers[self.f['estatus_del_recorrido']] = 'en_proceso' if rondin_en_progreso else 'realizado'
        answers[self.f['fecha_fin_rondin']] = today if data_rondin.get(self.f['check_status'], '') == ['finalizado', 'realizado', 'cerrado'] else ''
        
        format_list_incidencias = []
        for incidencia in rondin.get('bitacora_rondin_incidencias', []):
            inc = incidencia.get(self.f['tipo_de_incidencia'])
            if inc:
                incidencia.pop(self.f['tipo_de_incidencia'], None)
                incidencia.update({
                    self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                        self.f['tipo_de_incidencia']: inc
                    }
                })
                format_list_incidencias.append(incidencia)
            
        rondin['bitacora_rondin_incidencias'] = format_list_incidencias
             
        for incidencia in data_rondin.get(self.f['grupo_incidencias_check'], []):
            rondin['bitacora_rondin_incidencias'].append(incidencia)
        
        incidencias_list = rondin['bitacora_rondin_incidencias']
        incidencias_dict = {str(idx): incidencia for idx, incidencia in enumerate(incidencias_list)}
        answers[self.f['bitacora_rondin_incidencias']] = incidencias_dict
        
        if data_rondin.get('answers', {}).get(self.f['check_status']) == 'finalizado':
            answers[self.f['estatus_del_recorrido']] = 'realizado'

        # print("ans", simplejson.dumps(answers, indent=4))

        if answers:
            metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_RONDINES)
            metadata.update(self.get_record_by_folio(rondin.get('folio'), self.BITACORA_RONDINES, select_columns={'_id': 1}, limit=1))

            metadata.update({
                'properties': {
                    "device_properties": {
                        "system": "Addons",
                        "process":"Actualizacion de Bitacora", 
                        "accion":'rondines_cache', 
                        "folio": rondin.get('folio'), 
                        "archive": "rondines_cache.py"
                    }
                },
                'answers': answers,
                '_id': rondin.get('_id')
            })
            res = self.net.patch_forms_answers(metadata)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res
            
    def get_unique_locations_from_cache(self):
        """
        Get unique locations from cache.
        """
        pipeline = [
            {'$group': {'_id': '$location'}},
            {'$project': {'location': '$_id', '_id': 0}}
        ]
        resp = self.cr_cache.aggregate(pipeline)
        return [item['location'] for item in resp if item['location']]

    def find_best_bitacora_and_position(self, bitacoras, dt_check):
        """
        Busca la mejor bitácora y posición para un check previo erroneo.
        """
        tz = pytz.timezone('America/Mexico_City')
        best_bitacora = None
        best_idx = None
        min_time_diff = None

        for bitacora in bitacoras:
            areas = bitacora.get('areas_del_rondin', [])
            if not areas:
                best_bitacora = bitacora
                best_idx = 0
                min_time_diff = 0
                continue

            for idx in range(len(areas) + 1):
                prev_time = None
                next_time = None
                if idx > 0:
                    prev_time = tz.localize(datetime.strptime(areas[idx - 1]['fecha_hora_inspeccion_area'], '%Y-%m-%d %H:%M:%S'))
                if idx < len(areas):
                    next_time = tz.localize(datetime.strptime(areas[idx]['fecha_hora_inspeccion_area'], '%Y-%m-%d %H:%M:%S'))
                fits = True
                if prev_time and dt_check < prev_time:
                    fits = False
                if next_time and dt_check > next_time:
                    fits = False
                if prev_time and next_time:
                    fits = prev_time <= dt_check <= next_time
                elif prev_time:
                    fits = dt_check >= prev_time
                elif next_time:
                    fits = dt_check <= next_time
                if fits:
                    diff = 0
                    if prev_time:
                        diff += abs((dt_check - prev_time).total_seconds())
                    if next_time:
                        diff += abs((next_time - dt_check).total_seconds())
                    if min_time_diff is None or diff < min_time_diff:
                        best_bitacora = bitacora
                        best_idx = idx
                        min_time_diff = diff

        return best_bitacora, best_idx

    def update_closed_bitacora(self, rondin):
        """
        Actualiza la bitácora cerrada con la información del rondín.
        """
        tz = pytz.timezone('America/Mexico_City')
        today = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        rondin = self.unlist(rondin)
        rondin_en_progreso = True
        answers={}
        areas_list = []

        if not rondin.get('fecha_inicio_rondin'):
            rondin['fecha_inicio_rondin'] = self.timestamp and datetime.fromtimestamp(self.timestamp, tz).strftime('%Y-%m-%d %H:%M:%S')

        conf_recorrido = {}
        for key, value in rondin.items():
            if key == 'fecha_programacion':
                answers[self.f['fecha_programacion']] = value
            elif key == 'fecha_inicio_rondin':
                answers[self.f['fecha_inicio_rondin']] = value
            elif key == 'fecha_fin_rondin':
                answers[self.f['fecha_fin_rondin']] = value
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
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            area_name = (
                                item.get('incidente_area') or 
                                item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.f['nombre_area'], '')
                            )
                            if area_name:
                                areas_list.append({
                                    self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                        self.f['nombre_area']: area_name
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
            
            answers[self.f['areas_del_rondin']] = all_areas_sorted
        else:
            pass

        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = conf_recorrido
        
        format_list_incidencias = []
        for incidencia in rondin.get('bitacora_rondin_incidencias', []):
            inc = incidencia.get(self.f['tipo_de_incidencia'])
            if inc:
                incidencia.pop(self.f['tipo_de_incidencia'], None)
                incidencia.update({
                    self.LISTA_INCIDENCIAS_CAT_OBJ_ID: {
                        self.f['tipo_de_incidencia']: inc
                    }
                })
                format_list_incidencias.append(incidencia)
            
        rondin['bitacora_rondin_incidencias'] = format_list_incidencias
             
        for incidencia in data_rondin.get(self.f['grupo_incidencias_check'], []):
            rondin['bitacora_rondin_incidencias'].append(incidencia)
        
        incidencias_list = rondin['bitacora_rondin_incidencias']
        incidencias_dict = {str(idx): incidencia for idx, incidencia in enumerate(incidencias_list)}
        answers[self.f['bitacora_rondin_incidencias']] = incidencias_dict

        # print("ans", simplejson.dumps(answers, indent=4))

        if answers:
            metadata = self.lkf_api.get_metadata(form_id=self.BITACORA_RONDINES)
            metadata.update(self.get_record_by_folio(rondin.get('folio'), self.BITACORA_RONDINES, select_columns={'_id': 1}, limit=1))

            metadata.update({
                'properties': {
                    "device_properties": {
                        "system": "Addons",
                        "process":"Actualizacion de Bitacora", 
                        "accion":'rondines_cache', 
                        "folio": rondin.get('folio'), 
                        "archive": "rondines_cache.py"
                    }
                },
                'answers': answers,
                '_id': rondin.get('_id')
            })
            res = self.net.patch_forms_answers(metadata)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    script_obj.cr_cache = script_obj.net.get_collections(collection='rondin_caches')
    # print(simplejson.dumps(script_obj.answers, indent=3))
    data_rondin = json.loads(sys.argv[1])
    script_obj.timestamp = data_rondin.get('start_timestamp', '')
    # script_obj.clear_cache()
    # breakpoint()

    data = script_obj.data.get('data', {})
    location = script_obj.answers.get(script_obj.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
    script_obj.location = script_obj.unlist(location.get(script_obj.Location.f['location'], ''))
    check_area = script_obj.answers.get(script_obj.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
    script_obj.check_area = script_obj.unlist(check_area.get(script_obj.Location.f['area'], ''))
    validacion_area = False
    
    #! Validacion inicial: Cerrar bitacoras fuera de tiempo
    rondines = script_obj.get_rondines_by_status()
    if rondines:
        res = script_obj.close_rondines(rondines)
        if res and res.get('status_code') in [200, 201, 202]:
            validacion_area = True
            
    #! Si el check no es de hoy se verifica si pertenece a una bitacora ya cerrada
    tz = pytz.timezone('America/Mexico_City')
    dt_check = datetime.fromtimestamp(script_obj.timestamp, tz)
    date_from = (dt_check - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    date_to = (dt_check + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    today_str = datetime.now(tz).strftime('%Y-%m-%d')

    if not date_from.startswith(today_str):
        bitacoras = script_obj.search_closed_bitacora_by_check(date_from=date_from, date_to=date_to)
        if bitacoras:
            my_check = {
                'duracion_traslado_area': 0.0,
                'comentario_area_rondin': script_obj.answers.get(script_obj.f['comentario_check_area'], ''),
                'foto_evidencia_area_rondin': script_obj.answers.get(script_obj.f['foto_evidencia_area'], []),
                'fecha_hora_inspeccion_area': dt_check.strftime('%Y-%m-%d %H:%M:%S'),
                'url_registro_rondin': f'https://app.linkaform.com/#/records/detail/{script_obj.record_id}',
                'incidente_area': script_obj.check_area,
            }

            winner_bitacora, insert_idx = script_obj.find_best_bitacora_and_position(bitacoras, dt_check)
            if winner_bitacora is not None and insert_idx is not None:
                areas = winner_bitacora['areas_del_rondin']
                areas.insert(insert_idx, my_check)
                script_obj.update_closed_bitacora(winner_bitacora)
            
    recorridos = script_obj.search_rondin_by_area()
    exists_bitacora = script_obj.search_active_bitacora_by_rondin(recorridos=recorridos)
            
    if exists_bitacora and not validacion_area:
        #! Si existe una bitacora activa se hace el check en esa bitacora
        resp = script_obj.create_cache()
        time.sleep(5)
        cache = script_obj.search_cache(location=script_obj.location)
        
        should_create_new = False
        finalized_timestamp = None
        
        #! Se verifica si hay un check con status finalizado
        for cache_item in cache:
            check_status = cache_item.get('check_data', {}).get(script_obj.f['check_status'])
            if check_status == 'finalizado':
                finalized_timestamp = cache_item.get('timestamp')
                break
        
        if finalized_timestamp and script_obj.timestamp > finalized_timestamp:
            #! Si un check no tiene un timestamp menor al check con status finalizado 
            #! se manda a crear una nueva bitacora
            should_create_new = True

        if should_create_new:
            script_obj.clear_cache(record_id=script_obj.record_id)
            validacion_area = True
            time.sleep(5)
        else:
            #! Se actualiza bitacora activa con cache filtrado o cache normal, dependiendo si venia un status finalizado
            winner = script_obj.select_winner(caches_list=cache, location=script_obj.location)

            if winner and winner.get('folio', '') == script_obj.folio:
                if finalized_timestamp:
                    cache_filtered = [
                        cache_item for cache_item in cache 
                        if cache_item.get('timestamp', 0) <= finalized_timestamp
                    ]
                    data_rondin['answers'][script_obj.f['check_status']] = 'finalizado'
                    script_obj.check_areas_in_rondin(cache=cache_filtered, rondin=exists_bitacora)
                    for cache_item in cache_filtered:
                        script_obj.clear_cache(record_id=str(cache_item['_id']))
                else:
                    script_obj.check_areas_in_rondin(cache=cache, rondin=exists_bitacora)
                    script_obj.clear_cache(location=script_obj.location)
    else:
        validacion_area = True

    if validacion_area:
        #! Si no existe una bitacora activa se busca un ganador en el cache
        exists_winner = script_obj.search_cache(search_winner=True, location=script_obj.location)
        if exists_winner and script_obj.unlist(exists_winner).get('folio', '') != script_obj.folio:
            #! Si existe un ganador en el cache se agrega en su lista de checks
            checks_list = [{
                "area": script_obj.answers.get(script_obj.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(script_obj.Location.f['area'], []),
                "record_id": script_obj.record_id,
                "timestamp": script_obj.timestamp,
                "check_data": script_obj.answers
            }]
            script_obj.add_checks_to_winner(winner=script_obj.unlist(exists_winner), checks_list=checks_list)
        else:
            #! Si no existe un ganador en el cache se crea un cache
            resp = script_obj.create_cache()
            time.sleep(5)
            #! Se obtienen las ubicaciones de los caches creados
            locations = script_obj.get_unique_locations_from_cache()
            for location in locations:
                start_time = time.time()
                max_duration = 60
                bitacoras_created = False
                
                while True:
                    #! Validacion para evitar un loop infinito en caso de error inesperado
                    if time.time() - start_time > max_duration:
                        print(f'⚠️  Timeout alcanzado para {location}. Forzando salida.')
                        script_obj.clear_cache(location=location)
                        break
                    
                    cache = script_obj.search_cache(location=location)
                    if not cache:
                        print(f'Cache vacío para {location}. Terminando procesamiento.')
                        break
                        
                    finalized_timestamp = None
                    earliest_finalized = None

                    #! Se busca el timestamp más temprano de los caches finalizados
                    for cache_item in cache:
                        check_status = cache_item.get('check_data', {}).get(script_obj.f['check_status'])
                        if check_status == 'finalizado':
                            item_timestamp = cache_item.get('timestamp')
                            if earliest_finalized is None or item_timestamp < earliest_finalized:
                                earliest_finalized = item_timestamp
                                finalized_timestamp = item_timestamp

                    if finalized_timestamp:
                        #! Se filtran los caches hasta el timestamp finalizado si hay un check finalizado
                        cache_to_process = [
                            cache_item for cache_item in cache 
                            if cache_item.get('timestamp', 0) <= finalized_timestamp
                        ]
                        is_finalized_bitacora = True
                    else:
                        cache_to_process = cache
                        is_finalized_bitacora = False
                    
                    winner = script_obj.select_winner(caches_list=cache_to_process, location=location)
                    
                    if winner and winner.get('_id') == ObjectId(script_obj.record_id):
                        #! Se agrega la información de los checks al ganador y se crea la bitacora
                        script_obj.add_checks_to_winner(winner=winner, checks_list=cache_to_process)
                        time.sleep(2)
                        script_obj.add_checks_to_winner(winner=winner, checks_list=cache_to_process)
                        final_winner = script_obj.unlist(script_obj.search_cache(search_winner=True, location=location))
                        
                        final_winner['check_data'] = final_winner.get('check_data', {})
                        final_winner['check_data'][script_obj.f['check_status']] = 'en_proceso'

                        if is_finalized_bitacora:
                            has_finalized_check = any(
                                cache_item.get('check_data', {}).get(script_obj.f['check_status']) == 'finalizado'
                                for cache_item in cache_to_process
                            )
                            
                            if has_finalized_check:
                                final_winner['check_data'] = final_winner.get('check_data', {})
                                final_winner['check_data'][script_obj.f['check_status']] = 'finalizado'

                        script_obj.create_bitacora_from_cache(winner=final_winner, recorridos_names=recorridos)
                        
                        for cache_item in cache_to_process:
                            script_obj.clear_cache(record_id=str(cache_item['_id']))
                        script_obj.clear_cache(winner=True)
                        
                        bitacoras_created = True
                        print(f'✅ Bitácora {"finalizada" if is_finalized_bitacora else "en proceso"} creada por {script_obj.folio}')
                        
                    elif winner:
                        #! Mientras exista ganador los demas checks entran en espera por si otro resulta ganador
                        if not bitacoras_created:
                            # print(f'❌ No soy el ganador para {location}. Esperando...')
                            time.sleep(2)
                            continue
                        else:
                            # print(f'✅ Bitácoras ya creadas para {location}. Saliendo.')
                            break
                    else:
                        break
    cache = script_obj.search_cache()
    for c in cache:
        c['_id'] = str(c['_id'])
    print('Cache====>', simplejson.dumps(cache, indent=3))