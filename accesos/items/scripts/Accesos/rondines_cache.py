# coding: utf-8
from genericpath import exists
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
        answers[self.f['estatus_del_recorrido']] = 'en_proceso'
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
        print(simplejson.dumps(metadata, indent=3))
        # breakpoint()

        res = self.lkf_api.post_forms_answers(metadata)
        return res

    def clear_cache(self, location=None, record_id=None):
        """
        Clear collection rondin_caches.
        """
        if location:
            self.cr_cache.delete_many({'location': location})
        if record_id:
            self.cr_cache.delete_one({'_id': ObjectId(record_id)})
        else:
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
            
    recorridos = script_obj.search_rondin_by_area()
    exists_bitacora = script_obj.search_active_bitacora_by_rondin(recorridos=recorridos)
            
    if exists_bitacora and not validacion_area:
        #! Si existe una bitacora activa se hace el check en esa bitacora
        resp = script_obj.create_cache()
        time.sleep(5)
        cache = script_obj.search_cache(location=script_obj.location)
        winner = script_obj.select_winner(caches_list=cache, location=script_obj.location)

        if winner and winner.get('folio', '') == script_obj.folio:
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
            locations = script_obj.get_unique_locations_from_cache()
            for location in locations:
                cache = script_obj.search_cache(location=location)
                winner = script_obj.select_winner(caches_list=cache, location=location)
                
                if winner and winner.get('folio', '') == script_obj.folio:
                    script_obj.add_checks_to_winner(winner=winner, checks_list=cache)
                    time.sleep(5)
                    cache = script_obj.search_cache(location=location)
                    script_obj.add_checks_to_winner(winner=winner, checks_list=cache)
                    winner = script_obj.unlist(script_obj.search_cache(search_winner=True, location=location))
                    script_obj.create_bitacora_from_cache(winner=winner, recorridos_names=recorridos)
                    script_obj.clear_cache(location=location)
                else:
                    print(f'Byeee para {location}')
    cache = script_obj.search_cache()
    for c in cache:
        c['_id'] = str(c['_id'])
    print('Cache====>', simplejson.dumps(cache, indent=3))