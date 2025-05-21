# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos
from datetime import datetime
import pytz

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.CONFIGURACION_DE_RECORRIDOS_FORM = self.lkm.form_id('configuracion_de_recorridos','id')
        self.BITACORA_RONDINES = self.lkm.form_id('bitacora_rondines','id')

        self.f.update({
            'nombre_del_recorrido': '6645050d873fc2d733961eba',
            'grupo_de_areas_recorrido': '6645052ef8bc829a5ccafaf5',
            'nombre_del_recorrido_en_catalog': '6644fb97e14dcb705407e0ef',
            'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            'comentario_area_rondin': '66462b9d7124d1540f962088',
            'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            'comentario_check_area': '681144fb0d423e25b42818d4',
            'foto_evidencia_area': '681144fb0d423e25b42818d2',
            'foto_evidencia_area_rondin': '66462b9d7124d1540f962087'
        })

    def get_recorridos_by_area(self, area_rondin):
        """
        Recibe: El area que se buscara en la configuracion de recorridos
        Retorna: Una lista de objetos con los nombres y ids de los recorridos que tengan esa area
        Error: Arroja una exception
        """
        if not area_rondin:
            raise Exception('No se proporciono el area a buscar en la configuracion de recorridos')

        query = [
            {
                '$match': {
                    'deleted_at': {'$exists': False},
                    'form_id': self.CONFIGURACION_DE_RECORRIDOS_FORM,
                    f"answers.{self.f['grupo_de_areas_recorrido']}": {'$exists': True}
                }
            },
            {
                '$addFields': {
                    'match_areas': {
                        '$filter': {
                            'input': f"$answers.{self.f['grupo_de_areas_recorrido']}",
                            'as': 'item',
                            'cond': {
                                '$eq': [
                                    {
                                        '$getField': {
                                            'field': self.f['nombre_area'],
                                            'input': {
                                                '$first': {
                                                    '$map': {
                                                        'input': {'$objectToArray': '$$item'},
                                                        'as': 'kv',
                                                        'in': '$$kv.v'
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    area_rondin
                                ]
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    'match_areas': {'$ne': []}
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'nombre_recorrido': f"$answers.{self.f['nombre_del_recorrido']}"
                }
            }
        ]

        recorridos = self.format_cr(self.cr.aggregate(query))
        return recorridos
    
    def search_rondin_by_name(self, names=[], status_list=['programado', 'en_proceso']):
        format_names = []
        for name in names:
            format_names.append(name.get('nombre_recorrido', ''))

        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.f['nombre_del_recorrido_en_catalog']}": {"$in": format_names},
                f"answers.{self.f['estatus_del_recorrido']}": {"$in": status_list},
            }},
            {'$project': {
                '_id': 1,
            }},
            {'$limit': 1}
        ]

        rondin = self.format_cr(self.cr.aggregate(query))
        return rondin
    
    def check_area_in_rondin(self, data_rondin, area_rondin, id_rondin):
        tz = pytz.timezone('America/Mexico_City')
        today = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

        format_id_rondin = id_rondin[0].get('_id', '')
        answers={}
        answers[self.f['areas_del_rondin']] = {}
        
        index=1
        rondin = {}
        rondin['grupo_areas'] = [
            {
                'nombre_area': area_rondin,
                'comentario_area': data_rondin.get(self.f['comentario_check_area']),
                'foto_evidencia_area': data_rondin.get(self.f['foto_evidencia_area']),
                'fecha_hora_inspeccion_area': today,
            }
        ]

        for index, item in enumerate(rondin.get('grupo_areas', [])):
            nombre_area = item.get('nombre_area', '')
            comentario_area = item.get('comentario_area', '')
            foto_evidencia_area = item.get('foto_evidencia_area', '')
            fecha_hora_inspeccion_area = item.get('fecha_hora_inspeccion_area', '')
            obj = {
                self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                    self.mf['nombre_area']: nombre_area
                },
                self.f['foto_evidencia_area_rondin']: foto_evidencia_area,
                self.f['comentario_area_rondin']: comentario_area,
                self.f['fecha_hora_inspeccion_area']: fecha_hora_inspeccion_area
            }
            answers[self.f['areas_del_rondin']][-index] = obj
        
        print("ans", simplejson.dumps(answers, indent=4))

        if answers:
            res= self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=[format_id_rondin])
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()
    print('answers', acceso_obj.answers)
    
    cat_area_rondin = acceso_obj.answers.get(acceso_obj.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
    nombre_area_rondin = cat_area_rondin.get(acceso_obj.mf['nombre_area'], [])[0]
    print(nombre_area_rondin)

    nombres_recorrido = acceso_obj.get_recorridos_by_area(nombre_area_rondin)
    print('nombres_recorrido', nombres_recorrido)

    rondin = acceso_obj.search_rondin_by_name(names=nombres_recorrido)
    print('rondin', rondin)

    resultado = acceso_obj.check_area_in_rondin(data_rondin=acceso_obj.answers, area_rondin=nombre_area_rondin, id_rondin=rondin)
    print('resultado', resultado)
