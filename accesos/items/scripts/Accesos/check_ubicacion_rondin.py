# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

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
            'areas_del_rondin': '66462aa5d4a4af2eea07e0d1'
        })

    def get_data_area_rondin(self, answers):
        """
        Recibe: Las answers de el registro emitido en lkf
        Retorna: Un objeto con el area del rondin y la id de la ubicacion
        Error: Arroja una exception
        """
        cat_area_rondin = answers.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
        area_rondin = cat_area_rondin.get(self.mf['nombre_area'], [])[0]
        id_area_rondin = cat_area_rondin.get(self.f['location_id'], '')

        if not area_rondin or not id_area_rondin:
            raise Exception('No se ha encontrado el area del rondin o la id de la ubicacion')

        data = {
            'area_rondin': area_rondin,
            'id_area_rondin': id_area_rondin
        }

        return data
    
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
    
    def search_rondin_by_name(self, name='Recorrido Uno', status_list=['programado', 'en_proceso']):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.CONFIGURACION_RECORRIDOS_OBJ_ID}.{self.f['nombre_del_recorrido_en_catalog']}": name,
                f"answers.{self.f['estatus_del_recorrido']}": {"$in": status_list},
            }},
            {'$project': {
                '_id': 1,
            }},
        ]

        rondin = self.format_cr(self.cr.aggregate(query))
        return rondin
    
    def check_area_in_rondin(self, area_rondin, id_rondin='682b9f78328f25bbb4297c4a'):
        answers={}

        #REVISARRRRRRRRRRRRRRRRRRRRRRR
        answers[self.f['areas_del_rondin']] = [
            {
                '680ffa2c87c49a88ab7ba9b6': {
                    '663e5d44f5b8a7ce8211ed0f': area_rondin
                },
                '6760a908a43b1b0e41abad6b': '2025-05-10 12:30:33',
                '66462b9d7124d1540f962087': {},
                '66462b9d7124d1540f962088': 'comentariotest',
                '6750adb2936622aecd075607': '',
                '6760a9581e31b10a38a22f1f': ''
            },
            {
                '680ffa2c87c49a88ab7ba9b6': {
                    '663e5d44f5b8a7ce8211ed0f': 'Area de Prueba'
                },
                '6760a908a43b1b0e41abad6b': '',
                '66462b9d7124d1540f962087': {},
                '66462b9d7124d1540f962088': '',
                '6750adb2936622aecd075607': '',
                '6760a9581e31b10a38a22f1f': ''
            },
            {
                '680ffa2c87c49a88ab7ba9b6': {
                    '663e5d44f5b8a7ce8211ed0f': 'Área de químicos y residuos peligrosos'
                },
                '6760a908a43b1b0e41abad6b': '',
                '66462b9d7124d1540f962087': {},
                '66462b9d7124d1540f962088': '',
                '6750adb2936622aecd075607': '',
                '6760a9581e31b10a38a22f1f': ''
            }
        ]

        print("ans", simplejson.dumps(answers, indent=4))

        if answers:
            res= self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=[id_rondin])
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()
    print('answers', acceso_obj.answers)
    
    data_area = acceso_obj.get_data_area_rondin(acceso_obj.answers)
    print('data_area', data_area)

    recorridos = acceso_obj.get_recorridos_by_area(data_area['area_rondin'])
    print('recorridos', recorridos)

    rondines_programados = acceso_obj.search_rondin_by_name()
    print('rondines_programados', rondines_programados)

    resultado = acceso_obj.check_area_in_rondin(area_rondin=data_area['area_rondin'])
    print('resultado', resultado)
