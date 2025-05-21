# coding: utf-8
import sys, simplejson
from datetime import datetime
import pytz

from accesos_utils import Accesos

from account_settings import *

class Accesos(Accesos):

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
            {'$unwind':f"$answers.{self.f['grupo_de_areas_recorrido']}"},
            {'$project':
                {'area':f"$answers.{self.f['grupo_de_areas_recorrido']}.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area']}",
                'nombre_recorrido':f"$answers.{self.f['nombre_del_recorrido']}"
                }
            },
            {'$match':
                {'area':area_rondin}
            }
        ]
        recorridos = self.format_cr(self.cr.aggregate(query))
        return recorridos

    def search_rondin_by_name(self, names=[], status_list=['programado', 'en_proceso']):
        """
        Recibe: Una lista de nombres de recorridos y una lista de estatus para filtrar los recorridos
        Retorna: En formato de lista el primer rondin que cumpla con los criterios
        Error: 
        """
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
                'fecha_programacion': f"$answers.{self.f['fecha_programacion']}",
                'answers': f"$answers"
            }},
            {'$sort': {'fecha_programacion': 1}},
            {'$limit': 1}
        ]

        rondin = self.format_cr(self.cr.aggregate(query), **{'labels_off': True})
        return rondin
    
    def check_area_in_rondin(self, data_rondin, area_rondin, rondin):
        """
        Recibe: Las answers del check de ubicacion, el area que se hice check de ubicacion y el registro de rondin
        Retorna: La respuesta de la api al hacer el patch de un registro
        Error: La respuesta de la api al hacer el patch de un registro
        """
        tz = pytz.timezone('America/Mexico_City')
        today = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        rondin = self.unlist(rondin)
        format_id_rondin = rondin.get('_id', '')
        answers={}

        print('rondinnnnnnnnnnnnnnnnn', simplejson.dumps(rondin, indent=3))

        if not rondin.get('fecha_inicio_rondin'):
            rondin['fecha_inicio_rondin'] = today

        conf_recorrido = {}
        for key, value in rondin.items():
            if key == 'fecha_programacion':
                answers[self.f['fecha_programacion']] = value
            elif key == 'fecha_inicio_rondin':
                answers[self.f['fecha_inicio_rondin']] = value
            elif key == 'ubicacion_recorrido':
                conf_recorrido.update({
                    self.f['ubicacion_recorrido']: value
                })
            elif key == 'nombre_del_recorrido_en_catalog':
                conf_recorrido.update({
                    self.f['nombre_del_recorrido_en_catalog']: value
                })
            elif key == 'estatus_del_recorrido':
                answers[self.f['estatus_del_recorrido']] = value
            elif key == 'areas_del_rondin':
                areas_rondin = {}
                items = []
                for index, item in enumerate(value):
                    if item.get('note_booth', '') == area_rondin:
                        obj = {
                            self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.f['nombre_area']: area_rondin
                            },
                            self.f['fecha_hora_inspeccion_area']: today,
                            self.f['foto_evidencia_area_rondin']: data_rondin.get(self.f['foto_evidencia_area'], []),
                            self.f['comentario_area_rondin']: data_rondin.get(self.f['comentario_check_area'], '')
                        }
                    else:
                        obj = {
                            self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID: {
                                self.f['nombre_area']: item.get('note_booth', '')
                            },
                            self.f['fecha_hora_inspeccion_area']: item.get('fecha_hora_inspeccion_area', ''),
                            self.f['foto_evidencia_area_rondin']: item.get('foto_evidencia_area_rondin', []),
                            self.f['comentario_area_rondin']: item.get('comentario_area_rondin', '')
                        }
                    items.append(obj)

                items_sorted = sorted(
                    items,
                    key=lambda x: not bool(x.get(self.f['fecha_hora_inspeccion_area'], '').strip())
                )

                rondin_en_progreso = True
                for idx, obj in enumerate(items_sorted):
                    if not obj.get(self.f['fecha_hora_inspeccion_area']):
                        rondin_en_progreso = True
                    else:
                        rondin_en_progreso = False
                    areas_rondin[str(idx)] = obj

                answers[self.f['areas_del_rondin']] = areas_rondin
                print(rondin_en_progreso)
            else:
                pass

        answers[self.CONFIGURACION_RECORRIDOS_OBJ_ID] = conf_recorrido
        answers[self.f['estatus_del_recorrido']] = 'en_proceso' if rondin_en_progreso else 'realizado'
        answers[self.f['fecha_fin_rondin']] = today if not rondin_en_progreso else ''

        print("ans", simplejson.dumps(answers, indent=4))

        # print(stop)
        if answers:
            res= self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=[format_id_rondin])
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res
            
    def create_rondin(self):
        #TODO Crear un rondin
        pass

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()
    print('answers', acceso_obj.answers)
    acceso_obj.load(module='Location', **acceso_obj.kwargs)
    cat_area_rondin = acceso_obj.answers.get(acceso_obj.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {})
    nombre_area_rondin = acceso_obj.unlist(cat_area_rondin.get(acceso_obj.f['nombre_area'], []))
    # print('nombre_area_rondin', nombre_area_rondin)

    nombres_recorrido = acceso_obj.get_recorridos_by_area(nombre_area_rondin)
    # print('nombres_recorrido', nombres_recorrido)

    rondin = acceso_obj.search_rondin_by_name(names=nombres_recorrido)
    print('rondin', rondin)

    if not rondin:
        # acceso_obj.create_rondin()
        print('No se encontro un rondin con el area proporcionada')
    else:
        resultado = acceso_obj.check_area_in_rondin(data_rondin=acceso_obj.answers, area_rondin=nombre_area_rondin, rondin=rondin)
        print('resultado', resultado)
