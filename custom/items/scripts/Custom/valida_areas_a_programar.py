# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from bson import ObjectId
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.field_planta = "696130ce57ba2b8308adef4d"
        self.field_area = "696133f1829d117f5e819e8d"
        self.field_responsable = "638a9a7767c332f5d459fc81"
        self.field_grupo_areas = "69dfc84f6748944372b3d533"

    def get_areas_by_planta(self, planta):
        query = {"$and":[ {self.field_planta: {'$eq': planta}} ]}
        records_catalog = lkf_obj.lkf_api.search_catalog_answers(145464, query)
        # print('\n records_catalog =',records_catalog)

        return { 
            rec[self.field_area]: rec.get(self.field_responsable) 
            for rec in records_catalog 
            if rec.get(self.field_area) 
        }

    def valida_programacion_de_areas(self):
        print('... ... Validando Areas a Programar ... ...')
        planta = answers.get('696130ce57ba2b8308adef4c', {}).get(self.field_planta)
        if not planta:
            print('[ERROR] Planta no seleccionada')
            return

        # Se consulta en el catalogo Plantas y areas (145464)
        map_area_responsable = self.get_areas_by_planta(planta)

        # VALIDACIONES. Se itera el grupo de Areas a programar
        areas_selected = set()
        errores = []
        for index_area, item_area in enumerate( answers.get(self.field_grupo_areas, []) ):
            posicion = index_area + 1
            area = item_area.get('696133f0829d117f5e819e8c', {}).get(self.field_area)

            if not area:
                errores.append(f"Set {posicion}: No hay área definida")
                continue

            if area in areas_selected:
                errores.append(f"Set {posicion}: El área '{area}' está duplicada")
                continue

            areas_selected.add(area)

            # El usuario seleccionado debe ser diferente al responsable de area en el catalogo
            usuario_asignacion = item_area.get('696517d545ba5981006be647', {}).get(self.field_responsable)
            if map_area_responsable.get(area) == usuario_asignacion:
                errores.append(f"Set {posicion}: No es posible programar el area '{area}' al usuario '{usuario_asignacion}', ya que es el responsable de Área")

        # Todas las áreas deben estar cubiertas
        areas_totales = set( map_area_responsable.keys() )
        
        # Encontrar las áreas faltantes
        faltantes = areas_totales - areas_selected

        # si hay areas faltantes, se integran a los errores
        if faltantes:
            errores.append( f"Falta programar las áreas: {lkf_obj.list_to_str( list(faltantes) )}" )

        # si hay errores se muestra en pantalla
        if errores:
            msg_error_app = {
                self.field_grupo_areas:{
                    "msg": [ lkf_obj.list_to_str(errores) ],
                    "label": "Areas a programar",
                    "error":[]
                }
            }
            raise Exception(simplejson.dumps(msg_error_app))

if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    lkf_obj.console_run()

    answers = lkf_obj.answers

    lkf_obj.valida_programacion_de_areas()