# coding: utf-8
#####
# Script para actualizar 
#####
import sys, simplejson, json

from accesos_utils import Accesos
from account_settings import *

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.configuracion_area = {
            'qr_area': '68487646684fe30a8f9f3ef3',
            'foto_area': '68487646684fe30a8f9f3ef4',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'area': '663e5d44f5b8a7ce8211ed0f',
            'option': '68487646684fe30a8f9f3ef2'
        }

        self.area_update = {
            'foto_area': '6763096aa99cee046ba766ad',
            'tag_id_area': '6762f7b0922cc2a2f57d4044',
            'tipo_area': '663e5e68f5b8a7ce8211ed18',
            'nombre_direccion': '663a7e0fe48382c5b1230901',
            'estatus_area': '663e5e4bf5b8a7ce8211ed15',
            'estatus': '663e5e4bf5b8a7ce8211ed14',
            'qr_area': '663e5e4bf5b8a7ce8211ed13'
        }

    def format_data_area(self, data):
        formatted_data = {}

        if data.get(self.configuracion_area['qr_area']):
            formatted_data.update({
                'qr_area': data.get(self.configuracion_area['qr_area'])
            })

        if data.get(self.configuracion_area['foto_area']):
            formatted_data.update({
                'foto_area': data.get(self.configuracion_area['foto_area'])
            })

        formatted_data.update({
            'option': data.get(self.configuracion_area['option'], ''),
            'ubicacion': data.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.configuracion_area['ubicacion'], ''),
            'area': data.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.configuracion_area['area'], ''),
        })

        return formatted_data

    def get_record_ubicacion(self, ubicacion, area, data):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.AREAS_DE_LAS_UBICACIONES,
        }

        if data.get('option') == 'actualizar_foto_con_scann_de_qr':
            match_query.update({
                f"answers.{self.area_update['tag_id_area']}": data.get('qr_area')
            })
        elif data.get('option') == 'acutalizacion_de_qr' or data.get('option') == 'actualizar_foto_con_seleccion_de_nombre':
            match_query.update({
                f"answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.configuracion_area['ubicacion']}": ubicacion,
                f"answers.{self.configuracion_area['area']}": area
            })

        query = [
            {'$match': match_query },
            {'$project': {
                'folio': {'$ifNull': ['$folio', '']},
                '_id': 1,
                'ubicacion': f"$answers.{self.UBICACIONES_CAT_OBJ_ID}.{self.configuracion_area['ubicacion']}",
                'area': f"$answers.{self.configuracion_area['area']}",
                'tag_id_area': {'$ifNull': [f"$answers.{self.area_update['tag_id_area']}", '']},
                'foto_area': {'$ifNull': [f"$answers.{self.area_update['foto_area']}", []]},
                'tipo_area': f"$answers.{self.TIPO_AREA_OBJ_ID}.{self.area_update['tipo_area']}",
                'nombre_direccion': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['nombre_direccion']}",
                'estatus_area': f"$answers.{self.area_update['estatus_area']}",
                'estatus': f"$answers.{self.area_update['estatus']}",
                'qr_area': f"$answers.{self.area_update['qr_area']}"
            }},
            {'$limit': 1},
            {'$sort':{'folio':-1}},
        ]
        res = self.format_cr(self.cr.aggregate(query))
        res = self.unlist(res)
        if not res and data.get('option') == 'actualizar_foto_con_scann_de_qr':
            raise Exception("No se encontró un registro de área para el QR proporcionado. Intenta asignandole uno nuevo.")
        return res

    def update_area(self, data):
        ubicacion = data.get('ubicacion', '')
        area = data.get('area', '')
        area_ubicacion_data = self.get_record_ubicacion(ubicacion=ubicacion, area=area, data=data)
        print('area_ubicacion_data', area_ubicacion_data)
        folio = area_ubicacion_data.get('folio', '')
        record_id = area_ubicacion_data.get('_id', '')

        if data.get('option') == 'actualizar_foto_con_scann_de_qr' or data.get('option') == 'actualizar_foto_con_seleccion_de_nombre':
            data.pop('qr_area', None)

        answers={}

        for key, value in area_ubicacion_data.items():
            if key == 'area':
                answers[self.configuracion_area['area']] = value # type: ignore
            elif key == 'ubicacion':
                answers[self.UBICACIONES_CAT_OBJ_ID] = {
                    self.configuracion_area['ubicacion']: value, # type: ignore
                }
            elif key == 'tipo_area':
                answers[self.TIPO_AREA_OBJ_ID] = {
                    self.area_update['tipo_area']: value # type: ignore
                }
            elif key == 'nombre_direccion':
                answers[self.CONTACTO_CAT_OBJ_ID] = {
                    self.area_update['nombre_direccion']: value # type: ignore
                }
            elif key == 'estatus_area':
                answers[self.area_update['estatus_area']] = value
            elif key == 'estatus':
                answers[self.area_update['estatus']] = value
            elif key == 'tag_id_area':
                answers[self.area_update['tag_id_area']] = data.get('qr_area', value)
            elif key == 'qr_area':
                answers[self.area_update['qr_area']] = value
            elif key == 'foto_area':
                answers[self.area_update['foto_area']] = data.get('foto_area', value)
            else:
                pass

        if answers:
            metadata = self.lkf_api.get_metadata(form_id=self.AREAS_DE_LAS_UBICACIONES)
            metadata.update({
                'properties': {
                    "device_properties":{
                        "system": "Addons",
                        "process":"Actualizacion de Area", 
                        "accion":'update_area', 
                        "folio": folio, 
                        "archive": "incidencias.py"
                    }
                },
                'answers': answers,
                '_id': record_id
            })

            res = self.net.patch_forms_answers(metadata)
            if res.get('status_code') in [200, 201, 202]:
                return f'Area actualizada correctamente: {folio}'
            else:
                return f'Error al actualizar el area: {folio}'

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    print('answers', simplejson.dumps(acceso_obj.answers, indent=3))

    data = acceso_obj.format_data_area(acceso_obj.answers)
    response = acceso_obj.update_area(data)

    acceso_obj.HttpResponse({"data": response})
