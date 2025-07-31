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
        self.load(module='Location', **self.kwargs)

        self.configuracion_area = {
            'qr_area': '68487646684fe30a8f9f3ef3',
            'foto_area': '68487646684fe30a8f9f3ef4',
            'ubicacion': '663e5c57f5b8a7ce8211ed0b',
            'area': '663e5d44f5b8a7ce8211ed0f',
            'option': '68487646684fe30a8f9f3ef2',
            'create_area': '688a33d9e61fcd2c299ff39e',
            'nombre_nueva_area': '688a33d9e61fcd2c299ff39f',
        }

        self.area_update = {
            'foto_area': '6763096aa99cee046ba766ad',
            'tag_id_area': '6762f7b0922cc2a2f57d4044',
            'tipo_area': '663e5e68f5b8a7ce8211ed18',
            'nombre_direccion': '663a7e0fe48382c5b1230901',
            'estatus_area': '663e5e4bf5b8a7ce8211ed15',
            'estatus': '663e5e4bf5b8a7ce8211ed14',
            'qr_area': '663e5e4bf5b8a7ce8211ed13',
            'pais_area': '663a7ca6e48382c5b12308fa',
            'ciudad_area': '6654187fc85ce22aaf8bb070',
            'colonia_area': '663a7f79e48382c5b123090a',
            'direccion_area': '663a7e0fe48382c5b1230902',
            'geolocalizacion_area': '663e5c8cf5b8a7ce8211ed0c',
            'geolocalizacion_area_ubicacion': '688bac1ecfdcf8b16eb209b5'
        }
        
        self.f.update({
            'status_details': '6889337c4db2c8b3de148e77',
        })

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
            'create_area': True if data.get(self.configuracion_area['create_area']) == 'crear_nueva_area' else False,
            'nombre_nueva_area': data.get(self.configuracion_area['nombre_nueva_area'], ''),
            'geolocation_area': data.get(self.area_update['geolocalizacion_area_ubicacion'], {}), # type: ignore
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
        elif data.get('option') == 'actualización_de_qr' or data.get('option') == 'actualizar_foto_con_selección_de_nombre':
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
                'pais_area': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['pais_area']}",
                'ciudad_area': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['ciudad_area']}",
                'colonia_area': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['colonia_area']}",
                'direccion_area': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['direccion_area']}",
                'geolocalizacion_area': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.area_update['geolocalizacion_area']}",
                'geolocation_especific': f"$answers.{self.area_update['geolocalizacion_area_ubicacion']}",
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
            msg = 'No se encontró un registro de área para el QR proporcionado. Intenta asignandole uno nuevo.'
            self.LKFException({'msg': msg, 'title': 'Actualizacion de area'})
        if not res and data.get('option') == 'actualización_de_qr':
            msg = 'No se encontró un registro de área para la ubicación y área proporcionadas en la forma Areas de las Ubicaciones.'
            self.LKFException({'msg': msg, 'title': 'Actualizacion de area'})
        return res

    def update_area(self, data):
        ubicacion = data.get('ubicacion', '')
        area = data.get('area', '')
        area_ubicacion_data = self.get_record_ubicacion(ubicacion=ubicacion, area=area, data=data)
        print('area_ubicacion_data', simplejson.dumps(area_ubicacion_data, indent=3))
        folio = area_ubicacion_data.get('folio', '')
        record_id = area_ubicacion_data.get('_id', '')

        if data.get('option') == 'actualizar_foto_con_scann_de_qr' or data.get('option') == 'actualizar_foto_con_seleccion_de_nombre':
            data.pop('qr_area', None)

        answers={}
        geolocation_especific = {
            'latitude': area_ubicacion_data.get('latitude'),
            'longitude': area_ubicacion_data.get('longitude')
        }

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
                    self.area_update['nombre_direccion']: value,
                    self.area_update['pais_area']: area_ubicacion_data.get('pais_area', []),
                    self.area_update['ciudad_area']: area_ubicacion_data.get('ciudad_area', []),
                    self.area_update['colonia_area']: area_ubicacion_data.get('colonia_area', []),
                    self.area_update['direccion_area']: area_ubicacion_data.get('direccion_area', []),
                    self.area_update['geolocalizacion_area']: area_ubicacion_data.get('geolocalizacion_area', [])
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
            elif key == 'latitude' or key == 'longitude':
                answers[self.area_update['geolocalizacion_area_ubicacion']] = geolocation_especific # type: ignore
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

            response = self.net.patch_forms_answers(metadata)
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

    def get_contact_details(self, direccion):
        selector = {}
        
        selector.update({
            f"answers.{self.area_update['nombre_direccion']}": direccion, # type: ignore
        })
            
        fields = [
            "_id",
            f"answers.{self.area_update['nombre_direccion']}", # type: ignore
            f"answers.{self.area_update['pais_area']}",
            f"answers.{self.area_update['direccion_area']}",
            f"answers.{self.area_update['colonia_area']}",
            f"answers.{self.area_update['geolocalizacion_area']}",
            f"answers.{self.area_update['ciudad_area']}",
        ]
        
        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 1,
        }
        res = self.lkf_api.search_catalog(131890, mango_query)
        res = self.unlist(res)
        res.pop('_id', None)
        res.pop('_rev', None)
        res.pop('created_at', None)
        res.pop('updated_at', None)
        return res if res else {}

    def create_new_area(self, data):
        if not data.get('ubicacion'):
            msg = 'Debes dejar una ubicacion seleccionada en el catalogo.'
            self.LKFException({'msg': msg, 'title': 'Creacion de area'})
        if not data.get('nombre_nueva_area'):
            msg = 'Debes de rellenar el campo nombre del area para crear un area valida.'
            self.LKFException({'msg': msg, 'title': 'Creacion de area'})
            
        contact_details = self.get_contact_details(data.get('ubicacion', {}))
        
        answers = {
            self.mf['nombre_area']: data.get('nombre_nueva_area'),
            self.Location.UBICACIONES_CAT_OBJ_ID: {
                self.mf['nombre_ubicacion_salida']: data.get('ubicacion', ''),
            },
            self.Location.TIPO_AREA_OBJ_ID: {
                self.area_update['tipo_area']: 'Área Pública'
            },
            self.area_update['geolocalizacion_area_ubicacion']: data.get('geolocation_area', ''), #type: ignore
            self.CONTACTO_CAT_OBJ_ID: contact_details,
            self.area_update['estatus']: 'activa',
            self.area_update['estatus_area']: 'disponible',
        }
        
        response = self.create_register(
            module='Accesos',
            process='Creacion de una area',
            action='rondines',
            file='accesos/app.py',
            form_id=131892, #TODO Modularizar este ID
            answers=answers
        )
        
        if response.get('status') != 'success':
            msg = 'El area no fue creada correctamente, solicita a soporte revisar logs.'
            self.LKFException({'msg': msg, 'title': 'Creacion de area'})

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

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    print('answers', simplejson.dumps(acceso_obj.answers, indent=3))

    data = acceso_obj.format_data_area(acceso_obj.answers)
    if (data.get('option') == 'actualizar_foto_con_selección_de_nombre' or data.get('option') == 'actualización_de_qr') \
        and data.get('create_area'):
            acceso_obj.create_new_area(data)
            data['area'] = data.get('nombre_nueva_area')
        
    response = acceso_obj.update_area(data)
    if response:
        acceso_obj.answers[acceso_obj.f['status_details']] = response.get('message', 'No se pudo actualizar el área')

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers
    }))
