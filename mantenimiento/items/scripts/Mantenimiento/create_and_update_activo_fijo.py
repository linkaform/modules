# coding: utf-8
import sys, simplejson, json, pytz
from linkaform_api import settings
from account_settings import *
from datetime import datetime

from mantenimiento_utils import Mantenimiento

class Mantenimiento(Mantenimiento):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.load(module='Activo_Fijo', module_class='Vehiculo', import_as='ActivoFijo', **self.kwargs)
        self.load(module='Employee', **self.kwargs)

    def create_response(self, status, status_code, message="", data=[]):
        """
            Crea una respuesta estructurada.

            Parámetros:
                status (str): Indica que sucedio con la petición(error, success, etc.).
                status_code (int): Código de estado HTTP de la respuesta.
                message (str, opcional): Mensaje descriptivo de la respuesta.
                data (list, opcional): Datos devueltos en la respuesta.

            Retorna:
                dict: Diccionario con el estado, código, mensaje y datos.
        """
        response = {}
        response = {
            "status": status,
            "status_code": status_code,
            "message": message,
            "data": data
        }
        return response
    
    def format_data_to_create_activo_fijo(self, answers):
        mx_time = datetime.now(pytz.timezone("America/Mexico_City"))
        catalog_key = self.ActivoFijo.ACTIVOS_FIJOS_CAT_OBJ_ID
        marca = answers.get(catalog_key, {}).get(self.ActivoFijo.f['marca'], '')
        modelo = answers.get(catalog_key, {}).get(self.ActivoFijo.f['modelo'], '')

        if not marca:
            marca = answers.get(self.f['marca_campo'])
        if not modelo:
            modelo = answers.get(self.f['modelo_campo'])

        data = {
            'nombre_cliente': answers.get(self.CLIENTE_CAT_OBJ_ID, {}).get(self.f['nombre_comercial'], ''),
            'email_cliente': answers.get(self.CLIENTE_CAT_OBJ_ID, {}).get(self.f['email_contacto'], ''),
            'nombre_equipo': answers.get(catalog_key, {}).get(self.ActivoFijo.f['nombre_equipo']),
            'nick_eco': answers.get(self.f['nick_eco']),
            'nombre_direccion_contacto': answers.get(self.CONTACTO_CAT_OBJ_ID, {}).get(self.f['address_name'], ''),
            'geolocalizacion_contacto': answers.get(self.CONTACTO_CAT_OBJ_ID, {}).get(self.f['address_geolocation'], []),
            'telefono_contacto': answers.get(self.CONTACTO_CAT_OBJ_ID, {}).get(self.f['phone'], ''),
            'email_contacto': answers.get(self.CONTACTO_CAT_OBJ_ID, {}).get(self.f['email'], ''),
            'tipo_de_equipo': answers.get(catalog_key, {}).get(self.ActivoFijo.f['categoria']),
            'marca': marca,
            'modelo': modelo,
            'placa': answers.get(self.f['placa'], ''),
            'imagen_del_equipo': answers.get(catalog_key, {}).get(self.f['imagen_del_equipo'], []),
            'fecha_instalacion': mx_time.strftime("%Y-%m-%d"),
            'tecnico_asignado': answers.get(self.Employee.EMPLOYEE_OBJ_ID, {}).get(self.Employee.f['worker_name'], ''),
            'estatus': 'disponible',
            'estado': 'activo'
        }

        return data

    def create_activo_fijo(self, data={}):
        if not data:
            response = self.create_response("error", 500, f"No se envio informacion suficiente para agregar el registro")
            return response
        
        nombre_equipo = data.get('nombre_equipo')
        nick_eco = data.get('nick_eco')
        nuevo_nombre_equipo = nombre_equipo + '-' + nick_eco
        
        metadata = self.lkf_api.get_metadata(form_id=self.ActivoFijo.ACTIVOS_FIJOS)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Mantenimiento",
                    "Process": "Creación de Activo Fijo",
                    "Action": "create_activo_fijo",
                    "File": "mantenimiento/app.py"
                }
            },
        })

        answers = {}

        try:
            for key, value in data.items():
                if key == 'nombre_cliente':
                    answers[self.CLIENTE_CAT_OBJ_ID] = {
                        self.f['nombre_comercial']: value,
                        self.f['email']: data.get('email_cliente', ''),
                    }
                elif key == 'nombre_equipo':
                    answers[self.ActivoFijo.f['nombre_equipo']] = nuevo_nombre_equipo
                elif key == 'nombre_direccion_contacto':
                    answers[self.CONTACTO_CAT_OBJ_ID] = {
                        self.f['address_name']: value,
                        self.f['address_geolocation']: data.get('geolocalizacion_contacto', ''),
                        self.f['phone']: data.get('telefono_contacto', ''),
                        self.f['email']: data.get('email_contacto', '')
                    }
                elif key == 'tipo_de_equipo':
                    answers[self.ActivoFijo.TIPO_DE_EQUIPO_OBJ_ID] = {
                        self.ActivoFijo.f['tipo_equipo']: value[0]
                    }
                elif key == 'marca':
                    answers[self.ActivoFijo.MODELO_OBJ_ID] = {
                        self.ActivoFijo.f['marca']: value,
                        self.ActivoFijo.f['modelo']: data.get('modelo', ''),
                        self.ActivoFijo.f['categoria_marca']: data.get('tipo_de_equipo', ''),
                    }
                elif key == 'placa':
                    answers[self.ActivoFijo.f['numero_de_serie_chasis']] = value
                elif key == 'imagen_del_equipo':
                    answers[self.f['imagen_del_equipo']] = value
                elif key == 'fecha_instalacion':
                    answers[self.f['fecha_de_instalacion']] = value
                elif key == 'tecnico_asignado':
                    answers[self.Employee.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {
                        self.Employee.f['worker_name']: value
                    }
                elif key == 'estatus':
                    answers[self.ActivoFijo.f['estatus']] = value
                elif key == 'estado':
                    answers[self.ActivoFijo.f['estado']] = value
                else:
                    pass
            metadata.update({'answers':answers})
            request = self.lkf_api.post_forms_answers(metadata)
            status_code = request.get('status_code')

            if status_code == 201:
                response = self.create_response("success", 200, "Activo Fijo agregado con exito")
                return response
            else:
                data = [request.get('json')]
                response = self.create_response("error", status_code, "Activo Fijos tuvo un error al agregar el registro", data)
                return response

        except Exception as e:
            response = self.create_response("error", 500, f"Error al agregar un registro: {e}")
            return response
        
    def update_activo_fijo_in_orden_instalacion(self, answers, data):
        nombre_equipo = data.get('nombre_equipo')
        nick_eco = data.get('nick_eco')
        nuevo_nombre_equipo = nombre_equipo + '-' + nick_eco

        answers[self.ActivoFijo.ACTIVOS_FIJOS_CAT_OBJ_ID] = {
            self.ActivoFijo.f['marca']: data.get('marca'),
            self.ActivoFijo.f['modelo']: data.get('modelo'),
            self.ActivoFijo.f['nombre_equipo']: nuevo_nombre_equipo,
            self.ActivoFijo.f['categoria_marca']: data.get('tipo_de_equipo'),
            self.f['imagen_del_equipo']: data.get('imagen_del_equipo')
        }

        return answers

if __name__ == "__main__":
    mantenimiento_obj = Mantenimiento(settings, sys_argv=sys.argv)
    mantenimiento_obj.console_run()

    data = mantenimiento_obj.format_data_to_create_activo_fijo(mantenimiento_obj.answers)
    response = mantenimiento_obj.create_activo_fijo(data=data)
    replace_answers = mantenimiento_obj.answers
    if response.get('status') == 'success':
        replace_answers = mantenimiento_obj.update_activo_fijo_in_orden_instalacion(mantenimiento_obj.answers, data)

    print(response)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': replace_answers
    }))