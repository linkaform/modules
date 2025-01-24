# coding: utf-8
#####
# Script para grupo repetitivo a catalogos
# Forma: Configuracion Areas y Empleados
# Catalogos: Configuracion Areas y Empleados & Configuracion Areas y Empleados Apoyo
#####
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def in_catalog(self, data):
        # print(simplejson.dumps(data, indent=4))
        nombre_completo = data.get(self.EMPLOYEE_OBJ_ID, {}).get(self.mf['nombre_empleado'])
        grupo_repetitivo = data.get(self.mf['areas_grupo'], [])
        
        if not nombre_completo:
            print("El nombre completo no se encontró en los datos.")
            return
        
        selector = {}
        
        selector.update({f"answers.{self.mf['nombre_empleado']}": nombre_completo})

        if not selector:
            selector = {"_id": {"$gt": None}}

        fields = ["_id", f"answers.{self.mf['nombre_empleado']}", f"answers.{self.mf['ubicacion']}", f"answers.{self.mf['nombre_area']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 100
        }

        try:
            row_catalog = self.lkf_api.search_catalog(self.CONF_AREA_EMPLEADOS_CAT_ID, mango_query)
            # print(f"Resultado de la búsqueda: {row_catalog}")
            if row_catalog:
                existing_areas = set()
                existing_locations = set()
                
                for item_catalog in row_catalog:
                    area = item_catalog.get(self.mf['nombre_area'])
                    location = item_catalog.get(self.mf['ubicacion'])

                    if area: 
                        existing_areas.add(area)
                    if location:
                        existing_locations.add(location)

                for item in grupo_repetitivo:
                    area_grupo = item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['nombre_area'])
                    location_grupo = item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['ubicacion'])
                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if area_grupo in existing_areas:
                        item.get(self.mf['catalogo_ubicaciones']).update({'existente': True})
                    else:
                        item.get(self.mf['catalogo_ubicaciones']).update({'existente': False})

                # print(grupo_repetitivo)
                self.format_group_to_catalog(data, grupo_repetitivo)
            else:
                # print(grupo_repetitivo)
                self.format_group_to_catalog(data, grupo_repetitivo)
        except Exception as e:
            print(f"Error al realizar la búsqueda: {e}")

    def format_group_to_catalog(self, data, grupo_repetitivo):
        print('/////////////////entra en format_group_to_catalog')

        answer = {}
        catalogo_metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.CONF_AREA_EMPLEADOS_CAT_ID)
        nombre_completo = data.get(self.EMPLOYEE_OBJ_ID, {}).get(self.mf['nombre_empleado'])
        # grupo_repetitivo = data.get(self.mf['areas_grupo'], [])
        usuario_data = data.get(self.EMPLOYEE_OBJ_ID, {})

        for item in usuario_data:
            answer.update({item: usuario_data[item]})
        
        answer[self.mf['nombre_empleado']] = nombre_completo
        answer.pop(self.employee_fields['estatus_disponibilidad'])

        for item in grupo_repetitivo:
            if not item.get(self.mf['catalogo_ubicaciones'], {}).get('existente'):
                answer.update(
                    {
                        self.mf['ubicacion']: item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['ubicacion']),
                        self.mf['nombre_area']: item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['nombre_area'])
                    }
                )
                catalogo_metadata.update({'answers': answer})
                # print(simplejson.dumps(catalogo_metadata, indent=4))
                self.lkf_api.post_catalog_answers(catalogo_metadata, jwt_settings_key='APIKEY_JWT_KEY')
                # print('Respuesta de la creación del registro en el catálogo:', res)
            else:
                print("Ya se encuentran registradas esas areas y ubicaciones")

    def in_catalog_apoyo(self, data):
        # print(simplejson.dumps(data, indent=4))
        nombre_completo = data.get(self.EMPLOYEE_OBJ_ID, {}).get(self.mf['nombre_empleado'])
        grupo_repetitivo = data.get(self.mf['areas_grupo'], [])
        
        if not nombre_completo:
            print("El nombre completo no se encontró en los datos.")
            return
        
        selector = {}
        
        selector.update({f"answers.{self.mf['nombre_guardia_apoyo']}": nombre_completo})

        if not selector:
            selector = {"_id": {"$gt": None}}

        fields = ["_id", f"answers.{self.mf['nombre_guardia_apoyo']}", f"answers.{self.mf['ubicacion']}", f"answers.{self.mf['nombre_area_salida']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 100
        }

        try:
            row_catalog = self.lkf_api.search_catalog(self.CONF_AREA_EMPLEADOS_AP_CAT_ID, mango_query)
            # print(f"Resultado de la búsqueda: {row_catalog}")
            if row_catalog:
                existing_areas = set()
                existing_locations = set()
                
                for item_catalog in row_catalog:
                    area = item_catalog.get(self.mf['nombre_area_salida'])
                    location = item_catalog.get(self.mf['ubicacion'])

                    if area: 
                        existing_areas.add(area)
                    if location:
                        existing_locations.add(location)

                for item in grupo_repetitivo:
                    area_grupo = item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['nombre_area'])
                    location_grupo = item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['ubicacion'])
                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if area_grupo in existing_areas:
                        item.get(self.mf['catalogo_ubicaciones']).update({'existente': True})
                    else:
                        item.get(self.mf['catalogo_ubicaciones']).update({'existente': False})

                # print(grupo_repetitivo)
                self.format_group_to_catalog_apoyo(data, grupo_repetitivo)
            else:
                # print(grupo_repetitivo)
                self.format_group_to_catalog_apoyo(data, grupo_repetitivo)
        except Exception as e:
            print(f"Error al realizar la búsqueda: {e}")
    
    def format_group_to_catalog_apoyo(self, data, grupo_repetitivo):
        print('////////////////entra en format_group_to_catalog_apoyo')

        answer = {}
        catalogo_metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.CONF_AREA_EMPLEADOS_AP_CAT_ID)
        nombre_completo = data.get(self.EMPLOYEE_OBJ_ID, {}).get(self.mf['nombre_empleado'])
        # grupo_repetitivo = data.get(self.mf['areas_grupo'], [])
        usuario_data = data.get(self.EMPLOYEE_OBJ_ID, {})

        for item in usuario_data:
            answer.update({item: usuario_data[item]})
        
        answer[self.mf['nombre_guardia_apoyo']] = nombre_completo
        answer.pop(self.employee_fields['estatus_disponibilidad'])

        for item in grupo_repetitivo:
            if not item.get(self.mf['catalogo_ubicaciones'], {}).get('existente'):
                answer.update(
                    {
                        self.mf['ubicacion']: item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['ubicacion']),
                        self.mf['nombre_area_salida']: item.get(self.mf['catalogo_ubicaciones'], {}).get(self.mf['nombre_area'])
                    }
                )
                catalogo_metadata.update({'answers': answer})
                # print(simplejson.dumps(catalogo_metadata, indent=4))
                self.lkf_api.post_catalog_answers(catalogo_metadata, jwt_settings_key='APIKEY_JWT_KEY')
                # print('Respuesta de la creación del registro en el catálogo:', res)
            else:
                print("Ya se encuentran registradas esas areas y ubicaciones")

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    acceso_obj.in_catalog(acceso_obj.answers)
    acceso_obj.in_catalog_apoyo(acceso_obj.answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
    }))