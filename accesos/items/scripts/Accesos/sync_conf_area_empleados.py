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
                print('=====Ya hay algunos registros en el catalogo Conf Areas y Empleados======')
                existing_pairs = set()

                for item_catalog in row_catalog:
                    area = item_catalog.get(self.mf['nombre_area'], '').strip()
                    location = item_catalog.get(self.mf['ubicacion'], '').strip()

                    if area and location:
                        existing_pairs.add((area, location))
                    elif not area and location:
                        existing_pairs.add(('', location))

                # print('existing_pairssssssss', existing_pairs)
                for item in grupo_repetitivo:
                    area_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'], '').strip()
                    location_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion'], '').strip()

                    item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID][self.mf['nombre_area']] = area_grupo

                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if (area_grupo, location_grupo) in existing_pairs:
                        item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({'existente': True})
                    else:
                        item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({'existente': False})

                # print(grupo_repetitivo)
                self.format_group_to_catalog(data, grupo_repetitivo)
            else:
                print('=====No hay ninguno de estos registros en el catalogo Conf Areas y Empleados======')
                for item in grupo_repetitivo:
                    area_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'], '').strip()
                    location_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion']).strip()
                    
                    item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID][self.mf['nombre_area']] = area_grupo

                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')
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
        for item, value in usuario_data.items():
            if isinstance(value, list) and value:
                if isinstance(value[0], list):
                    value = value[0]
            answer[item] = value
        
        answer[self.mf['nombre_empleado']] = nombre_completo
        # answer.pop(self.employee_fields['estatus_disponibilidad'])
        for item in grupo_repetitivo:
            if not item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get('existente'):
                answer.update(
                    {
                        self.mf['ubicacion']: item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion']),
                        self.mf['nombre_area']: item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'])
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
                print('=====Ya hay algunos registros en el catalogo Conf Areas y Empleados Apoyo======')
                existing_pairs = set()

                for item_catalog in row_catalog:
                    area = item_catalog.get(self.mf['nombre_area_salida'], '').strip()
                    location = item_catalog.get(self.mf['ubicacion'], '').strip()

                    if area and location:
                        existing_pairs.add((area, location))
                    elif not area and location:
                        existing_pairs.add(('', location))

                # print('existing_pairssssssss', existing_pairs)

                for item in grupo_repetitivo:
                    area_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area']).strip()
                    location_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion']).strip()

                    item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID][self.mf['nombre_area']] = area_grupo

                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if (area_grupo, location_grupo) in existing_pairs:
                        item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({'existente': True})
                    else:
                        item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID].update({'existente': False})

                # print(grupo_repetitivo)
                self.format_group_to_catalog_apoyo(data, grupo_repetitivo)
            else:
                print('=====No hay ninguno de estos registros en el catalogo Conf Areas y Empleados Apoyo======')
                for item in grupo_repetitivo:
                    area_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'], '').strip()
                    location_grupo = item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion']).strip()
                    
                    item[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID][self.mf['nombre_area']] = area_grupo

                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')
                    item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID).pop('existente', None)
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

        for item, value in usuario_data.items():
            if isinstance(value, list) and value:
                if isinstance(value[0], list):
                    value = value[0]
            answer[item] = value
        
        answer[self.mf['nombre_guardia_apoyo']] = nombre_completo
        # answer.pop(self.employee_fields['estatus_disponibilidad'])

        for item in grupo_repetitivo:
            if not item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get('existente'):
                answer.update(
                    {
                        self.mf['ubicacion']: item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['ubicacion']),
                        self.mf['nombre_area_salida']: item.get(self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID, {}).get(self.mf['nombre_area'])
                    }
                )
                catalogo_metadata.update({'answers': answer})
                # print(simplejson.dumps(catalogo_metadata, indent=4))
                self.lkf_api.post_catalog_answers(catalogo_metadata, jwt_settings_key='APIKEY_JWT_KEY')
                # print('Respuesta de la creación del registro en el catálogo:', res)
            else:
                print("Ya se encuentran registradas esas areas y ubicaciones en el catálogo de apoyo")

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    acceso_obj.in_catalog(acceso_obj.answers)
    acceso_obj.in_catalog_apoyo(acceso_obj.answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
    }))