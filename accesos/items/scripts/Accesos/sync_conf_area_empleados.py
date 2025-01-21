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
        nombre_completo = data.get('66a83aaa40fcd6362499844c', {}).get('62c5ff407febce07043024dd', '')
        grupo_repetitivo = data.get('663cf9d77500019d1359eb9f', [])
        
        if not nombre_completo:
            print("El nombre completo no se encontró en los datos.")
            return
        
        selector = {}
        
        selector.update({"answers.62c5ff407febce07043024dd": nombre_completo})

        if not selector:
            selector = {"_id": {"$gt": None}}

        fields = ["_id", "answers.62c5ff407febce07043024dd", "answers.663e5c57f5b8a7ce8211ed0b", "answers.663e5d44f5b8a7ce8211ed0f"]

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
                    area = item_catalog.get('663e5d44f5b8a7ce8211ed0f', '')
                    location = item_catalog.get('663e5c57f5b8a7ce8211ed0b', '')

                    if area: 
                        existing_areas.add(area)
                    if location:
                        existing_locations.add(location)

                for item in grupo_repetitivo:
                    area_grupo = item.get('66a83a77cfed7f342775c161', {}).get('663e5d44f5b8a7ce8211ed0f', '')
                    location_grupo = item.get('66a83a77cfed7f342775c161', {}).get('663e5c57f5b8a7ce8211ed0b', '')
                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if area_grupo in existing_areas:
                        item.get('66a83a77cfed7f342775c161').update({'existente': True})
                    else:
                        item.get('66a83a77cfed7f342775c161').update({'existente': False})

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
        nombre_completo = data.get('66a83aaa40fcd6362499844c', '').get('62c5ff407febce07043024dd', '')
        # grupo_repetitivo = data.get('663cf9d77500019d1359eb9f', [])
        usuario_data = data.get('66a83aaa40fcd6362499844c', '')

        for item in usuario_data:
            answer.update({item: usuario_data[item]})
        
        answer['62c5ff407febce07043024dd'] = nombre_completo
        answer.pop('663bcbe2274189281359eb78')

        for item in grupo_repetitivo:
            if not item.get('66a83a77cfed7f342775c161', {}).get('existente'):
                answer.update(
                    {
                        '663e5c57f5b8a7ce8211ed0b': item.get('66a83a77cfed7f342775c161', {}).get('663e5c57f5b8a7ce8211ed0b', ''),
                        '663e5d44f5b8a7ce8211ed0f': item.get('66a83a77cfed7f342775c161', {}).get('663e5d44f5b8a7ce8211ed0f', '')
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
        nombre_completo = data.get('66a83aaa40fcd6362499844c', {}).get('62c5ff407febce07043024dd', '')
        grupo_repetitivo = data.get('663cf9d77500019d1359eb9f', [])
        
        if not nombre_completo:
            print("El nombre completo no se encontró en los datos.")
            return
        
        selector = {}
        
        selector.update({"answers.663bd36eb19b7fb7d9e97ccb": nombre_completo})

        if not selector:
            selector = {"_id": {"$gt": None}}

        fields = ["_id", "answers.663bd36eb19b7fb7d9e97ccb", "answers.663e5c57f5b8a7ce8211ed0b", "answers.663fb45992f2c5afcfe97ca8"]

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
                    area = item_catalog.get('663fb45992f2c5afcfe97ca8', '')
                    location = item_catalog.get('663e5c57f5b8a7ce8211ed0b', '')

                    if area: 
                        existing_areas.add(area)
                    if location:
                        existing_locations.add(location)

                for item in grupo_repetitivo:
                    area_grupo = item.get('66a83a77cfed7f342775c161', {}).get('663e5d44f5b8a7ce8211ed0f', '')
                    location_grupo = item.get('66a83a77cfed7f342775c161', {}).get('663e5c57f5b8a7ce8211ed0b', '')
                    print(f'Area: {area_grupo}, Ubicación: {location_grupo}')

                    if area_grupo in existing_areas:
                        item.get('66a83a77cfed7f342775c161').update({'existente': True})
                    else:
                        item.get('66a83a77cfed7f342775c161').update({'existente': False})

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
        nombre_completo = data.get('66a83aaa40fcd6362499844c', '').get('62c5ff407febce07043024dd', '')
        # grupo_repetitivo = data.get('663cf9d77500019d1359eb9f', [])
        usuario_data = data.get('66a83aaa40fcd6362499844c', '')

        for item in usuario_data:
            answer.update({item: usuario_data[item]})
        
        answer['663bd36eb19b7fb7d9e97ccb'] = nombre_completo
        answer.pop('663bcbe2274189281359eb78')

        for item in grupo_repetitivo:
            if not item.get('66a83a77cfed7f342775c161', {}).get('existente'):
                answer.update(
                    {
                        '663e5c57f5b8a7ce8211ed0b': item.get('66a83a77cfed7f342775c161', {}).get('663e5c57f5b8a7ce8211ed0b', ''),
                        '663fb45992f2c5afcfe97ca8': item.get('66a83a77cfed7f342775c161', {}).get('663e5d44f5b8a7ce8211ed0f', '')
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