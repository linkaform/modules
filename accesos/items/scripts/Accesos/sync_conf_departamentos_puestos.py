# coding: utf-8
#####
# Script para grupo repetitivo a catalogos
# Forma: Configuracion de Departamentos y Puestos
# Catalogo: Configuracion de Departamentos y Puestos
#####
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def search_in_catalog(self, data):
        # print('dataaaaaaaaaaaaaaaaaaaaaaaaa')
        # print(simplejson.dumps(data, indent=4))
        departamento = data.get(self.mf['catalogo_departamentos'], {}).get(self.mf['departamento_empleado'])
        grupo_repetitivo = data.get(self.mf['grupo_puestos'], [])
        
        if not departamento:
            print("El departamento no se encontró en los datos.")
            return
        
        selector = {}
        
        selector.update({f"answers.{self.mf['departamento_empleado']}": departamento})

        if not selector:
            selector = {"_id": {"$gt": None}}

        fields = ["_id", f"answers.{self.mf['departamento_empleado']}", f"answers.{self.mf['puesto_empleado']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 100
        }

        try:
            row_catalog = self.lkf_api.search_catalog(self.CONF_DEPARTAMENTOS_PUESTOS_CAT_ID, mango_query)
            # print(f"Resultado de la búsqueda: {row_catalog}")
            # print(f"Resultado del grupo repetitivo: {grupo_repetitivo}")
            if row_catalog:
                existing_puesto = set()
                
                for item_catalog in row_catalog:
                    puesto = item_catalog.get(self.mf['puesto_empleado'])
                    # print(f'Puesto row_catalog: {puesto}')

                    if puesto:
                        existing_puesto.add(puesto)

                for item in grupo_repetitivo:
                    puesto_grupo = item.get(self.mf['catalogo_puestos'], {}).get(self.mf['puesto_empleado'])
                    # print(f'Puesto: {puesto_grupo}')

                    if puesto_grupo in existing_puesto:
                        item.get(self.mf['catalogo_puestos']).update({'existente': True})
                    else:
                        item.get(self.mf['catalogo_puestos']).update({'existente': False})

                # print('grupo_repetitivo/////////////', grupo_repetitivo)
                self.insert_group_to_catalog(data, grupo_repetitivo)
            else:
                # print('grupo_repetitivo/////////////', grupo_repetitivo)
                self.insert_group_to_catalog(data, grupo_repetitivo)
        except Exception as e:
            print(f"Error al realizar la búsqueda: {e}")

    def insert_group_to_catalog(self, data, grupo_repetitivo):
        # print('/////////////////entra en insert_group_to_catalog')
        # print('DATAAAAAAAAAAAAAAAAAAAAAAAA')
        # print(simplejson.dumps(data, indent=4))
        # print('GRUPO_REPETITIVOOOOOOOOOOOO')
        # print(simplejson.dumps(grupo_repetitivo, indent=4))

        answer = {}
        departamento = data.get(self.mf['catalogo_departamentos'], {}).get(self.mf['departamento_empleado'])
        grupo_repetitivo = data.get(self.mf['grupo_puestos'], [])
        catalogo_metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.CONF_DEPARTAMENTOS_PUESTOS_CAT_ID)
        
        answer[self.mf['departamento_empleado']] = departamento

        for item in grupo_repetitivo:
            if not item.get(self.mf['catalogo_puestos'], {}).get('existente'):
                answer.update(
                    {
                        self.mf['puesto_empleado']: item.get(self.mf['catalogo_puestos'], {}).get(self.mf['puesto_empleado']),
                    }
                )
                catalogo_metadata.update({'answers': answer})
                # print('catalogo_metadata/////////////////////')
                print(f"Nuevo registro: {item.get(self.mf['catalogo_puestos'], {}).get(self.mf['puesto_empleado'])}")
                self.lkf_api.post_catalog_answers(catalogo_metadata, jwt_settings_key='APIKEY_JWT_KEY')
                # print('Respuesta de la creación del registro en el catálogo:', res)
            else:
                print(f"{item.get(self.mf['catalogo_puestos'], {}).get(self.mf['puesto_empleado'])} ya se encuentra registrado")

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    acceso_obj.search_in_catalog(acceso_obj.answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
    }))