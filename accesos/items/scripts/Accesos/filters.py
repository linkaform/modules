# coding: utf-8
import dis
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def get_mongo_string_list(func):
        """
        Decorador que utiliza aggregate para obtener una lista de strings únicos de un campo específico tomando en cuenta un filtro personalizado.
        """
        def wrapper(self, *args, **kwargs):
            config = func(self, *args, **kwargs)
            
            match_query_custom = config.get('query', {})
            form_id = config.get('form_id')
            project_fields = config.get('project', {})

            match_query = {"deleted_at": {"$exists": False}}
            if form_id:
                match_query["form_id"] = form_id
            match_query.update(match_query_custom)
            
            query = [{"$match": match_query}, {"$project": {"_id": 0, **project_fields}}]
            print('query', query)
            data = self.format_cr(self.cr.aggregate(query))
            format_data = []
            if data:
                for item in data:
                    format_data.append(item.get("value"))
                format_data = list(set(format_data))
            return format_data
        return wrapper

    def get_mongo_distinct_list(func):
        """
        Decorador que utiliza distinct para obtener una lista de valores únicos de un campo específico tomando en cuenta un filtro personalizado.
        """
        def wrapper(self, *args, **kwargs):
            config = func(self, *args, **kwargs)
            
            field_name = config.get('field')
            form_id = config.get('form_id')
            match_query_custom = config.get('query', {})

            if not field_name:
                return []

            # Construir filtro base
            match_query = {"deleted_at": {"$exists": False}}
            if form_id:
                match_query["form_id"] = form_id
            match_query.update(match_query_custom)

            # Ejecutar distinct directamente en la colección
            print(f'Ejecutando distinct en campo: {field_name} con filtro: {match_query}')
            data = self.cr.distinct(field_name, match_query)
            
            # Asegurar que todos son strings y remover Nones si existen
            return [str(item) for item in data if item is not None]
        return wrapper

    @get_mongo_string_list
    def get_profiles(self):
        return {
            "form_id": self.CONF_PERFILES,
            "query": {
                f"answers.{self.PERFILES_OBJ_ID}.{self.mf['walkin']}": "Si"
            },
            "project": {
                "value": f"$answers.{self.PERFILES_OBJ_ID}.{self.mf['nombre_perfil']}" 
            }
        }
    
    @get_mongo_string_list
    def get_employees_names(self):
        return {
            "form_id": self.EMPLEADOS,
            "project": {
                "value": f"$answers.{self.mf['nombre_empleado']}" 
            }
        }

    @get_mongo_distinct_list
    def get_in_and_out_status(self):
        return {
            "form_id": self.BITACORA_ACCESOS,
            "field": f"answers.{self.mf['tipo_registro']}"
        }

    def get_filters_in_and_out(self):
        """
        Obtiene los filtros para la Bitacora de Entradas y Salidas
        """
        profiles = self.get_profiles()
        estatus = self.get_in_and_out_status()
        employees = self.get_employees_names()
        filters = [
            {
                "defaultDisplayOpen": True,
                "key": "status",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatus]
            },
            {
                "defaultDisplayOpen": False,
                "key": "perfil_visita",
                "label": "Perfil",
                "type": "multiple",
                "options": [{"label": i, "value": i} for i in profiles]
            },
            {
                "defaultDisplayOpen": False,
                "key": "visita_a",
                "label": "Visita a",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in employees]
            }
        ]
        return filters

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data',{})
    option = data.get("option",'')

    dispatcher = {
        "in_and_out": lambda: script_obj.get_filters_in_and_out()
    }

    action = dispatcher.get(option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data":response})

