# coding: utf-8
import dis
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def get_mongo_string_list(func):
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
            data = self.format_cr(self.cr.aggregate(query))
            format_data = []
            if data:
                for item in data:
                    format_data.append(item.get("value"))
                format_data = list(set(format_data))
            return format_data
        return wrapper

    def get_mongo_distinct_list(func):
        def wrapper(self, *args, **kwargs):
            config = func(self, *args, **kwargs)
            field_name = config.get('field')
            form_id = config.get('form_id')
            match_query_custom = config.get('query', {})
            if not field_name:
                return []
            match_query = {"deleted_at": {"$exists": False}}
            if form_id:
                match_query["form_id"] = form_id
            match_query.update(match_query_custom)
            data = self.cr.distinct(field_name, match_query)
            return [str(item) for item in data if item is not None]
        return wrapper

    # ── Helpers ──────────────────────────────────────────────────────────────

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

    @get_mongo_string_list
    def get_areas(self):
        return {
            "form_id": self.AREAS_DE_LAS_UBICACIONES,
            "project": {
                "value": f"$answers.{self.mf['nombre_area']}"
            }
        }

    @get_mongo_distinct_list
    def get_in_and_out_status(self):
        return {
            "form_id": self.BITACORA_ACCESOS,
            "field": f"answers.{self.mf['tipo_registro']}"
        }

    @get_mongo_distinct_list

    def get_incidencias_estatus(self):
        return {
            "form_id": self.BITACORA_INCIDENCIAS,
            "field": f"answers.{self.incidence_fields['estatus']}"
        }

    @get_mongo_distinct_list
    def get_incidencias_tipo(self):
        return {
            "form_id": self.BITACORA_INCIDENCIAS,
            "field": f"answers.{self.incidence_fields['incidencia']}"
        }

    @get_mongo_distinct_list
    def get_fallas_estatus(self):
        return {
            "form_id": self.BITACORA_FALLAS,
            "field": f"answers.{self.mf['estatus_falla']}"
        }

    @get_mongo_distinct_list
    def get_fallas_tipo(self):
        return {
            "form_id": self.BITACORA_FALLAS,
            "field": f"answers.{self.mf['tipo_falla']}"
        }


    def get_pases_status(self):
        return {
            "form_id": self.PASE_ENTRADA,
            "field": f"answers.{self.pase_entrada_fields['status_pase']}"
        }

    def get_filters_in_and_out(self):
        profiles  = self.get_profiles()
        estatus   = self.get_in_and_out_status()
        employees = self.get_employees_names()
        return [
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

    def get_filters_pases(self):
        profiles = self.get_profiles()
        estatus = self.get_pases_status()
        employees = self.get_employees_names()
        filters = [
            {
                "defaultDisplayOpen": True,
                "key": "status",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize().replace("_", " "), "value": i} for i in estatus if i]
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
        
    def get_filters_recorridos(self):
        asignado_a = self.get_employees_names()
        areas      = self.get_areas()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_rondin",
                "label": "Estatus",
                "type": "single",
                "options": [
                    {"label": "Corriendo", "value": "corriendo"},
                    {"label": "Pausado",   "value": "pausado"},
                    {"label": "Cancelado", "value": "cancelado"},
                    {"label": "Cerrado",   "value": "cerrado"},
                ]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_rondin",
                "label": "Tipo",
                "type": "single",
                "options": [
                    {"label": "QR",  "value": "qr"},
                    {"label": "NFC", "value": "nfc"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "recurrencia",
                "label": "Recurrencia",
                "type": "single",
                "options": [
                    {"label": "Minuto",           "value": "Minuto"},
                    {"label": "Hora",             "value": "Hora"},
                    {"label": "Día de la semana", "value": "Dia de la Semana"},
                    {"label": "Día del mes",      "value": "Dia del Mes"},
                    {"label": "Mes",              "value": "Mes"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "asignado_a",
                "label": "Asignado a",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in asignado_a]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_rondines(self):
        asignado_a = self.get_employees_names()
        areas      = self.get_areas()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_rondin",
                "label": "Estatus",
                "type": "single",
                "options": [
                    {"label": "Programado", "value": "Programado"},
                    {"label": "Realizado",  "value": "Realizado"},
                    {"label": "En Proceso", "value": "En Proceso"},
                    {"label": "Cancelado",  "value": "Cancelado"},
                    {"label": "Cerrado",    "value": "Cerrado"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "incidencias",
                "label": "Tiene incidencias",
                "type": "single",
                "options": [
                    {"label": "Si", "value": "Si"},
                    {"label": "No", "value": "No"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "asignado_a",
                "label": "Asignado a",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in asignado_a]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_check_areas(self):
        areas = self.get_areas()
        return [
            {
                "defaultDisplayOpen": False,
                "key": "incidencias",
                "label": "Tiene incidencias",
                "type": "single",
                "options": [
                    {"label": "Si", "value": "Si"},
                    {"label": "No", "value": "No"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_incidencias(self):
        estatuses = self.get_incidencias_estatus()
        tipos     = self.get_incidencias_tipo()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_incidencia",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_incidencia",
                "label": "Tipo",
                "type": "multiple",
                "options": [{"label": i, "value": i} for i in tipos]
            },
        ]

    def get_filters_fallas(self):
        estatuses = self.get_fallas_estatus()
        tipos     = self.get_fallas_tipo()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_falla",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_falla",
                "label": "Tipo",
                "type": "multiple",
                "options": [{"label": i, "value": i} for i in tipos]
            },
        ]


if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    data = script_obj.data.get('data', {})
    option = data.get("option", '')

    dispatcher = {
        "recorridos":  lambda: script_obj.get_filters_recorridos(),
        "rondines":    lambda: script_obj.get_filters_rondines(),
        "check_areas": lambda: script_obj.get_filters_check_areas(),
        "incidencias": lambda: script_obj.get_filters_incidencias(),
        "fallas":      lambda: script_obj.get_filters_fallas(),
        "in_and_out": lambda: script_obj.get_filters_in_and_out(),
        "rondines":   lambda: script_obj.get_filters_rondines(),
        "pases":      lambda: script_obj.get_filters_pases(),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})