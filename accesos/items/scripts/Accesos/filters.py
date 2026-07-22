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
            "field": f"answers.{self.fallas_fields['falla_estatus']}"
        }

    @get_mongo_distinct_list
    def get_paqueteria_estatus(self):
        return {
            "form_id": self.PAQUETERIA,
            "field": f"answers.{self.paquetes_fields['estatus_paqueteria']}"
        }

    @get_mongo_distinct_list
    def get_lockers(self):
        return {
            "form_id": self.PAQUETERIA,
            "field": f"answers.{self.paquetes_fields['guardado_en_paqueteria']}"
        }

    @get_mongo_distinct_list
    def get_cons_estatus(self):
        return {
            "form_id": self.CONCESSIONED_ARTICULOS,
            "field": f"answers.{self.cons_f['status_concesion']}"
        }
    @get_mongo_distinct_list
    def get_cons_categoria(self):
        return {
            "form_id": self.CONCESSIONED_ARTICULOS,
            "field": f"answers.{self.cons_f['grupo_equipos']}.{self.cons_f['categoria_equipo_concesion']}"
        }
    @get_mongo_distinct_list
    def get_cons_equipos(self):
        return {
            "form_id": self.CONCESSIONED_ARTICULOS,
            "field": f"answers.{self.cons_f['grupo_equipos']}.{self.cons_f['nombre_equipo']}"
        }

    @get_mongo_distinct_list
    def get_perdidos_estatus(self):
        return {
            "form_id": self.BITACORA_OBJETOS_PERDIDOS,
            "field": f"answers.{self.perdidos_fields['estatus_perdido']}"
        }
    @get_mongo_distinct_list
    def get_perdidos_cat(self):
        return {
            "form_id": self.BITACORA_OBJETOS_PERDIDOS,
            "field": f"answers.{self.perdidos_fields['articulo_seleccion_catalog']}.{self.cons_f['_categoria_equipo_concesion']}"
        }
    @get_mongo_distinct_list
    def get_perdidos_art(self):
        return {
            "form_id": self.BITACORA_OBJETOS_PERDIDOS,
            "field": f"answers.{self.perdidos_fields['articulo_seleccion_catalog']}.{self.fallas_fields['falla_objeto_afectado']}"
        }
    @get_mongo_distinct_list
    def get_perdidos_color(self):
        return {
            "form_id": self.BITACORA_OBJETOS_PERDIDOS,
            "field": f"answers.{self.perdidos_fields['color_perdido']}"
        }
    @get_mongo_distinct_list
    def get_fallas_tipo(self):
        return {
            "form_id": self.BITACORA_FALLAS,
            "field": f"answers.{self.LISTA_FALLAS_CAT_OBJ_ID}.{self.fallas_fields['falla']}"
        }
    @get_mongo_distinct_list
    def get_notas_estatus(self):
        return {
            "form_id": self.ACCESOS_NOTAS,
            "field": f"answers.{self.notes_fields['note_status']}"
        }
    @get_mongo_distinct_list
    def get_transportistas_estatus(self):
        return {
            "form_id": self.BITACORA_TRANSPORTISTAS,
            "field": f"answers.{self.bitacora_transportista_fields['estatus']}"
        }
    @get_mongo_distinct_list
    def get_transportistas_tipo_vehiculo(self):
        return {
            "form_id": self.BITACORA_TRANSPORTISTAS,
            "field": f"answers.{self.bitacora_transportista_fields['tipo_de_vehiculo']}"
        }
    @get_mongo_distinct_list
    def get_transportistas_empresa(self):
        return {
            "form_id": self.BITACORA_TRANSPORTISTAS,
            "field": f"answers.{self.bitacora_transportista_fields['proveedor_cliente']}"
        }
    @get_mongo_distinct_list
    def get_transportistas_anden(self):
        return {
            "form_id": self.BITACORA_TRANSPORTISTAS,
            "field": f"answers.{self.bitacora_transportista_fields['anden_asignado']}"
        }
    @get_mongo_distinct_list
    def get_proveedores(self):
        return {
            "form_id": self.PAQUETERIA,
            "field": f"answers.{self.paquetes_fields['proveedor_cat']}.{self.paquetes_fields['proveedor']}"
        }
    @get_mongo_distinct_list
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
                "key": "estatus_recorrido",
                "label": "Estatus",
                "type": "multiple",
                "options": [
                    {"label": "Corriendo", "value": "Corriendo"},
                    {"label": "Pausado",   "value": "Pausado"},
                    {"label": "Eliminado", "value": "Eliminado"},
                    {"label": "Sin Programar",   "value": "Sin Programar"},
                ]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_rondin",
                "label": "Tipo",
                "type": "multiple",
                "options": [
                    {"label": "QR",  "value": "qr"},
                    {"label": "NFC", "value": "nfc"},
                ]
            },
            {
                "defaultDisplayOpen": False,
                "key": "recurrencia",
                "label": "Recurrencia",
                "type": "multiple",
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
                "type": "multiple",
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
        asignado_a = self.get_employees_names()
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

    def get_filters_incidencias(self):
        estatuses = self.get_incidencias_estatus()
        tipos     = self.get_incidencias_tipo()
        reportado_por = self.get_employees_names()
        areas = self.get_areas()
        
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_incidencia",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": False,
                "key": "reportado_por",
                "label": "Reportado por",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_incidencia",
                "label": "Incidente",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in tipos]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Lugar del incidente",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_incidencias_rondines(self):
        tipos     = self.get_incidencias_tipo()
        areas = self.get_areas()
        
        return [
            {
                "defaultDisplayOpen": True,
                "key": "tipo_incidencia",
                "label": "Incidente",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in tipos]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Lugar del incidente",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_fallas(self):
        estatuses = self.get_fallas_estatus()
        tipos     = self.get_fallas_tipo()
        reportado_por = self.get_employees_names()
        areas = self.get_areas()
        
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_falla",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": False,
                "key": "reportado_por",
                "label": "Reportado por",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
            {
                "defaultDisplayOpen": True,
                "key": "tipo_falla",
                "label": "Falla",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in tipos]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area",
                "label": "Lugar de la falla",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_paqueteria(self):
        estatuses = self.get_paqueteria_estatus()
        reportado_por = self.get_employees_names()
        areas = self.get_areas()
        lockers = self.get_lockers()
        proveedor = self.get_proveedores()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_paqueteria",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": False,
                "key": "quien_recibe_paqueteria",
                "label": "Quien recibe ",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
              {
                "defaultDisplayOpen": False,
                "key": "locker",
                "label": "Locker ",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in lockers]
            },
            {
                "defaultDisplayOpen": False,
                "key": "proveedor",
                "label": "Proveedor",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in proveedor]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area_paqueteria",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_concesionados(self):
        estatuses = self.get_cons_estatus()
        equipos = self.get_cons_equipos()
        reportado_por = self.get_employees_names()
        areas = self.get_areas()
        categoria = self.get_cons_categoria()

        return [
            {
                "defaultDisplayOpen": True,
                "key": "status_concesion",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatuses]
            },
            {
                "defaultDisplayOpen": False,
                "key": "persona_nombre_concesion",
                "label": "Solicitante ",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
            {
                "defaultDisplayOpen": False,
                "key": "created_by",
                "label": "Creado por ",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
            {
                "defaultDisplayOpen": False,
                "key": "categoria_equipo_concesion",
                "label": "Categoría",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in categoria]
            },
             {
                "defaultDisplayOpen": False,
                "key": "nombre_equipo",
                "label": "Equipo ",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in equipos]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area_paqueteria",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]

    def get_filters_perdidos(self):
        estatus = self.get_perdidos_estatus()
        cat = self.get_perdidos_cat()
        art = self.get_perdidos_art()
        color = self.get_perdidos_color()
        reportado_por = self.get_employees_names()
        areas = self.get_areas()
        
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus_p",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatus]
            },
            {
                "defaultDisplayOpen": False,
                "key": "categoria",
                "label": "Categoría",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in cat]
            },
            {
                "defaultDisplayOpen": False,
                "key": "articulo",
                "label": "Artículo",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in art]
            },
            {
                "defaultDisplayOpen": False,
                "key": "color",
                "label": "Color",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in color]
            },
            {
                "defaultDisplayOpen": False,
                "key": "area_paqueteria",
                "label": "Área",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in areas]
            },
        ]
        
    def get_filters_notas(self):
        reportado_por = self.get_employees_names()
        estatus = self.get_notas_estatus()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize(), "value": i} for i in estatus]
            },
            {
                "defaultDisplayOpen": False,
                "key": "creador_por",
                "label": "Creado por",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in reportado_por]
            },
   
        ]

    def get_filters_transportistas(self):
        estatus = self.get_transportistas_estatus()
        tipo_vehiculo = self.get_transportistas_tipo_vehiculo()
        empresa = self.get_transportistas_empresa()
        anden = self.get_transportistas_anden()
        return [
            {
                "defaultDisplayOpen": True,
                "key": "estatus",
                "label": "Estatus",
                "type": "multiple",
                "options": [{"label": i.capitalize().replace('_', ' '), "value": i} for i in estatus]
            },
            {
                "defaultDisplayOpen": False,
                "key": "tipo_de_vehiculo",
                "label": "Tipo de vehículo",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in tipo_vehiculo]
            },
            {
                "defaultDisplayOpen": False,
                "key": "proveedor_cliente",
                "label": "Empresa / Transportista",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in empresa]
            },
            {
                "defaultDisplayOpen": False,
                "key": "anden_asignado",
                "label": "Andén asignado",
                "type": "multiselect",
                "options": [{"label": i, "value": i} for i in anden]
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
        "incidencias_rondines": lambda: script_obj.get_filters_incidencias_rondines(),
        "incidencias": lambda: script_obj.get_filters_incidencias(),
        "fallas":      lambda: script_obj.get_filters_fallas(),
        "in_and_out":  lambda: script_obj.get_filters_in_and_out(),
        "transportistas": lambda: script_obj.get_filters_transportistas(),
        "pases":       lambda: script_obj.get_filters_pases(),
        "paqueteria": lambda:script_obj.get_filters_paqueteria(),
        "concesionados": lambda:script_obj.get_filters_concesionados(),
        "perdidos": lambda:script_obj.get_filters_perdidos(),
        "notas": lambda:script_obj.get_filters_notas()
    }

    action = dispatcher.get(option)
    if action:
        response = action()
        print(simplejson.dumps(response, indent=4))
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})