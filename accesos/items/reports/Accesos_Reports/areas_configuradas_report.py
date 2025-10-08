# -*- coding: utf-8 -*-
import sys, simplejson, math
from tokenize import group
import json
import math
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

print('inicia....')
#Se agrega path para que obtenga el archivo de Stock de este modulo
sys.path.append('/srv/scripts/addons/modules/accesos/items/scripts/Accesos')
from accesos_utils import Accesos
from calendar import monthrange
from pytz import timezone
from bson import ObjectId

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
    def get_areas(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.AREAS_DE_LAS_UBICACIONES
            }},
            {"$project": {
                "_id": 0,
                "area": f"$answers.{self.Location.f['area']}",
                "ubicacion": f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}"
            }}
        ]
        print('===================', query)
        resp = self.format_cr(self.cr.aggregate(query))
        format_resp = [{'area': i.get('area'), 'ubicacion': i.get('ubicacion')} for i in resp]
        return format_resp
        
    def get_areas_not_configured(self, locations=[]):
        areas = self.get_areas()
        
        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.CONFIGURACION_RECORRIDOS_FORM
        }
        
        if locations:
            match_query.update({
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": locations}
            })
        
        query = [
            {"$match": match_query},
            {"$project": {
                "areas_recorrido": f"$answers.{self.f['grupo_de_areas_recorrido']}"
            }},
            {"$unwind": "$areas_recorrido"},
            {"$group": {
                "_id": "$areas_recorrido",
                "cantidad": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "areas_configuradas": f"$_id.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.Location.f['area']}",
            }}
        ]

        resp = self.cr.aggregate(query)
        format_resp = []
        agrupado = {}
        if resp:
            resp = [i.get('areas_configuradas') for i in resp]
            format_resp = [a for a in areas if a.get('area') not in resp]
            for item in format_resp:
                ubicacion = item['ubicacion']
                area = item['area']
                if ubicacion not in agrupado:
                    agrupado[ubicacion] = []
                agrupado[ubicacion].append(area)
                format_resp = agrupado
        # Visualizar resultado de las areas no configuradas con acentos y ñ
        # print(simplejson.dumps(format_resp, indent=4, ensure_ascii=False))
        return format_resp

    def get_locations(self):
        selector = {}
        fields = ["_id", f"answers.{self.Location.f['location']}"]

        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 200
        }

        row_catalog = self.lkf_api.search_catalog(self.Location.UBICACIONES_CAT_ID, mango_query)
        format_row_catalog = [i.get(self.Location.f['location']) for i in row_catalog]
        return format_row_catalog

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()

    data = script_obj.data
    data = data.get('data', [])
    locations = data.get('locations', [])
    option = data.get('option', 'get_report')

    response = {}
    if option == 'get_report':
        response = script_obj.get_areas_not_configured(locations=locations)
    elif option == 'get_locations':
        response = script_obj.get_locations()

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})