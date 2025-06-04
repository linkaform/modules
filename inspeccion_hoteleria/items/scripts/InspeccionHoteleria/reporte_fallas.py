# -*- coding: utf-8 -*-
from pickle import NONE
import re
import sys, simplejson

from inspeccion_hoteleria_utils import Inspeccion_Hoteleria

from account_settings import *


class Inspeccion_Hoteleria(Inspeccion_Hoteleria):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

        self.f.update({
            'tipo_ubicacion': '683d2970bb02c9bb7a0bfe1c',
            'habitacion_remodelada': '67f0844734855c523e1390d7',
            'ubicacion_nombre': '663e5c57f5b8a7ce8211ed0b',
            'tipo_area': '663e5e68f5b8a7ce8211ed18',
            'status_auditoria': '67f0844734855c523e139132',
            'habitacion_remodelada': '67f0844734855c523e1390d7',
        })

        self.form_ids = {
            'revison_habitacion': self.REVISON_HABITACION,
            'hi_parque_fundidora': self.HI_PARQUE_FUNDIDORA,
            'crowne_plaza_torreon': self.CROWNE_PLAZA_TORREN,
            'travo': self.TRAVO,
            'hie_tecnologico': self.HIE_TECNOLGICO,
            'wyndham_garden_mcallen': self.WYNDHAM_GARDEN_MCALLEN,
            'istay_victoria': self.ISTAY_VICTORIA,
            'hie_torreon': self.HIE_TORREN,
            'hilton_garden_silao': self.HILTON_GARDEN_SILAO,
            'ms_milenium': self.MS_MILENIUM,
            'istay_monterrey_historico': self.ISTAY_MONTERREY_HISTRICO,
            'hie_guanajuato': self.HIE_GUANAJUATO,
            'hie_silao': self.HIE_SILAO,
            'istay_ciudad_juarez': self.ISTAY_CIUDAD_JUREZ,
            'crowne_plaza_mty': self.CROWNE_PLAZA_MONTERREY,
            'hie_galerias': self.HIE_GALERIAS,
            'holiday_inn_tijuana': self.HOLIDAY_INN_TIJUANA,
        }

    def get_hoteles(self):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.Location.UBICACIONES,
                f"answers.{self.f['tipo_ubicacion']}": "hotel",
            }},
            {'$project': {
                '_id': 0,
                'nombre_hotel': f"$answers.{self.Location.f['location']}",
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))
        
        hoteles = {
            'hoteles': result
        }

        return hoteles
    
    def get_cantidad_si_y_no(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list},
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'paresAnswers': { '$objectToArray': "$answers" }
            }},
            {'$unwind': "$paresAnswers"},
            {'$match': {
                "paresAnswers.v": { '$in': [ "sí", "no" ] }
            }},
            {'$group': {
                '_id': "$paresAnswers.v",
                'total': { '$sum': 1 }
            }},
            {'$project': {
                '_id': 0,
                'respuesta': '$_id',
                'total': 1
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))
        
        return result
    
    def get_cantidad_habitaciones(self, ubicaciones_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
            f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area']}": "Habitación",
        }

        if isinstance(ubicaciones_list, list) and len(ubicaciones_list) > 1:
            match_query.update({
               f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": ubicaciones_list}
            })
        elif isinstance(ubicaciones_list, list) and len(ubicaciones_list) == 1:
            match_query.update({
               f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": self.unlist(ubicaciones_list)
            })

        query = [
            {'$match': match_query},
            {
                '$count': "totalHabitaciones"
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))
        result = self.unlist(result)

        return result

    def get_cantidad_inspecciones_y_remodeladas(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list}
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list)
            })

        query = [
            {'$match': match_query},
            {'$project': {
                'status_completada': {
                    '$cond': [
                        { '$eq': [f"$answers.{self.f['status_auditoria']}", "completada"] },
                        1,
                        0
                    ]
                },
                'habitacion_remodelada_si': {
                    '$cond': [
                        { '$eq': [f"$answers.{self.f['habitacion_remodelada']}", "sí"] },
                        1,
                        0
                    ]
                }
            }},
            {
                '$group': {
                    '_id': None,
                    'total_inspecciones_completadas': { '$sum': "$status_completada" },
                    'total_habitaciones_remodeladas': { '$sum': "$habitacion_remodelada_si" }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'total_inspecciones_completadas': 1,
                    'total_habitaciones_remodeladas': 1
                }
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))
        result = self.unlist(result)

        return result
    
    def resumen_cuatrimestres(self, calificaciones_dict):
        calificaciones = calificaciones_dict.get('cuatrimestres', [])
        if not calificaciones:
            return {
                'max_global': None,
                'min_global': None,
                'promedio_global': None
            }
    
        max_global = max(item['max'] for item in calificaciones)
        min_global = min(item['min'] for item in calificaciones)
        promedio_global = round(sum(item['promedio'] for item in calificaciones) / len(calificaciones), 2)
    
        return {
            'max_global': max_global,
            'min_global': min_global,
            'promedio_global': promedio_global
        }

    def get_calificaciones(self, forms_id_list=[], anio=None, cuatrimestres=None):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list},
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {
                "$lookup": {
                    "from": "voucher",
                    "let": { "vid": { "$toObjectId": "$voucher_id" } },
                    "pipeline": [
                        { "$match": { "$expr": { "$eq": [ "$_id", "$$vid" ] } } },
                        { "$project": { "_id": 0, "max_points": "$grading.max_points" } }
                    ],
                    "as": "voucherInfo"
                }
            },
            {
                "$unwind": {
                    "path": "$voucherInfo",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$match": {
                    "points": { "$type": "double" }
                }
            },
            {
                "$addFields": {
                    "calificacion": {
                        "$multiply": [
                            { "$divide": [ "$points", "$voucherInfo.max_points" ] },
                            100
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_fecha": {
                        "$cond": [
                            {
                                "$in": [
                                    { "$type": "$start_date" },
                                    [ "double", "int", "long", "decimal" ]
                                ]
                            },
                            { "$toDate": { "$multiply": [ "$start_date", 1000 ] } },
                            "$start_date"
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_anio": { "$year":  "$_fecha" },
                    "_mes":  { "$month": "$_fecha" }
                }
            },
            {
                "$addFields": {
                    "_cuatrimestre": {
                        "$ceil": { "$divide": [ "$_mes", 4 ] }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "anio":         "$_anio",
                        "cuatrimestre": "$_cuatrimestre"
                    },
                    "maxCalif": { "$max": "$calificacion" },
                    "minCalif": { "$min": "$calificacion" },
                    "avgCalif": { "$avg": "$calificacion" }
                }
            },
            {
                "$project": {
                    "_id":          0,
                    "anio":         "$_id.anio",
                    "cuatrimestre": "$_id.cuatrimestre",
                    "max":          { "$round": [ "$maxCalif", 2 ] },
                    "min":          { "$round": [ "$minCalif", 2 ] },
                    "promedio":     { "$round": [ "$avgCalif", 2 ] }
                }
            }
        ]

        match_filter = {}
        if anio is not None:
            match_filter["anio"] = anio
        if cuatrimestres:
            match_filter["cuatrimestre"] = {"$in": cuatrimestres}
        if match_filter:
            query.append({"$match": match_filter})

        query.append({
            "$sort": {
                "anio": 1,
                "cuatrimestre": 1
            }
        })

        result = self.format_cr(self.cr.aggregate(query))

        calificaciones = {
            'cuatrimestres': result,
        }

        resumen = self.resumen_cuatrimestres(calificaciones)
        calificaciones['resumen'] = resumen

        return calificaciones

    def porcentaje_propiedades_inspeccionadas(self, total_habitaciones, total_inspecciones):
        if total_habitaciones == 0:
            return 0.0
    
        porcentaje = (total_inspecciones / total_habitaciones) * 100
        return round(porcentaje, 2)

    def get_forms_id_list(self, hoteles=None):
        if not hoteles:
            return list(self.form_ids.values())

        hoteles_normalizados = [
            hotel.lower().replace(' ', '_') for hotel in hoteles
        ]

        ids = [
            self.form_ids[nombre]
            for nombre in hoteles_normalizados
            if nombre in self.form_ids
        ]

        return ids

    def get_report(self, anio=None, cuatrimestres=None, hoteles=[]):
        forms_id_list = self.get_forms_id_list(hoteles)
        print("forms_id_list", forms_id_list)

        cantidad_si_y_no = self.get_cantidad_si_y_no(forms_id_list=forms_id_list)

        total_habitaciones = self.get_cantidad_habitaciones(ubicaciones_list=hoteles)

        total_inspecciones_y_remodeladas = self.get_cantidad_inspecciones_y_remodeladas(forms_id_list=forms_id_list)

        calificaciones = self.get_calificaciones(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)

        propiedades_inspeccionadas = self.porcentaje_propiedades_inspeccionadas(
            total_habitaciones.get('totalHabitaciones', 0) if total_habitaciones else 0,
            total_inspecciones_y_remodeladas.get('total_inspecciones_completadas', 0) if total_inspecciones_y_remodeladas else 0
        )
        
        calificacion_x_hotel_grafica = self.get_cuatrimestres_by_hotel()


        cards = {
            'cantidad_si_y_no': cantidad_si_y_no,
            'total_habitaciones': total_habitaciones,
            'total_inspecciones_y_remodeladas': total_inspecciones_y_remodeladas,
            'calificaciones': calificaciones,
            'porcentaje_propiedades_inspeccionadas': propiedades_inspeccionadas,
            'calificacion_x_hotel_grafica': calificacion_x_hotel_grafica,
        }

        return cards

    def get_cuatrimestres_by_hotel(self, hoteles=[], anio=None, cuatrimestres=None):
        forms_id_list = self.get_forms_id_list(hoteles)
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            }) # type: ignore
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {
                "$lookup": {
                    "from": "voucher",
                    "let": { "vid": { "$toObjectId": "$voucher_id" } },
                    "pipeline": [
                        { "$match": { "$expr": { "$eq": [ "$_id", "$$vid" ] } } },
                        { "$project": { "_id": 0, "max_points": "$grading.max_points" } }
                    ],
                    "as": "voucherInfo"
                }
            },
            {
                "$unwind": {
                    "path": "$voucherInfo",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$match": {
                    "points": { "$type": "double" }
                }
            },
            {
                "$addFields": {
                    "calificacion": {
                        "$multiply": [
                            { "$divide": [ "$points", "$voucherInfo.max_points" ] },
                            100
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_fecha": {
                        "$cond": [
                            {
                                "$in": [
                                    { "$type": "$start_date" },
                                    [ "double", "int", "long", "decimal" ]
                                ]
                            },
                            { "$toDate": { "$multiply": [ "$start_date", 1000 ] } },
                            "$start_date"
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_anio": { "$year":  "$_fecha" },
                    "_mes":  { "$month": "$_fecha" }
                }
            },
            {
                "$addFields": {
                    "_cuatrimestre": {
                        "$ceil": { "$divide": [ "$_mes", 4 ] }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "form_id": "$form_id",
                        "anio": "$_anio",
                        "cuatrimestre": "$_cuatrimestre"
                    },
                    "maxCalif": { "$max": "$calificacion" },
                    "minCalif": { "$min": "$calificacion" },
                    "avgCalif": { "$avg": "$calificacion" }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "form_id": "$_id.form_id",
                    "anio": "$_id.anio",
                    "cuatrimestre": "$_id.cuatrimestre",
                    "max": { "$round": [ "$maxCalif", 2 ] },
                    "min": { "$round": [ "$minCalif", 2 ] },
                    "promedio": { "$round": [ "$avgCalif", 2 ] }
                }
            }
        ]

        match_filter = {}
        if anio is not None:
            match_filter["anio"] = anio
        if cuatrimestres:
            match_filter["cuatrimestre"] = {"$in": cuatrimestres}
        if match_filter:
            query.append({"$match": match_filter})

        query.append({
            "$sort": {
                "form_id": 1,
                "anio": 1,
                "cuatrimestre": 1
            }
        })

        result = self.format_cr(self.cr.aggregate(query))

        form_id_to_hotel = {v: k for k, v in self.form_ids.items()}
        for item in result:
            item["hotel"] = form_id_to_hotel.get(item["form_id"], item["form_id"])

        hoteles_list = []
        agrupados = {}

        for item in result:
            hotel = item["hotel"]
            if hotel not in agrupados:
                agrupados[hotel] = []
            agrupados[hotel].append({
                "anio": item["anio"],
                "cuatrimestre": item["cuatrimestre"],
                "max": item["max"],
                "min": item["min"],
                "promedio": item["promedio"]
            })

        for hotel, cuatrimestres in agrupados.items():
            hoteles_list.append({
                "hotel": hotel,
                "cuatrimestres": cuatrimestres
            })

        return hoteles_list

if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=True)
    module_obj.console_run()
    data = module_obj.data.get('data', {})
    option = data.get('option', 'get_cards')
    anio = data.get('anio', None)
    cuatrimestres = data.get('cuatrimestres', None)
    hoteles = data.get('hoteles', [])

    if option == 'get_hoteles':
        response = module_obj.get_hoteles()
    elif option == 'get_cards':
        response = module_obj.get_report(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)

    print(simplejson.dumps(response, indent=3))
    module_obj.HttpResponse({"data": response})