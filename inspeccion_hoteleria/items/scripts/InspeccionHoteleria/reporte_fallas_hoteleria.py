# -*- coding: utf-8 -*-
from ast import mod
import enum
from pickle import NONE
import re
import sys, simplejson
import datetime
import unicodedata
from copy import deepcopy
from collections import Counter

from inspeccion_hoteleria_utils import Inspeccion_Hoteleria

from account_settings import *
from bson import ObjectId
import datetime

class Inspeccion_Hoteleria(Inspeccion_Hoteleria):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

        self.cr_inspeccion = self.net.get_collections(collection='inspeccion_hoteleria')
        
        self.labels_to_exclude = [
            'Nombre de la Camarista',
            'Habitación Remodelada',
            'Fotografía',
            'Comentarios',
            '¿La habitación es suite?',
            'Comentarios Generales',
            'STATUS AUDITORIA',
            'DIAS EN PROCESO',
            'status_auditoria',
            'dias_en_proceso'
        ]
        
        self.ids_to_exclude = [
            '67f81a13a48f0a56cf242558',
            '680bb736c2009c72987ba955',
            '680bb736c2009c72987ba957',
            '680bb736c2009c72987ba959',
            '680bb736c2009c72987ba95b',
            '680bb736c2009c72987ba95d',
            '680bb736c2009c72987ba95f',
            '680bb736c2009c72987ba961',
            '680bb736c2009c72987ba963',
            '680bb736c2009c72987ba965',
            '680bb736c2009c72987ba967'
        ]
        
        self.fallas_dict = {}

        self.f.update({
            'tipo_ubicacion': '683d2970bb02c9bb7a0bfe1c',
            'habitacion_remodelada': '67f0844734855c523e1390d7',
            'ubicacion_nombre': '663e5c57f5b8a7ce8211ed0b',
            'tipo_area': '663e5e68f5b8a7ce8211ed18',
            'status_auditoria': '67f0844734855c523e139132',
            'tipo_area_habitacion': '663e5e68f5b8a7ce8211ed18',
            'numero_habitacion': '680977786d022b9bff6e3645',
            'piso_habitacion': '680977786d022b9bff6e3646',
            'nombre_area_habitacion': '663e5d44f5b8a7ce8211ed0f',
            'nombre_camarista': '67f0844734855c523e1390d6'
        })

        self.form_ids = {
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

        self.hotel_name_abreviatura = {
            'HILTON GARDEN SILAO': 'HGI SILAO',
            'HOLIDAY INN TIJUANA': 'HI TIJUANA ZONA RIO',
            'WYNDHAM GARDEN MCALLEN': 'MCALLEN',
        }
        self.my_hotels_ids = []

        #TODO variable temporal para obtener el nombre del hotel segun el nombre de la forma
        self.form_name_hotel_name = {
            'Revisión de Habitaciones HIE Guanajuato': self.HIE_GUANAJUATO,
            'Revisión de Habitaciones Crowne Plaza Torreón': self.CROWNE_PLAZA_TORREN,
            'Revisión de Habitaciones HI Parque Fundidora': self.HI_PARQUE_FUNDIDORA,
            'Revisión de Habitaciones HIE Galerias': self.HIE_GALERIAS,
            'Revisión de Habitaciones Crowne Plaza Monterrey': self.CROWNE_PLAZA_MONTERREY,
            'Revisión de Habitaciones HIE Silao': self.HIE_SILAO,
            'Revisión de Habitaciones HIE Tecnológico': self.HIE_TECNOLGICO,
            'Revisión de Habitaciones HIE Torreón': self.HIE_TORREN,
            'Revisión de Habitaciones Hilton Garden Silao': self.HILTON_GARDEN_SILAO,
            'Revisión de Habitaciones Holiday Inn Tijuana': self.HOLIDAY_INN_TIJUANA,
            'Revisión de Habitaciones ISTAY Ciudad Juárez': self.ISTAY_CIUDAD_JUREZ,
            'Revisión de Habitaciones ISTAY Monterrey Histórico': self.ISTAY_MONTERREY_HISTRICO,
            'Revisión de Habitaciones ISTAY Victoria': self.ISTAY_VICTORIA,
            'Revisión de Habitaciones MS Milenium': self.MS_MILENIUM,
            'Revisión de Habitaciones TRAVO': self.TRAVO,
            'Revisión de Habitaciones WYNDHAM GARDEN MCALLEN': self.WYNDHAM_GARDEN_MCALLEN,
        }


    def normalize_types(self, obj):
        if isinstance(obj, list):
            return [self.normalize_types(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self.normalize_types(v) for k, v in obj.items()}
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return obj
        
    def normaliza_texto(self, texto):
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = texto.replace(" ", "_")
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
        return texto
    
    def limpia_keys_dict(self, dic):
        return {k.replace('.', '_'): v for k, v in dic.items()}

    @property
    def my_hotels(self):
        if not self.my_hotels_ids:
            # forms = self.lkf_api.get_all_forms()
            user_id = self.user.get('user_id')
            # user_id = 7742
            forms = self.lkf_api.get_user_forms(user_id)
            all_forms = forms.get('data')
            #TODO cambiar a obtener ids de la forma
            forms = [f['name'] for f in all_forms if f.get('name')]
            my_hotels = []
            # for f in forms:
            #     if self.form_name_hotel_name.get(f):
            #     my_hotels.append(self.form_name_hotel_name.get(f))
            my_hotels = [self.form_name_hotel_name[f] for f in forms if self.form_name_hotel_name.get(f)]
            # my_hotels = [f['form_id'] for f in forms if f.get('form_id')]
            self.my_hotels_ids = my_hotels
        return self.my_hotels_ids


    def get_hoteles(self):
        # print('mis hoteles', self.my_hotels)
        # query = [
        #     {'$match': {
        #         "deleted_at": {"$exists": False},
        #         "form_id": self.Location.UBICACIONES,
        #         f"answers.{self.f['tipo_ubicacion']}": "hotel",
        #     }},
        #     {'$project': {
        #         '_id': 0,
        #         'nombre_hotel': f"$answers.{self.Location.f['location']}",
        #     }},
        # ]

        # result = self.format_cr(self.cr.aggregate(query))
        # TODO
        # Temp se descarta corporativo mexico porque no cuenta con una forma de inspeccion
        my_hotels_set = set(self.my_hotels)
        # result = [
        #     h for h in result
        #     if h.get('nombre_hotel', '').strip().upper() != 'CORPORATIVO MEXICO'
        # ]
        filtered_form_ids = [
            {'nombre_hotel': name.upper().replace('_', ' ')}
            for name, form_id in self.form_ids.items()
            if form_id in my_hotels_set
        ]
        # * Cambia MCALLEN por WYNDHAM GARDEN MCALLEN para que sea el nombre valido en la inspeccion
        # for h in filtered_form_ids:
        #     if h.get('nombre_hotel', '').strip().upper() == 'MCALLEN':
        #         h['nombre_hotel'] = 'WYNDHAM GARDEN MCALLEN'
        
        hoteles = {
            'hoteles': filtered_form_ids
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

        if isinstance(ubicaciones_list, list):
            ubicaciones_list = [
                str(u).replace('_', ' ').upper() for u in ubicaciones_list
            ]

        if isinstance(ubicaciones_list, list):
            ubicaciones_list = [
                self.hotel_name_abreviatura.get(u.replace('_', ' ').upper(), u)
                for u in ubicaciones_list
            ]

        match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.Location.AREAS_DE_LAS_UBICACIONES,
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

    def calculate_metrics(self, inspecciones=[]):
        if not inspecciones:
            return {
                'total_inspecciones_completadas': 0,
                'total_habitaciones_remodeladas': 0,
                'cards': {},
                'fallas': {'por_hotel': [], 'totales': []},
                'mejor_y_peor_habitacion': {'mejor_habitacion': None, 'habitacion_mas_fallas': None}
            }

        total_inspecciones_completadas = 0
        total_habitaciones_remodeladas = 0
        
        # Cards metrics
        cards_fallas = 0
        cards_aciertos = 0
        cards_grades = []
        
        # Flaws metrics
        fallas_by_hotel = {}  # {hotel: Counter}
        fallas_totales = Counter()
        
        # Rankings metrics (group by hotel, habitacion)
        rankings_grouped = {}

        for insp in inspecciones:
            # 1. Cantidad inspecciones y remodeladas
            if insp.get('status_auditoria') == 'completada':
                total_inspecciones_completadas += 1
            
            remodelada = str(insp.get('habitacion_remodelada', '')).lower()
            if remodelada in ['sí', 'si']:
                total_habitaciones_remodeladas += 1 # Be wary of overcounting if iterating same room multiple times for other metrics? 
                # Actually original logic counted 'sí' per inspection record, so this is consistent.

            detalle = insp.get('inspeccion', {})
            if not detalle:
                continue

            # 2. Cards
            cards_fallas += detalle.get('fallas', 0)
            cards_aciertos += detalle.get('aciertos', 0)
            grade = detalle.get('grade')
            if grade is not None:
                cards_grades.append(grade)

            # 3. Fallas
            hotel = insp.get('hotel', 'N/A')
            if hotel not in fallas_by_hotel:
                fallas_by_hotel[hotel] = Counter()
            
            field_labels = detalle.get('field_label', {})
            for falla_id, falla_nombre in field_labels.items():
                fallas_by_hotel[hotel][falla_nombre] += 1
                fallas_totales[falla_nombre] += 1

            # 4. Rankings
            habitacion = insp.get('habitacion', 'N/A')
            key = (hotel, habitacion)
            if key not in rankings_grouped:
                rankings_grouped[key] = {
                    '_id': {'hotel': hotel, 'habitacion': habitacion},
                    'grades': [],
                    'fallas': [],
                    # 'max_points': [], 'obtained_points': [], 'aciertos': [], 'total_inspecciones': 0 # not strictly needed for final min/max logic but kept if useful
                }
            rankings_grouped[key]['grades'].append(detalle.get('grade', 0))
            rankings_grouped[key]['fallas'].append(detalle.get('fallas', 0))

        # Finalize Cards
        grade_max = max(cards_grades) if cards_grades else 0
        grade_min = min(cards_grades) if cards_grades else 0
        grade_avg = round(sum(cards_grades) / len(cards_grades), 2) if cards_grades else 0
        
        cards_result = {
            'habitaciones_remodeladas': total_habitaciones_remodeladas, # reusing this count
            'inspecciones_realizadas': len(inspecciones),
            'total_fallas': cards_fallas,
            'total_aciertos': cards_aciertos,
            'grade_max': grade_max,
            'grade_min': grade_min,
            'grade_avg': grade_avg
        }

        # Finalize Fallas
        fallas_por_hotel = []
        for hotel, counter in fallas_by_hotel.items():
            hotel_data = {"hotel": hotel}
            hotel_data.update(counter)
            fallas_por_hotel.append(hotel_data)
        
        fallas_list_totales = [{"falla": k, "total": v} for k, v in fallas_totales.items()]
        fallas_list_totales.sort(key=lambda x: x["total"], reverse=True)
        
        fallas_result = {
            "por_hotel": fallas_por_hotel,
            "totales": fallas_list_totales
        }

        # Finalize Rankings
        rankings_list = list(rankings_grouped.values())
        valid_rankings = [i for i in rankings_list if i.get('fallas') and len(i['fallas']) > 0 and sum(i['fallas']) > 0] # sum check to match 'exists and len > 0' intent loosely, or just check if list has nonzero elements? 
        # Original: [i for i in result if i.get('fallas') and len(i['fallas']) > 0]
        # In original aggregation, 'fallas' was array of ints. len > 0 means at least one inspection had fallas field? No, 'fallas' was pushed. So at least one inspection.
        # But wait, original code: `inspecciones_validas = [i for i in result if i.get('fallas') and len(i['fallas']) > 0]`
        # If an inspection has 0 fallas, it's pushed as 0. So list length is number of inspections.
        # So it just filters out rooms with NO inspections? But aggregation groups by room, so only rooms with inspections exist.
        # Ah, maybe it filters based on VALID data. 
        # Let's stick to: list is not empty.
        
        if not rankings_list:
            mejor = peor = None
        else:
            # Original logic: max of max grades
            mejor = max(rankings_list, key=lambda x: max(x['grades']) if x['grades'] else 0)
            # Original logic: max of max fallas
            peor = max(rankings_list, key=lambda x: max(x['fallas']) if x['fallas'] else 0)

        rankings_result = {
            'mejor_habitacion': mejor,
            'habitacion_mas_fallas': peor
        }

        return {
            'total_inspecciones_completadas': total_inspecciones_completadas,
            'total_habitaciones_remodeladas': total_habitaciones_remodeladas,
            'cards': cards_result,
            'fallas': fallas_result,
            'mejor_y_peor_habitacion': rankings_result
        }
    
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
        print("hoteles", hoteles)
        
        if not hoteles:
            my_hotels_set = self.form_ids.values()
            my_hotels_set = list(my_hotels_set)
            return my_hotels_set

        hoteles_normalizados = [
            hotel.lower().replace(' ', '_') for hotel in hoteles
        ]

        ids = [
            self.form_ids[nombre]
            for nombre in hoteles_normalizados
            if nombre in self.form_ids
        ]

        return ids

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
        # Si pides los 3 cuatrimestres, calculamos el actual y los 2 anteriores dinámicamente
        if cuatrimestres and set(cuatrimestres) == {1, 2, 3}:
            now = datetime.datetime.now()
            current_year = now.year
            current_cuatri = (now.month - 1) // 4 + 1
            
            target_periods = []
            temp_y, temp_c = current_year, current_cuatri
            for _ in range(3):
                target_periods.append({"anio": temp_y, "cuatrimestre": temp_c})
                temp_c -= 1
                if temp_c < 1:
                    temp_c = 3
                    temp_y -= 1

            query.append({
                "$match": {
                    "$or": target_periods
                }
            })
        else:
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

    def get_fallas(self, inspecciones):
        if not inspecciones:
            return {"por_hotel": [], "totales": []}

        counts_by_hotel = {}  # {hotel_name: Counter({falla_name: count})}
        totales_counter = Counter()

        for insp in inspecciones:
            hotel = insp.get('hotel', 'N/A')
            if hotel not in counts_by_hotel:
                counts_by_hotel[hotel] = Counter()
            
            detalle = insp.get('inspeccion', {})
            # El usuario indica que las fallas están en inspeccion.field_label
            field_labels = detalle.get('field_label', {})
            
            for falla_id, falla_nombre in field_labels.items():
                counts_by_hotel[hotel][falla_nombre] += 1
                totales_counter[falla_nombre] += 1

        # Formatear result_por_hotel
        result_por_hotel = []
        for hotel, counter in counts_by_hotel.items():
            hotel_data = {"hotel": hotel}
            hotel_data.update(counter)
            result_por_hotel.append(hotel_data)

        # Formatear totales_list
        totales_list = [{"falla": k, "total": v} for k, v in totales_counter.items()]
        totales_list.sort(key=lambda x: x["total"], reverse=True)

        return {
            "por_hotel": result_por_hotel,
            "totales": totales_list
        }

    def get_habitaciones_by_hotel(self, hotel_name, fallas=None, anio=None, cuatrimestres=None):
        hotel_name_list = [hotel_name.lower().replace(' ', '_')]
        form_id = self.get_forms_id_list(hotel_name_list)
        form_id = self.unlist(form_id)

        if hotel_name in self.hotel_name_abreviatura:
            hotel_name = self.hotel_name_abreviatura[hotel_name]

        lookup_pipeline = [
            {
                '$match': {
                    '$expr': {
                        '$and': [
                        {'$eq': ['$form_id', form_id]},
                        {'$eq': [f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}", "$$nombre_hab"]}
                        ]
                    }
                }
            }
        ]

        if anio is not None or cuatrimestres:
            lookup_pipeline.extend([
                {'$addFields': {
                    "_fecha": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at",
                            "timezone": "America/Mexico_City"
                        }
                    },
                    "_anio": {
                        "$year": {
                            "$dateFromString": {
                                "dateString": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d",
                                        "date": "$created_at",
                                        "timezone": "America/Mexico_City"
                                    }
                                }
                            }
                        }
                    },
                    "_mes": {
                        "$month": {
                            "$dateFromString": {
                                "dateString": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d",
                                        "date": "$created_at",
                                        "timezone": "America/Mexico_City"
                                    }
                                }
                            }
                        }
                    },
                }},
                {'$addFields': {
                    "_cuatrimestre": {
                        "$ceil": {"$divide": ["$_mes", 4]}
                    }
                }},
            ])
            match_date = {}
            if anio is not None:
                match_date["_anio"] = anio
            if cuatrimestres:
                match_date["_cuatrimestre"] = {"$in": cuatrimestres}
            
            if match_date:
                lookup_pipeline.append({'$match': match_date})

        lookup_pipeline.extend([
            {
                '$sort': {'created_at': -1}
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    '_id': 1
                }
            }
        ])

        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.Location.AREAS_DE_LAS_UBICACIONES,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}": hotel_name,
                f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area_habitacion']}": "Habitación",
            }},
            {'$project': {
                '_id': 0,
                'nombre_area_habitacion': f"$answers.{self.f['nombre_area_habitacion']}",
                'numero_habitacion': f"$answers.{self.f['numero_habitacion']}",
            }},
            {
            '$lookup': {
                'from': 'form_answer',
                'let': {
                    'nombre_hab': '$nombre_area_habitacion'
                },
                'pipeline': lookup_pipeline,
                'as': 'inspeccion'
            }
            },
            {
            '$addFields': {
                'inspeccion_id': {
                    '$cond': [
                        {'$gt': [{'$size': '$inspeccion'}, 0]},
                        {'$arrayElemAt': ['$inspeccion._id', 0]},
                        None
                    ]
                }
            }
            },
            {
                '$project': {
                    'nombre_area_habitacion': 1,
                    'numero_habitacion': 1,
                    'inspeccion_id': 1
                }
            }
        ]

        habitaciones = list(self.cr.aggregate(query))
        habitaciones_id = [hab.get('inspeccion_id') for hab in habitaciones if hab.get('inspeccion_id')]
        if habitaciones_id:
            query = {"_id": {"$in": habitaciones_id}}
            res = self.cr_inspeccion.find(query)
            inspecciones = list(res)
            inspecciones_dict = {x['_id']: x for x in inspecciones}
        else:
            inspecciones_dict = {}
        for hab in habitaciones:
            inspeccion_id = hab.get('inspeccion_id')
            if inspecciones_dict.get(inspeccion_id):
                hab['inspeccion_habitacion'] = inspecciones_dict[inspeccion_id]
            else:
                hab['inspeccion_habitacion'] = None

        habitaciones = self.normalize_types(habitaciones)
        if fallas:
            fallas_normalizadas = set(self.normaliza_texto(f) for f in fallas)
            for hab in habitaciones:
                inspeccion_habitacion = hab.get('inspeccion_habitacion') # type: ignore
                if inspeccion_habitacion and inspeccion_habitacion.get('fallas') == 0:  # type: ignore
                    hab['inspeccion_habitacion'] = None # type: ignore
                    hab['sin_fallas'] = True # type: ignore
                else:
                    inspeccion = hab.get('inspeccion_habitacion') # type: ignore
                    if inspeccion and 'field_label' in inspeccion:
                        labels = inspeccion['field_label'].values() # type: ignore
                        labels_normalizadas = set(self.normaliza_texto(l) for l in labels)
                        if not any(falla in labels_normalizadas for falla in fallas_normalizadas):
                            hab['inspeccion_habitacion'] = None # type: ignore
        else:
            #TODO Mejorar este proceso, se repite el mismo proceso que arriba solo que lo hace cuando no hay fallas
            for hab in habitaciones:
                inspeccion_habitacion = hab.get('inspeccion_habitacion') # type: ignore
                if inspeccion_habitacion and inspeccion_habitacion.get('fallas') == 0:  # type: ignore
                    hab['inspeccion_habitacion'] = None # type: ignore
                    hab['sin_fallas'] = True # type: ignore
                else:
                    hab['inspeccion_habitacion'] = None # type: ignore

        return {
            'habitaciones': habitaciones,
        }
    
    def get_room_data(self, hotel_name, room_id):
        hotel_name_list = [hotel_name.lower().replace(' ', '_')]
        form_id = self.get_forms_id_list(hotel_name_list)
        form_id = self.unlist(form_id)

        query = [
            {
                '$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": form_id,
                    f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}": room_id,
                }
            },
            {
              '$sort': {
                   'created_at': -1
                }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    '_id': 1,
                    'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                    'habitacion_remodelada': f"$answers.{self.f['habitacion_remodelada']}",
                    'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                    'created_by_name': "$created_by_name",
                    'created_at': "$created_at",
                    'updated_at': "$updated_at",
                }
            },
            {
                '$lookup': {
                    'from': 'inspeccion_hoteleria',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'inspeccion'
                }
            },
            {'$project': {
                    '_id': 1,
                    'habitacion': '$habitacion',
                    'habitacion_remodelada': '$habitacion_remodelada',
                    'nombre_camarista': '$nombre_camarista',
                    'created_by_name': '$created_by_name',
                    'created_at': '$created_at',
                    'updated_at': '$updated_at',
                    'inspeccion': {'$first': '$inspeccion'}
                }
            },
        ]
        result = self.cr.aggregate(query)
        x = {}
        for x in result:
            x['_id'] = str(x['_id'])
            if x.get('inspeccion'):
                x['inspeccion'].pop('_id', None)
                x['inspeccion'].pop('created_at', None)
            x['created_at'] = self.get_date_str(x['created_at'])
            x['updated_at'] = self.get_date_str(x['updated_at'])
        if not x:
            return {"mensaje": "No hay inspección para esta habitación"}
        return x

    def get_table_habitaciones_inspeccionadas(self, inspecciones=[]):
        output = []
        for doc in inspecciones:
            inspeccion = doc.get('inspeccion', {})
            created_at = doc.get('created_at')
            if isinstance(created_at, datetime.datetime):
                fecha_str = created_at.strftime('%d/%m/%Y')
            else:
                fecha_str = ""

            if inspeccion:
                output.append({
                    'habitacion': doc.get('habitacion'),
                    'hotel': doc.get('hotel'),
                    'nombre_camarista': doc.get('nombre_camarista'),
                    'grade': inspeccion.get('grade'),
                    'total_aciertos': inspeccion.get('aciertos'),
                    'total_fallas': inspeccion.get('fallas'),
                    'created_at': fecha_str
                })

        return output
    
    def get_mejor_y_peor_habitacion(self, inspecciones=[]):
        if not inspecciones:
            return {'mejor_habitacion': None, 'habitacion_mas_fallas': None}

        # Agrupar por (hotel, habitación)
        grouped = {}
        for insp in inspecciones:
            hotel = insp.get('hotel', 'N/A')
            habitacion = insp.get('habitacion', 'N/A')
            key = (hotel, habitacion)
            
            if key not in grouped:
                grouped[key] = {
                    '_id': {'hotel': hotel, 'habitacion': habitacion},
                    'grades': [],
                    'fallas': [],
                    'max_points': [],
                    'obtained_points': [],
                    'aciertos': [],
                    'total_inspecciones': 0
                }
            
            detalle = insp.get('inspeccion', {})
            if detalle:
                grouped[key]['grades'].append(detalle.get('grade', 0))
                grouped[key]['fallas'].append(detalle.get('fallas', 0))
                grouped[key]['max_points'].append(detalle.get('max_points', 0))
                grouped[key]['obtained_points'].append(detalle.get('obtained_points', 0))
                grouped[key]['aciertos'].append(detalle.get('aciertos', 0))
                grouped[key]['total_inspecciones'] += 1

        result = list(grouped.values())

        # El código original filtraba i.get('fallas') y len(i['fallas']) > 0
        inspecciones_validas = [i for i in result if i.get('fallas') and len(i['fallas']) > 0]

        if not inspecciones_validas:
            mejor = peor = None
        else:
            # Mejor habitación basada en la calificación más alta obtenida en cualquiera de sus inspecciones
            mejor = max(inspecciones_validas, key=lambda x: max(x['grades']) if x['grades'] else 0)
            # Habitación con más fallas basada en el conteo máximo de fallas en cualquiera de sus inspecciones
            peor = max(inspecciones_validas, key=lambda x: max(x['fallas']) if x['fallas'] else 0)
    
        return {
            'mejor_habitacion': mejor,
            'habitacion_mas_fallas': peor
        }
    
    def get_graph_radar(self, inspecciones=[]):
        if not inspecciones:
            return {'radar_data': []}

        radar_data = []
        for insp in inspecciones:
            # Se extrae la información base
            item = {
                '_id': str(insp.get('_id')),
                'hotel': insp.get('hotel'),
                'created_at': insp.get('created_at').strftime("%d/%m/%Y") if isinstance(insp.get('created_at'), datetime.datetime) else insp.get('created_at')
            }
            
            # Se agregan las secciones de la inspección
            detalle = insp.get('inspeccion', {})
            item['sections'] = detalle.get('sections', {})
            
            radar_data.append(item)

        return {
            'radar_data': radar_data
        }

    def get_fotografias(self, inspecciones=[]):
        if not inspecciones:
            return {'hoteles_fotografias': []}

        hoteles_fotografias = []
        for insp in inspecciones:
            # Se extrae la información base
            item = {
                '_id': str(insp.get('_id')),
                'hotel': insp.get('hotel'),
                'habitacion': insp.get('habitacion')
            }
            
            # Se procesa la media vinculándola con las etiquetas (fallas)
            detalle = insp.get('inspeccion', {})
            media = detalle.get('media', {})
            field_label = detalle.get('field_label', {})
            
            new_media = {}
            for key, files in media.items():
                falla = field_label.get(key, None)
                if not falla:
                    continue
                new_media[key] = [
                    {'file_url': f['file_url'], 'falla': falla}
                    for f in files if 'file_url' in f
                ]
            
            item['media'] = new_media
            hoteles_fotografias.append(item)

        return {
            'hoteles_fotografias': hoteles_fotografias
        }
    
    def get_comentarios(self, inspecciones=[]):
        if not inspecciones:
            return {'hoteles_comentarios': []}

        hoteles_comentarios = []
        for insp in inspecciones:
            # Se extrae la información de la inspección
            detalle = deepcopy(insp.get('inspeccion', {}))
            if not detalle:
                continue

            # Se inyectan el hotel y la habitación del registro principal
            detalle['_id'] = str(insp.get('_id'))
            detalle['hotel'] = insp.get('hotel')
            detalle['habitacion'] = insp.get('habitacion')
            detalle.pop('created_at', None)

            # Se formatea la media dejando solo file_url
            media = detalle.get('media', {})
            new_media = {}
            for key, files in media.items():
                new_media[key] = [{'file_url': f['file_url']} for f in files if 'file_url' in f]
            detalle['media'] = new_media

            hoteles_comentarios.append(detalle)

        return {
            'hoteles_comentarios': hoteles_comentarios
        }

    def get_cards(self, inspecciones=[]):
        if not inspecciones:
            return {}

        total_fallas = 0
        total_aciertos = 0
        habitaciones_remodeladas = 0
        inspecciones_realizadas = len(inspecciones)
        grades = []

        for insp in inspecciones:
            # Remodelada
            remodelada = str(insp.get('habitacion_remodelada', '')).lower()
            if remodelada in ['sí', 'si']:
                habitaciones_remodeladas += 1
            
            # Datos de inspeccion
            detalle = insp.get('inspeccion', {})
            if detalle:
                total_fallas += detalle.get('fallas', 0)
                total_aciertos += detalle.get('aciertos', 0)
                grade = detalle.get('grade')
                if grade is not None:
                    grades.append(grade)

        grade_max = max(grades) if grades else 0
        grade_min = min(grades) if grades else 0
        grade_avg = round(sum(grades) / len(grades), 2) if grades else 0

        return {
            'habitaciones_remodeladas': habitaciones_remodeladas,
            'inspecciones_realizadas': inspecciones_realizadas,
            'total_fallas': total_fallas,
            'total_aciertos': total_aciertos,
            'grade_max': grade_max,
            'grade_min': grade_min,
            'grade_avg': grade_avg
        }

    def get_inspecciones(self, forms_id_list=[], anio=None, cuatrimestres=None):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query["form_id"] = {"$in": forms_id_list}
        else:
            match_query["form_id"] = self.unlist(forms_id_list)

        query = [
            {'$match': match_query},
            {'$addFields': {
                "_fecha": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$created_at",
                        "timezone": "America/Mexico_City"
                    }
                },
                "_anio": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "America/Mexico_City"
                                }
                            }
                        }
                    }
                },
                "_mes": {
                    "$month": {
                        "$dateFromString": {
                            "dateString": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "America/Mexico_City"
                                }
                            }
                        }
                    }
                },
            }},
            {'$addFields': {
                "_cuatrimestre": {
                    "$ceil": {"$divide": ["$_mes", 4]}
                }
            }},
        ]

        # Filtro por año y cuatrimestre si se pasan como parámetro
        match_cuatrimestre = {}
        if anio is not None:
            match_cuatrimestre["_anio"] = anio
        if cuatrimestres:
            match_cuatrimestre["_cuatrimestre"] = {"$in": cuatrimestres}
        if match_cuatrimestre:
            query.append({"$match": match_cuatrimestre})

        query += [
            {'$project': {
                '_id': 1,
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                'habitacion_remodelada': {"$ifNull": [f"$answers.{self.f['habitacion_remodelada']}", "No"]},
                'status_auditoria': f"$answers.{self.f['status_auditoria']}",
                'created_at': '$created_at',
            }},
            {
                '$lookup': {
                    'from': 'inspeccion_hoteleria',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'inspeccion'
                }
            },
            {
                '$unwind': {
                    'path': '$inspeccion',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$addFields': {
                    'inspeccion': '$inspeccion'
                }
            }
        ]
        result = self.cr.aggregate(query)
        return list(result)
    
    def get_report(self, anio=None, cuatrimestres=None, hoteles=[]):
        # Normaliza los nombres de hoteles usando las abreviaturas si corresponde
        if hoteles:
            hoteles_actualizados = []
            for hotel in hoteles:
                encontrado = False
                hotel_norm = hotel.lower().replace(' ', '_')
                for k, v in self.hotel_name_abreviatura.items():
                    v_norm = v.lower().replace(' ', '_')
                    k_norm = k.lower().replace(' ', '_')
                    if v_norm == hotel_norm:
                        hoteles_actualizados.append(k_norm)
                        encontrado = True
                        break
                if not encontrado:
                    hoteles_actualizados.append(hotel_norm)
            hoteles = hoteles_actualizados

        forms_id_list = self.get_forms_id_list(hoteles)
        # self.get_labels(forms_id_list=forms_id_list)

        inspecciones = self.get_inspecciones(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)
        
        # 1. Metricas consolidadas (Cards, Fallas, Rankings, Totales)
        metrics = self.calculate_metrics(inspecciones=inspecciones)
        
        # 2. Total Habitaciones (consulta aparte necesaria)
        total_habitaciones = self.get_cantidad_habitaciones(ubicaciones_list=hoteles)
        
        # 3. Propiedades Inspeccionadas
        total_inspecciones = metrics.get('total_inspecciones_completadas', 0)
        propiedades_inspeccionadas = self.porcentaje_propiedades_inspeccionadas(
            total_habitaciones.get('totalHabitaciones', 0), 
            total_inspecciones
        )
        
        # 4. Tablas que requieren procesamiento especifico
        habitaciones_inspeccionadas = self.get_table_habitaciones_inspeccionadas(inspecciones=inspecciones)
        
        # 5. Detalles
        rooms_details = self.get_rooms_details(inspecciones=inspecciones)

        report_data = {
            'cards': metrics.get('cards'),
            'porcentaje_propiedades_inspeccionadas': propiedades_inspeccionadas,
            'fallas': metrics.get('fallas'),
            'table_habitaciones_inspeccionadas': habitaciones_inspeccionadas,
            'mejor_y_peor_habitacion': metrics.get('mejor_y_peor_habitacion'),
        }

        # Nota: get_comentarios original retornaba {'hoteles_comentarios': [...]}, get_rooms_details retornaba [...].
        # Ajustamos arriba para que hoteles_comentarios en report_data sea compatible si el front espera esa key o la lista directa.
        # Revisando el código original: get_comentarios retornaba diccionario. get_rooms_details retornaba lista.
        # En el report_data original: 'hoteles_comentarios': hoteles_comentarios (que era el dict).
        # Así que aquí lo empaquetamos: 'hoteles_comentarios': {'hoteles_comentarios': rooms_details} (si la estructura de objetos es idéntica).
        # Verificando estructura de objetos: ambos tienen media formateada solo con url, id, hotel, habitacion. Sí, son compatibles.

        return report_data

    def get_pdf(self, record_id, template_id=131835, name_pdf='Inspeccion de Habitacion'):
        return self.lkf_api.get_pdf_record(record_id, template_id=template_id, name_pdf=name_pdf, send_url=True)
    
    def get_labels(self, forms_id_list=[]):
        form_id = [self.REVISON_HABITACION] # * Toma el id de la forma maestro para no demorar tanto
        form_data = self.lkf_api.get_form_for_answer(self.REVISON_HABITACION)
        fields = form_data[0].get('fields', [])
        for field in fields:
            if not field.get('catalog'):
                if not field.get('label') in self.labels_to_exclude and not field.get('field_id') in self.ids_to_exclude:
                    self.fallas_dict.update({
                        self.normaliza_texto(field.get('label')): field.get('field_id')
                    })
                        
    def get_rooms_details(self, inspecciones=[]):
        if not inspecciones:
            return []

        rooms_details = []
        for insp in inspecciones:
            # Se extrae la información de la inspección
            detalle = deepcopy(insp.get('inspeccion', {}))
            if not detalle:
                continue

            # Se inyectan el hotel y la habitación del registro principal
            detalle['_id'] = str(insp.get('_id'))
            detalle['hotel'] = insp.get('hotel')
            detalle['habitacion'] = insp.get('habitacion')
            detalle.pop('created_at', None)

            # Se formatea la media dejando solo file_url
            media = detalle.get('media', {})
            new_media = {}
            for key, files in media.items():
                new_media[key] = [{'file_url': f['file_url']} for f in files if 'file_url' in f]
            detalle['media'] = new_media

            rooms_details.append(detalle)

        return rooms_details

    def get_cantidad_habitaciones_x_hotel(self, hoteles=[]):
        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.Location.AREAS_DE_LAS_UBICACIONES,
            f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area']}": "Habitación"
        }

        if hoteles:
            if len(hoteles) > 1:
                match.update({
                    f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": hoteles},
                })
            else:
                match.update({
                    f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": self.unlist(hoteles),
                })
        
        query = [
            {'$match': match},
            {'$group': {
                '_id': f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                'total_habitaciones': {'$sum': 1}
            }},
            {'$project': {
                '_id': 0,
                'hotel': '$_id',
                'total_habitaciones': 1
            }},
            {'$sort': {
                'hotel': 1
            }}
        ]
        
        response = self.cr.aggregate(query)
        response = list(response)
        for item in response:
            hotel_nombre = item.get('hotel', '')
            if hotel_nombre in self.hotel_name_abreviatura:
                item['hotel'] = self.hotel_name_abreviatura[hotel_nombre]
        print("Cantidad de habitaciones por hotel:", simplejson.dumps(response, indent=2))
        return response

    def get_multi_line_chart_inspecciones(self, hoteles=[], anio=None, cuatrimestres=None):
        print('================', hoteles)
        hoteles_con_abreviatura = []
        if hoteles:
            for hotel in hoteles:
                hotel_encontrado = None
                for nombre_completo, abreviatura in self.hotel_name_abreviatura.items():
                    if hotel == nombre_completo:
                        hotel_encontrado = abreviatura
                        break
                if hotel_encontrado:
                    hoteles_con_abreviatura.append(hotel_encontrado)
                else:
                    hoteles_con_abreviatura.append(hotel)
        
        if hoteles:
            hoteles_actualizados = []
            for hotel in hoteles:
                encontrado = False
                hotel_norm = hotel.lower().replace(' ', '_')
                for k, v in self.hotel_name_abreviatura.items():
                    v_norm = v.lower().replace(' ', '_')
                    k_norm = k.lower().replace(' ', '_')
                    if v_norm == hotel_norm:
                        hoteles_actualizados.append(k_norm)
                        encontrado = True
                        break
                if not encontrado:
                    hoteles_actualizados.append(hotel_norm)
            hoteles = hoteles_actualizados

        forms_id_list = self.get_forms_id_list(hoteles)
        habitaciones_x_hotel = self.get_cantidad_habitaciones_x_hotel(hoteles=hoteles_con_abreviatura)

        match_query = {
            "deleted_at": {"$exists": False},
        }
        
        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })
        
        pipeline = [
            {'$match': match_query},
            {'$addFields': {
                "_fecha": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$created_at",
                        "timezone": "America/Mexico_City"
                    }
                },
                "_anio": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "America/Mexico_City"
                                }
                            }
                        }
                    }
                },
                "_mes": {
                    "$month": {
                        "$dateFromString": {
                            "dateString": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "America/Mexico_City"
                                }
                            }
                        }
                    }
                },
                "_dia": {
                    "$dayOfYear": {
                        "$dateFromString": {
                            "dateString": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "America/Mexico_City"
                                }
                            }
                        }
                    }
                }
            }},
            {'$addFields': {
                "_cuatrimestre": {
                    "$ceil": {"$divide": ["$_mes", 4]}
                }
            }},
        ]
        
        # Filtro por año y cuatrimestre
        match_cuatrimestre = {}
        if anio is not None:
            match_cuatrimestre["_anio"] = anio
        if cuatrimestres:
            match_cuatrimestre["_cuatrimestre"] = {"$in": cuatrimestres}
        
        if match_cuatrimestre:
            pipeline.append({"$match": match_cuatrimestre})
        
        pipeline += [
            {'$project': {
                'form_id': 1,
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                '_cuatrimestre': 1,
                '_anio': 1,
                '_fecha': 1,
                '_dia': 1
            }},
            {
                '$group': {
                    '_id': {
                        'form_id': '$form_id',
                        'hotel': '$hotel',
                        'anio': '$_anio',
                        'cuatrimestre': '$_cuatrimestre',
                        'fecha': '$_fecha'
                    },
                    'total_inspecciones_dia': {'$sum': 1},
                    'dia_del_anio': {'$first': '$_dia'}
                }
            },
            {
                '$group': {
                    '_id': {
                        'form_id': '$_id.form_id',
                        'hotel': '$_id.hotel',
                        'anio': '$_id.anio',
                        'cuatrimestre': '$_id.cuatrimestre'
                    },
                    'dias_data': {
                        '$push': {
                            'fecha': '$_id.fecha',
                            'dia_del_anio': '$dia_del_anio',
                            'inspecciones': '$total_inspecciones_dia'
                        }
                    },
                    'total_inspecciones_cuatrimestre': {'$sum': '$total_inspecciones_dia'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'form_id': '$_id.form_id',
                    'hotel': '$_id.hotel',
                    'anio': '$_id.anio',
                    'cuatrimestre': '$_id.cuatrimestre',
                    'dias_data': 1,
                    'total_inspecciones_cuatrimestre': 1
                }
            },
            {
                '$sort': {
                    'hotel': 1,
                    'anio': 1,
                    'cuatrimestre': 1
                }
            }
        ]
        
        response = self.cr.aggregate(pipeline)
        response = list(response)
        
        # NUEVO: Crear diccionario de habitaciones por hotel para cálculo de porcentajes
        habitaciones_dict = {}
        for hab_info in habitaciones_x_hotel:
            hotel_name = hab_info.get('hotel', '')
            total_habitaciones = hab_info.get('total_habitaciones', 0)
            habitaciones_dict[hotel_name] = total_habitaciones
        
        form_id_to_hotel_name = {v: k.upper().replace('_', ' ') for k, v in self.form_ids.items()}
        
        # Agrupar por hotel
        hoteles_agrupados = {}
        for item in response:
            hotel = item['hotel']
            
            if not hotel:
                hotel = form_id_to_hotel_name.get(item['form_id'], f"Hotel {item['form_id']}")
            
            if hotel in self.hotel_name_abreviatura:
                hotel = self.hotel_name_abreviatura[hotel]
            
            if hotel not in hoteles_agrupados:
                hoteles_agrupados[hotel] = []
            
            # NUEVO: Procesar dias_data para agregar porcentajes progresivos
            total_habitaciones = habitaciones_dict.get(hotel, 0)
            dias_ordenados = sorted(item['dias_data'], key=lambda x: x['dia_del_anio'])
            
            # CALCULAR: Progreso acumulativo de porcentaje por día
            inspecciones_acumuladas = 0
            for dia_info in dias_ordenados:
                inspecciones_acumuladas += dia_info['inspecciones']
                
                # Calcular porcentaje progresivo hasta ese día
                if total_habitaciones > 0:
                    porcentaje_progresivo = min((inspecciones_acumuladas / total_habitaciones) * 100, 100.0)
                    porcentaje_progresivo = round(porcentaje_progresivo, 2)
                else:
                    porcentaje_progresivo = 0.0
                
                # AGREGAR: Datos de porcentaje al día
                dia_info['porcentaje_progresivo'] = porcentaje_progresivo
                dia_info['porcentaje_del_dia'] = round((dia_info['inspecciones'] / total_habitaciones) * 100, 2) if total_habitaciones > 0 else 0.0
            
            # Calcular porcentaje total del cuatrimestre
            total_inspecciones = item['total_inspecciones_cuatrimestre']
            porcentaje_total_cuatrimestre = 0.0
            
            if total_habitaciones > 0:
                porcentaje_total_cuatrimestre = min((total_inspecciones / total_habitaciones) * 100, 100.0)
                porcentaje_total_cuatrimestre = round(porcentaje_total_cuatrimestre, 2)
            
            hoteles_agrupados[hotel].append({
                'anio': item['anio'],
                'cuatrimestre': item['cuatrimestre'],
                'total_inspecciones_cuatrimestre': total_inspecciones,
                'total_habitaciones': total_habitaciones,
                'porcentaje_inspeccionado': porcentaje_total_cuatrimestre,
                'dias_data': dias_ordenados  # ACTUALIZADO: Con porcentajes progresivos
            })
        
        resultado_final = {
            'agrupado_por_hotel': [
                {
                    'hotel': hotel,
                    'cuatrimestres_data': data
                }
                for hotel, data in hoteles_agrupados.items()
            ],
        }
        return resultado_final

    def get_background_graphs(self, anio=None, cuatrimestres=None, hoteles=None):
        # Normaliza los nombres de hoteles usando las abreviaturas si corresponde
        if hoteles:
            hoteles_actualizados = []
            for hotel in hoteles:
                encontrado = False
                hotel_norm = hotel.lower().replace(' ', '_')
                for k, v in self.hotel_name_abreviatura.items():
                    v_norm = v.lower().replace(' ', '_')
                    k_norm = k.lower().replace(' ', '_')
                    if v_norm == hotel_norm:
                        hoteles_actualizados.append(k_norm)
                        encontrado = True
                        break
                if not encontrado:
                    hoteles_actualizados.append(hotel_norm)
            hoteles = hoteles_actualizados

        forms_id_list = self.get_forms_id_list(hoteles)
        # self.get_labels(forms_id_list=forms_id_list)

        inspecciones = self.get_inspecciones(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)
        calificacion_x_hotel_grafica = self.get_cuatrimestres_by_hotel(hoteles=hoteles, anio=anio, cuatrimestres=[1, 2, 3])
        graph_radar = self.get_graph_radar(inspecciones=inspecciones)

        report_data = {
            'calificacion_x_hotel_grafica': calificacion_x_hotel_grafica,
            'graph_radar': graph_radar,
        }
        return report_data

    def get_background_comments_and_images(self, anio=None, cuatrimestres=None, hoteles=None):
        # Normaliza los nombres de hoteles usando las abreviaturas si corresponde
        if hoteles:
            hoteles_actualizados = []
            for hotel in hoteles:
                encontrado = False
                hotel_norm = hotel.lower().replace(' ', '_')
                for k, v in self.hotel_name_abreviatura.items():
                    v_norm = v.lower().replace(' ', '_')
                    k_norm = k.lower().replace(' ', '_')
                    if v_norm == hotel_norm:
                        hoteles_actualizados.append(k_norm)
                        encontrado = True
                        break
                if not encontrado:
                    hoteles_actualizados.append(hotel_norm)
            hoteles = hoteles_actualizados

        forms_id_list = self.get_forms_id_list(hoteles)
        # self.get_labels(forms_id_list=forms_id_list)

        inspecciones = self.get_inspecciones(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)
        hoteles_fotografias = self.get_fotografias(inspecciones=inspecciones)
        rooms_details = self.get_rooms_details(inspecciones=inspecciones)
        hoteles_comentarios = rooms_details

        report_data = {
            'hoteles_fotografias': hoteles_fotografias,
            'hoteles_comentarios': {'hoteles_comentarios': hoteles_comentarios},
            'rooms_details': rooms_details,
        }
        return report_data

if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=False)
    module_obj.console_run()
    data = module_obj.data.get('data', {})
    option = data.get('option', 'get_report')
    anio = data.get('anio', None)
    cuatrimestres = data.get('cuatrimestres', None)
    hoteles = data.get('hoteles', [])
    hotel_name = data.get('hotel_name', 'CROWNE PLAZA MTY')
    room_id = data.get('room_id', 'Habitación 326')
    fallas = data.get('fallas', ['plafon_fuera_de_la_habitacion'])
    record_id = data.get('record_id', None)
    if option == 'get_hoteles':
        response = module_obj.get_hoteles()
    elif option == 'get_report':
        if not hoteles:
            hoteles = module_obj.get_hoteles()
            hoteles = hoteles.get('hoteles', [])
            hoteles = [hotel.get('nombre_hotel', '') for hotel in hoteles]
        response = module_obj.get_report(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)
    elif option == 'get_habitaciones_by_hotel':
        response = module_obj.get_habitaciones_by_hotel(hotel_name=hotel_name, fallas=fallas, anio=anio, cuatrimestres=cuatrimestres)
    elif option == 'get_room_data':
        response = module_obj.get_room_data(hotel_name=hotel_name, room_id=room_id)
    elif option == 'get_room_pdf':
        response = module_obj.get_pdf(record_id=record_id)
    elif option == 'get_multi_line_chart_inspecciones':
        response = module_obj.get_multi_line_chart_inspecciones(hoteles=hoteles)
    elif option == 'get_background_graphs':
        response = module_obj.get_background_graphs(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)
    elif option == 'get_background_comments_and_images':
        response = module_obj.get_background_comments_and_images(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)

    # print('response=', response)
    if data.get('test'):
        print('end')
    else:
        # print(simplejson.dumps(response, indent=3))
        module_obj.HttpResponse({"data": response})