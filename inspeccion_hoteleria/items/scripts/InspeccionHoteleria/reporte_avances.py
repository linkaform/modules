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
            user_id = self.user.get('user_id')
            forms = self.lkf_api.get_user_forms(user_id)
            all_forms = forms.get('data')
            user_form_ids = [f['id'] for f in all_forms if f.get('id')]
            
            hotel_ids_set = set(self.form_ids.values())
            my_hotels = [form_id for form_id in user_form_ids if form_id in hotel_ids_set]
            
            self.my_hotels_ids = my_hotels
        return self.my_hotels_ids


    def get_hoteles(self):
        my_hotels_set = set(self.my_hotels)
        filtered_form_ids = [
            {'nombre_hotel': name.upper().replace('_', ' ')}
            for name, form_id in self.form_ids.items()
            if form_id in my_hotels_set
        ]
        hoteles = {
            'hoteles': filtered_form_ids
        }
        return hoteles
    
    def get_cantidad_habitaciones(self, ubicaciones_list=[]):
        if isinstance(ubicaciones_list, list):
            ubicaciones_list = [
                self.hotel_name_abreviatura.get(
                    str(u).replace('_', ' ').upper(), 
                    str(u).replace('_', ' ').upper()
                )
                for u in ubicaciones_list
            ]

        match_query = {
            "deleted_at": {"$exists": False},
            f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area']}": "Habitación",
        }

        if len(ubicaciones_list) > 1:
            match_query.update({
               f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": ubicaciones_list}
            })
        elif len(ubicaciones_list) == 1:
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

    def get_cantidad_inspecciones_y_remodeladas(self, forms_id_list=[], anio=None, cuatrimestres=None):
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
    
        # Pipeline para agregar año y cuatrimestre
        pipeline = [
            {'$match': match_query},
            {'$addFields': {
                "_anio": {"$year": "$created_at"},
                "_mes": {"$month": "$created_at"}
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
            pipeline.append({"$match": match_cuatrimestre})
    
        pipeline += [
            {'$project': {
                'habitacion_id': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
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
                    '_id': {
                        'habitacion': '$habitacion_id',
                        'hotel': '$hotel'
                    },
                    'total_inspecciones_habitacion': {'$sum': 1},
                    'es_remodelada': {'$max': '$habitacion_remodelada_si'}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_inspecciones_completadas': { '$sum': '$total_inspecciones_habitacion' },
                    'habitaciones_inspeccionadas_unicas': { '$sum': 1 },  # NUEVA
                    'total_habitaciones_remodeladas': { '$sum': '$es_remodelada' }  # CORREGIDA
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'total_inspecciones_completadas': 1,
                    'habitaciones_inspeccionadas_unicas': 1,
                    'total_habitaciones_remodeladas': 1
                }
            }
        ]
    
        result = self.format_cr(self.cr.aggregate(pipeline))
        result = self.unlist(result)
        return result
    
    def porcentaje_propiedades_inspeccionadas(self, total_habitaciones, total_inspecciones):
        if total_habitaciones == 0:
            return 0.0
    
        porcentaje = (total_inspecciones / total_habitaciones) * 100
        return round(porcentaje, 2)

    def get_forms_id_list(self, hoteles=None):
        print("hoteles", hoteles)
        
        if not hoteles:
            my_hotels_set = set(self.my_hotels)
            filtered_ids = [fid for fid in self.form_ids.values() if fid in my_hotels_set]

            return filtered_ids

        hoteles_normalizados = [
            hotel.lower().replace(' ', '_') for hotel in hoteles
        ]

        ids = [
            self.form_ids[nombre]
            for nombre in hoteles_normalizados
            if nombre in self.form_ids
        ]

        return ids

    def get_cards(self, forms_id_list=[], anio=None, cuatrimestres=None):
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
    
        # Pipeline para agregar año y cuatrimestre
        query = [
            {'$match': match_query},
            {'$addFields': {
                "_anio": {"$year": "$created_at"},
                "_mes": {"$month": "$created_at"}
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
                'habitacion_remodelada': f"$answers.{self.f['habitacion_remodelada']}",
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
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
            '$group': {
                '_id': '$_id',
                'habitacion': {'$first': '$habitacion'},
                'remodelada': {
                '$max': {
                    '$cond': [
                    {'$eq': ['$habitacion_remodelada', 'sí']},
                    1,
                    0
                    ]
                }
                },
                'fallas': {'$max': '$inspeccion.fallas'},
                'aciertos': {'$max': '$inspeccion.aciertos'},
                'grade_max': {'$max': '$inspeccion.grade'},
                'grade_min': {'$min': '$inspeccion.grade'},
                'grade_avg': {'$avg': '$inspeccion.grade'}
            }
            },
            {
            '$group': {
                '_id': None,
                'habitaciones_remodeladas': {'$sum': '$remodelada'},
                'inspecciones_realizadas': {'$sum': 1},
                'total_fallas': {'$sum': '$fallas'},
                'total_aciertos': {'$sum': '$aciertos'},
                'grade_max': {'$max': '$grade_max'},
                'grade_min': {'$min': '$grade_min'},
                'grade_avg': {'$avg': '$grade_avg'}
            }
            },
            {
            '$project': {
                '_id': 0,
                'habitaciones_remodeladas': 1,
                'inspecciones_realizadas': 1,
                'total_fallas': 1,
                'total_aciertos': 1,
                'grade_max': 1,
                'grade_min': 1,
                'grade_avg': {'$round': ['$grade_avg', 2]}
            }
            }
        ]
        result = self.cr.aggregate(query)
        result = list(result)
        result = self.unlist(result)
        
        return result
    
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
        # self.get_labels()

        total_habitaciones = self.get_cantidad_habitaciones(ubicaciones_list=hoteles)

        total_inspecciones_y_remodeladas = self.get_cantidad_inspecciones_y_remodeladas(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)

        cards = self.get_cards(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)
        
        propiedades_inspeccionadas = self.porcentaje_propiedades_inspeccionadas(
           total_habitaciones.get('totalHabitaciones', 0) if total_habitaciones else 0,
           total_inspecciones_y_remodeladas.get('total_inspecciones_completadas', 0) if total_inspecciones_y_remodeladas else 0
        )

        report_data = {
            'cards': cards,
            'total_inspecciones_y_remodeladas': total_inspecciones_y_remodeladas,
            'porcentaje_inspecciones': propiedades_inspeccionadas,
        }

        return report_data

    def get_labels(self):
        form_data = self.lkf_api.get_form_for_answer(self.REVISON_HABITACION)
        fields = form_data[0].get('fields', [])
        for field in fields:
            if not field.get('catalog'):
                if not field.get('label') in self.labels_to_exclude and not field.get('field_id') in self.ids_to_exclude:
                    self.fallas_dict.update({
                        self.normaliza_texto(field.get('label')): field.get('field_id')
                    })
                        
if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=False)
    module_obj.console_run()
    data = module_obj.data.get('data', {})
    option = data.get('option', 'get_report')
    anio = data.get('anio', None)
    cuatrimestres = data.get('cuatrimestres', None)
    hoteles = data.get('hoteles', [])

    if option == 'get_hoteles':
        response = module_obj.get_hoteles()
    elif option == 'get_report':
        response = module_obj.get_report(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)

    module_obj.HttpResponse({"data": response})