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

    @property
    def my_hotels(self):
        if not self.my_hotels_ids:
            user_id = self.user.get('user_id')
            forms = self.lkf_api.get_user_forms(user_id)
            all_forms = forms.get('data')
            forms = [f['name'] for f in all_forms if f.get('name')]
            my_hotels = []
            my_hotels = [self.form_name_hotel_name[f] for f in forms if self.form_name_hotel_name.get(f)]
            self.my_hotels_ids = my_hotels
        return self.my_hotels_ids
    
    def get_hotel_names(self):
        my_hotels_set = set(self.my_hotels)
        hotel_names_dict = {
            name.upper().replace('_', ' '): form_id
            for name, form_id in self.form_ids.items()
            if form_id in my_hotels_set
        }
        hotel_names_list = list(hotel_names_dict.keys())
        hotel_ids_list = list(hotel_names_dict.values())
        return hotel_names_dict, hotel_names_list, hotel_ids_list

    def get_cantidad_habitaciones(self, ubicaciones_list=[]):
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
                '$group': {
                    '_id': f"$answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}",
                    'totalHabitaciones': {'$sum': 1},
                    'ciudad': {'$first': f"$answers.{self.CONTACTO_CAT_OBJ_ID}.{self.f['state']}"}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'hotel': "$_id",
                    'totalHabitaciones': 1,
                    'ciudad': 1
                }
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))
        return result
    
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
                'form_id': 1,
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                'habitacion_remodelada': {"$ifNull": [f"$answers.{self.f['habitacion_remodelada']}", "No"]},
                'status_auditoria': f"$answers.{self.f['status_auditoria']}",
                'created_at': '$created_at',
                '_fecha': '$_fecha',
                'created_by_name': '$created_by_name',
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
                    '_id': '$form_id',
                    'inspecciones': {'$push': '$$ROOT'},
                    'total': {'$sum': 1}
                }
            }
        ]
        result = self.cr.aggregate(query)
        return list(result)

    def get_hoteles(self):
        hotel_names_list = self.get_hotel_names()
        return hotel_names_list
    
    def format_hoteles_information(self, inspecciones_agrupadas, hotel_names_dict, inventario_dict):
        hoteles_resultado = []
        # Mapeo invertido de form_id a Nombre de Hotel para el resultado
        id_to_name = {v: k for k, v in hotel_names_dict.items()}

        for grupo in inspecciones_agrupadas:
            form_id = grupo.get('_id')
            inspecciones_lista = grupo.get('inspecciones', [])
            nombre_hotel = id_to_name.get(form_id, f"Hotel {form_id}")
            if nombre_hotel in self.hotel_name_abreviatura:
                nombre_hotel = self.hotel_name_abreviatura[nombre_hotel]
            
            # Obtener datos del inventario (total y ciudad)
            datos_inventario = inventario_dict.get(nombre_hotel, {'total': 0, 'ciudad': 'N/A'})
            
            total_fallas = 0
            total_fallas_resueltas = 0
            total_inversion = 0
            ultima_fecha = None
            gerente = "N/A"
            habitaciones_distintas = set()
            inversion_x_hab = {}

            for insp in inspecciones_lista:
                # El primer created_by_name que encontremos será nuestro "gerente"
                if gerente == "N/A" and insp.get('created_by_name'):
                    gerente = insp.get('created_by_name')

                # Registrar habitación para contar únicas
                num_habitacion = insp.get('habitacion')
                if num_habitacion:
                    habitaciones_distintas.add(num_habitacion)

                # Sumar fallas del objeto de inspección
                resumen_insp = insp.get('inspeccion', {})
                total_fallas += resumen_insp.get('fallas', 0)
                total_fallas_resueltas += len(resumen_insp.get('field_label_acciones_correctivas', {}))
                total_inversion += resumen_insp.get('inversion_acciones_correctivas', 0)

                # Obtener la fecha más reciente
                fecha_str = insp.get('created_at')
                if fecha_str:
                    if not ultima_fecha or fecha_str > ultima_fecha:
                        ultima_fecha = fecha_str

                if num_habitacion:
                    entry = inversion_x_hab.setdefault(num_habitacion, {'inversion': 0, 'fallasResueltas': 0})
                    entry['inversion'] += resumen_insp.get('inversion_acciones_correctivas', 0)
                    entry['fallasResueltas'] += len(resumen_insp.get('field_label_acciones_correctivas', {}))

            inversion_por_habitacion = sorted(
                [{'habitacion': hab, **datos} for hab, datos in inversion_x_hab.items()],
                key=lambda x: x['inversion'], reverse=True
            )[:8]

            hotel_data = {
                "id": form_id,
                "nombre": nombre_hotel,
                "ciudad": datos_inventario.get('ciudad'),
                "totalHabitaciones": datos_inventario.get('total'),
                "habitacionesInspeccionadas": len(habitaciones_distintas),
                "totalInspecciones": grupo.get('total', 0),
                "fallasReportadas": total_fallas,
                "fallasResueltas": total_fallas_resueltas,
                "totalInversion": total_inversion,
                "gerente": gerente,
                "ultimaInspeccion": ultima_fecha.strftime('%Y-%m-%d') if isinstance(ultima_fecha, datetime.datetime) else (ultima_fecha.split(' ')[0] if ultima_fecha else "N/A"),
                "inversionPorHabitacion": inversion_por_habitacion,
            }
            hoteles_resultado.append(hotel_data)
        return hoteles_resultado

    def format_hoteles_sections(self, inspecciones_agrupadas, hotel_names_dict):
        """
        Extrae los nombres únicos de las secciones de todas las inspecciones proporcionadas.
        """
        secciones_unicas = set()
        for grupo in inspecciones_agrupadas:
            inspecciones_lista = grupo.get('inspecciones', [])
            for insp in inspecciones_lista:
                # El objeto 'inspeccion' contiene el diccionario 'sections'
                resumen_insp = insp.get('inspeccion', {})
                sections_dict = resumen_insp.get('sections', {})
                if isinstance(sections_dict, dict):
                    # Agregamos las llaves (nombres de secciones) al set
                    secciones_unicas.update(sections_dict.keys())
        
        # Retornamos como lista ordenada alfabéticamente para consistencia
        return sorted(list(secciones_unicas))

    def format_fallas_x_section(self, inspecciones_agrupadas, hotel_names_dict, hoteles_sections):
        """
        Calcula el total de fallas por cada sección sumando los datos de todas las inspecciones.
        """
        fallas_por_seccion = {seccion: 0 for seccion in hoteles_sections}

        for grupo in inspecciones_agrupadas:
            inspecciones_lista = grupo.get('inspecciones', [])
            for insp in inspecciones_lista:
                resumen_insp = insp.get('inspeccion', {})
                sections_dict = resumen_insp.get('sections', {})
                
                if isinstance(sections_dict, dict):
                    for nombre_seccion, datos_seccion in sections_dict.items():
                        if nombre_seccion in fallas_por_seccion:
                            fallas_por_seccion[nombre_seccion] += datos_seccion.get('fallas', 0)

        # Convertir el diccionario a la lista de objetos requerida
        resultado = [
            {
                "seccion": seccion,
                "total": total,
                "pendientes": 0,    # Dato no disponible aún
                "enProceso": 0,    # Dato no disponible aún
                "realizadas": 0,    # Dato no disponible aún
                "capex": 0          # Dato no disponible aún
            }
            for seccion, total in fallas_por_seccion.items()
        ]

        # Opcional: Ordenar por total de fallas de mayor a menor
        resultado = sorted(resultado, key=lambda x: x['total'], reverse=True)
        
        return resultado
    
    def format_inspecciones_x_dia(self, inspecciones_agrupadas):
        """
        Cuenta la cantidad de inspecciones realizadas por día.
        Retorna los últimos 12 días encontrados.
        """
        conteo_dias = {}

        for grupo in inspecciones_agrupadas:
            inspecciones_lista = grupo.get('inspecciones', [])
            for insp in inspecciones_lista:
                # El campo '_fecha' ya viene calculado en formato 'YYYY-MM-DD' desde la agregación
                fecha = insp.get('_fecha')
                if fecha:
                    conteo_dias[fecha] = conteo_dias.get(fecha, 0) + 1
        
        # Convertir a lista de diccionarios
        resultado_completo = [
            {"fecha": fecha, "cantidad": cantidad}
            for fecha, cantidad in conteo_dias.items()
        ]

        # Ordenar cronológicamente y tomar los últimos 12 días
        resultado_ordenado = sorted(resultado_completo, key=lambda x: x['fecha'])
        return resultado_ordenado[-12:]

    def format_hoteles_performance(self, inspecciones_agrupadas, hotel_names_dict):
        """
        Calcula el rendimiento de inspecciones por gerente/hotel:
        - hoy: inspecciones en la fecha actual (UTC/Local)
        - semana: inspecciones en los últimos 7 días
        - mes: inspecciones en el mes actual
        """
        performance_resultado = []
        id_to_name = {v: k for k, v in hotel_names_dict.items()}
        
        # Obtenemos fechas de referencia (asumiendo que operamos en el contexto de las inspecciones)
        hoy_dt = datetime.datetime.now()
        hoy_str = hoy_dt.strftime('%Y-%m-%d')
        hace_una_semana = hoy_dt - datetime.timedelta(days=7)
        
        for grupo in inspecciones_agrupadas:
            form_id = grupo.get('_id')
            inspecciones_lista = grupo.get('inspecciones', [])
            nombre_hotel = id_to_name.get(form_id, f"Hotel {form_id}")
            if nombre_hotel in self.hotel_name_abreviatura:
                nombre_hotel = self.hotel_name_abreviatura[nombre_hotel]

            gerente = "N/A"
            conteos = {"hoy": 0, "semana": 0, "mes": 0}

            for insp in inspecciones_lista:
                # Gerente
                if gerente == "N/A" and insp.get('created_by_name'):
                    gerente = insp.get('created_by_name')

                # Tiempos (usando created_at que es objeto datetime)
                fecha_insp = insp.get('created_at')
                if isinstance(fecha_insp, datetime.datetime):
                    # Hoy
                    if fecha_insp.strftime('%Y-%m-%d') == hoy_str:
                        conteos["hoy"] += 1
                    
                    # Últimos 7 días (Semana)
                    if fecha_insp >= hace_una_semana:
                        conteos["semana"] += 1
                    
                    # Mes actual
                    if fecha_insp.year == hoy_dt.year and fecha_insp.month == hoy_dt.month:
                        conteos["mes"] += 1
                
            performance_resultado.append({
                "gerente": gerente,
                "hotel": nombre_hotel,
                "hoy": conteos["hoy"],
                "semana": conteos["semana"],
                "mes": conteos["mes"]
            })

        return performance_resultado

    def format_ultimas_fallas(self, inspecciones_agrupadas, hotel_names_dict):
        """
        Extrae las últimas 5 fallas registradas en todas las inspecciones.
        """
        id_to_name = {v: k for k, v in hotel_names_dict.items()}
        todas_las_fallas = []

        for grupo in inspecciones_agrupadas:
            form_id = grupo.get('_id')
            inspecciones_lista = grupo.get('inspecciones', [])
            
            hotel_nombre = id_to_name.get(form_id, f"Hotel {form_id}")
            if hotel_nombre in self.hotel_name_abreviatura:
                hotel_nombre = self.hotel_name_abreviatura[hotel_nombre]

            for insp in inspecciones_lista:
                resumen_insp = insp.get('inspeccion', {})
                # Los comentarios suelen indicar el tipo de falla por fieldId
                comments = resumen_insp.get('comments', {})
                # media contiene las fotos por fieldId
                media = resumen_insp.get('media', {})
                # field_label contiene el nombre legible del campo (ej. "Regadera")
                field_labels = resumen_insp.get('field_label', {})

                # Iteramos sobre las secciones para saber a qué sección pertenece cada falla
                sections_dict = resumen_insp.get('sections', {})
                
                # Para simplificar, si hay comentarios, asumimos que son fallas reportadas
                if comments:
                    for field_id, comentario in comments.items():
                        # Intentamos encontrar en qué sección está este field_id
                        seccion_nombre = "GENERAL"
                        # Esto es una búsqueda simplificada
                        for s_name, s_data in sections_dict.items():
                            # Nota: En tu estructura JSON de ejemplo no se ve la relación directa 
                            # field_id -> sección, pero tomamos la primera que tenga fallas > 0
                            # o simplemente asignamos "GENERAL" si no se puede determinar.
                            if s_data.get('fallas', 0) > 0:
                                seccion_nombre = s_name
                                break

                        falla_data = {
                            "id": f"{insp.get('_id')}_{field_id}",
                            "hotelId": form_id,
                            "hotelNombre": hotel_nombre,
                            "habitacion": insp.get('habitacion', 'N/A'),
                            "gerente": insp.get('created_by_name', 'N/A'),
                            "fecha": insp.get('_fecha', 'N/A'),
                            "seccion": seccion_nombre,
                            "estado": "falla",
                            "tipoFalla": f"{field_labels.get(field_id, 'Falla detectada')}: {comentario}",
                            "departamento": "N/A",
                            "estatusFalla": "N/A",
                            "foto": field_id in media,
                            # Guardamos la fecha original para ordenar
                            "_timestamp": insp.get('created_at')
                        }
                        todas_las_fallas.append(falla_data)

        # Ordenar por el timestamp de la inspección de forma descendente
        todas_las_fallas = sorted(todas_las_fallas, key=lambda x: str(x['_timestamp']), reverse=True)
        
        # Quitamos el campo auxiliar y tomamos solo 5
        for f in todas_las_fallas: f.pop('_timestamp', None)
        
        return todas_las_fallas[:5]

    def format_fallas_x_departamento(self, inspecciones_agrupadas):
        totales = {'pendientes': 0, 'enProceso': 0, 'realizadas': 0, 'capex': 0}

        for grupo in inspecciones_agrupadas:
            for insp in grupo.get('inspecciones', []):
                resumen_insp = insp.get('inspeccion', {})
                totales['pendientes']  += len(resumen_insp.get('field_label', {}))
                totales['realizadas']  += len(resumen_insp.get('field_label_acciones_correctivas', {}))
                totales['capex']       += len(resumen_insp.get('inversion_x_acciones_correctivas', {}))

        return [{'departamento': 'Mantenimiento', **totales}]

    def get_report(self, selected_hoteles=[], cuatrimestres=None, anio=None):
        hotel_names_dict, hotel_names_list, hotel_ids_list = self.get_hotel_names()
        
        # Filtrar por hoteles seleccionados si se proporcionan IDs
        if selected_hoteles:
            # Aseguramos que los IDs sean enteros si vienen como strings
            selected_ids = [int(hid) for hid in selected_hoteles if str(hid).isdigit()]
            hotel_names_dict = {k: v for k, v in hotel_names_dict.items() if v in selected_ids}
            hotel_names_list = list(hotel_names_dict.keys())
            hotel_ids_list = list(hotel_names_dict.values())

        # Obtener inventario de habitaciones por hotel
        cantidad_habitaciones_x_hotel = self.get_cantidad_habitaciones(ubicaciones_list=hotel_names_list)
        # Convertir a diccionario para búsqueda rápida { 'NOMBRE HOTEL': { 'total': X, 'ciudad': Y } }
        inventario_dict = {
            h['hotel']: {
                'total': h['totalHabitaciones'],
                'ciudad': self.unlist(h.get('ciudad', 'N/A'))
            } for h in cantidad_habitaciones_x_hotel
        }

        # Obtener inspecciones agrupadas por form_id
        # TODO: Ajustar anio y cuatrimestres dinámicamente si es necesario
        inspecciones_agrupadas = self.get_inspecciones(forms_id_list=hotel_ids_list, anio=anio, cuatrimestres=cuatrimestres)

        #FINAL DATA
        hoteles_resultado = self.format_hoteles_information(inspecciones_agrupadas, hotel_names_dict, inventario_dict)
        hoteles_sections = self.format_hoteles_sections(inspecciones_agrupadas, hotel_names_dict)
        hoteles_fallas_x_section = self.format_fallas_x_section(inspecciones_agrupadas, hotel_names_dict, hoteles_sections)
        hoteles_inspecciones_x_dia = self.format_inspecciones_x_dia(inspecciones_agrupadas)
        hoteles_performance = self.format_hoteles_performance(inspecciones_agrupadas, hotel_names_dict)
        hoteles_ultimas_fallas = self.format_ultimas_fallas(inspecciones_agrupadas, hotel_names_dict)
        fallas_x_departamento = self.format_fallas_x_departamento(inspecciones_agrupadas)

        response = {
            "hoteles": hoteles_resultado,
            "secciones": hoteles_sections,
            "fallasPorSeccion": hoteles_fallas_x_section,
            "inspeccionesDiarias": hoteles_inspecciones_x_dia,
            "inspeccionesPorGerente": hoteles_performance,
            "fallasRecientes": hoteles_ultimas_fallas,
            "fallasPorDepartamento": fallas_x_departamento,
        }
        return response
        
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
    selected_hoteles = data.get('selected_hoteles', [])
    record_id = data.get('record_id', None)

    if option == 'get_hoteles':
        response = module_obj.get_hoteles()
    elif option == 'get_report':
        response = module_obj.get_report(selected_hoteles, cuatrimestres, anio)
    elif option == 'test':
        response = module_obj.cr_inspeccion.aggregate([{'$match': {'field_label_acciones_correctivas': {'$exists': True}, "form_id": 132791}}])
        response = list(response)
    else:
        response = {'error': 'Opción no válida'}

    print(simplejson.dumps(response, indent=4, default=str))
    module_obj.HttpResponse({"data": response})