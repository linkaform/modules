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
        
        self.f.update({
            'tipo_guardia': '68acee270f2af5e173b7f92e',
            'nombre_guardia_suplente': '68acb67685a044b5fdd869b2',
            'estatus_guardia': '663bffc28d00553254f274e0',
            'foto_inicio_turno': '6855e761adab5d93274da7d7',
            'foto_cierre_turno': '6879823d856f580aa0e05a3b',
            'fecha_inicio_turno': '6879828d0234f02649cad390',
            'fecha_cierre_turno': '6879828d0234f02649cad391',
            'comentario_inicio_turno': '66a5b9bed0c44910177eb724',
            'comentario_cierre_turno': '68798dd1205f333d8f53a1c7',
            'nombre_horario': '68b6427cc8f94827ebfed695',
            'hora_entrada': '68b6427cc8f94827ebfed696',
            'hora_salida': '68b6427cc8f94827ebfed697',
            'dias_de_la_semana': '68b861ba34290efdd49ab24f',
            'tolerancia_retardo': '68b6427cc8f94827ebfed698',
            'retardo_maximo': '68b642e2bc17e2713cabe019',
            'grupo_ubicaciones_horario': '68b6427cc8f94827ebfed699',
            'dias_libres_empleado': '68bb20095035e61c5745de05'
        })
        
        self.shifts = {}

    def get_employees_list(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.EMPLEADOS,
            }},
            {"$project": {
                "_id": 0,
                "employee_id": f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['id_usuario']}",
                "nombre_usuario": f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['nombre_usuario']}",
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = list({'nombre': i.get('nombre_usuario'), 'employee_id': self.unlist(i.get('employee_id', 0))} for i in response)
        return format_response

    def get_free_days(self, month, year):
        days_in_month = monthrange(year, month)[1]
        start_of_month = datetime(year, month, 1, 0, 0, 0)
        end_of_month = datetime(year, month, days_in_month, 23, 59, 59)
        
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.FORMATO_VACACIONES,
                "created_at": {
                    "$gte": start_of_month,
                    "$lte": end_of_month
                },
                f"answers.{self.f['free_day_autorization']}": "autorizado"
            }},
            {"$project": {
                "_id": 0,
                "fecha_inicio_dia_libre": f"$answers.{self.f['free_day_start']}",
                "fecha_fin_dia_libre": f"$answers.{self.f['free_day_end']}",
                "tipo_dia_libre": f"$answers.{self.f['free_day_type']}",
                "id_usuario": {"$arrayElemAt": [f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf['id_usuario']}", 0]}
            }},
            {"$group": {
                "_id": "$id_usuario",
                "dias_libres": {
                    "$push": {
                        "fecha_inicio": "$fecha_inicio_dia_libre",
                        "fecha_fin": "$fecha_fin_dia_libre",
                        "tipo": "$tipo_dia_libre"
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "id_usuario": "$_id",
                "dias_libres": 1
            }}
        ]
        
        response = self.format_cr(self.cr.aggregate(query))
        dias_por_usuario = {item['id_usuario']: item['dias_libres'] for item in response}
        return dias_por_usuario

    def get_employees_attendance(self, group_by="locations", locations=[], month=1, year=2026):
        employees_list = self.get_employees_list()
        employees_ids = list(i.get('employee_id', '') for i in employees_list)
        
        # Usar los parámetros month y year para crear el rango de fechas
        days_in_month = monthrange(year, month)[1]
        start_of_month = datetime(year, month, 1, 0, 0, 0)
        end_of_month = datetime(year, month, days_in_month, 23, 59, 59)
        
        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.REGISTRO_ASISTENCIA,
            "created_by_id": {"$in": employees_ids},
            "created_at": {
                "$gte": start_of_month,
                "$lte": end_of_month
            },
            f"answers.{self.f['start_shift']}": {"$exists": True},
        }
        
        if locations:
            match.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": locations}
            })

        query = [
            {"$match": match},
            {"$sort": {"created_at": -1}},
            {"$group": {
                "_id": {
                    "user_id": "$created_by_id",
                    "date": {"$substr": [f"$answers.{self.f['start_shift']}", 0, 10]}
                },
                "doc": {"$first": "$$ROOT"}
            }},
            {"$sort": {"doc.created_at": -1}},
            {"$group": {
                "_id": "$_id.user_id",
                "registros": {"$push": {
                    "answers": "$doc.answers",
                }}
            }},
            {"$project": {
                "_id": 0,
                "user_id": "$_id",
                "registros": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        response = {item["user_id"]: item["registros"] for item in response}
        format_response = {}
        free_days = self.get_free_days(month, year)
        if group_by == "employees":
            format_response = self.format_employees_attendance(response, employees_list, locations, month, year, free_days)
        elif group_by == "locations":
            format_response = self.format_locations_attendance(response, employees_list, month, year)
        return format_response
    
    def format_locations_attendance(self, data, employees_list, month, year):
        now = datetime.now(timezone('America/Mexico_City'))
        days_in_month = monthrange(year, month)[1]
        id_to_name = {emp['employee_id']: emp['nombre'] for emp in employees_list}

        # Estructura: {ubicacion: {turno_ref: {dia: [empleado-status, ...]}}}
        result = []

        # Recorre todos los registros de todos los empleados
        for emp_id, registros in data.items():
            for reg in registros:
                location = reg.get('incidente_location', 'Sin ubicación')
                nombre_horario = reg.get('nombre_horario', '')
                hora_entrada = reg.get('hora_entrada', '')
                hora_salida = reg.get('hora_salida', '')
                turno_ref = f"{nombre_horario} {hora_entrada}-{hora_salida}"

                # Inicializa estructura si no existe
                found = None
                for loc in result:
                    if loc["ubicacion"] == location and loc["turno_ref"] == turno_ref:
                        found = loc
                        break
                if not found:
                    found = {
                        "ubicacion": location,
                        "turno_ref": turno_ref,
                        "asistencia_mes": [{"dia": day, "empleados": []} for day in range(1, days_in_month + 1)],
                        "resumen": {"asistencias": 0, "retardos": 0, "faltas": 0}
                    }
                    result.append(found)

                # Días libres del empleado
                dias_libres = reg.get('dias_libres_empleado', [])

                # Status y día del registro
                fecha_inicio = reg.get('fecha_inicio_turno')
                status = reg.get('status_turn', '')
                nombre_empleado = id_to_name.get(emp_id, str(emp_id))
                if fecha_inicio:
                    dia = int(fecha_inicio[8:10])
                    # Si es día libre, status = "dia_libre"
                    dia_semana = datetime(year, month, dia).strftime("%A").lower()
                    dia_map = {
                        "monday": "lunes", "tuesday": "martes", "wednesday": "miercoles",
                        "thursday": "jueves", "friday": "viernes", "saturday": "sabado", "sunday": "domingo"
                    }
                    dia_es = dia_map.get(dia_semana, dia_semana)
                    if dias_libres and dia_es in dias_libres:
                        status = "dia_libre"
                    found["asistencia_mes"][dia - 1]["empleados"].append(f"{nombre_empleado}-{status}-{emp_id}")
                    # Contabilización
                    if status == "presente":
                        found["resumen"]["asistencias"] += 1
                    elif status == "retardo":
                        found["resumen"]["retardos"] += 1
                    elif status == "falta" or status == "falta_por_retardo":
                        found["resumen"]["faltas"] += 1

        # Completa los días sin registro para cada turno/ubicación
        for loc in result:
            for day_info in loc["asistencia_mes"]:
                if not day_info["empleados"]:
                    dia = day_info["dia"]
                    if dia < now.day:
                        day_info["empleados"] = ["sin_registro-sin_registro"]
                    else:
                        day_info["empleados"] = ["sin_registro-sin_registro"]

        return result
    
    def format_employees_attendance(self, data, employees_list, locations=[], month=1, year=2026, free_days={}):
        now = datetime.now(timezone('America/Mexico_City'))
        days_in_month = monthrange(year, month)[1]

        # Mapeo rápido de id a nombre
        id_to_name = {emp['employee_id']: emp['nombre'] for emp in employees_list}

        result = []

        for emp_id, registros in data.items():
            # Agrupar registros por ubicación
            ubicaciones = {}
            for reg in registros:
                location = reg.get('incidente_location', 'Sin ubicación')
                if location not in ubicaciones:
                    ubicaciones[location] = []
                ubicaciones[location].append(reg)

            for location, regs in ubicaciones.items():
                # Mapeo status por día
                dias_info = {}
                dias_libres = []
                for reg in regs:
                    fecha_inicio = reg.get('fecha_inicio_turno')
                    fecha_cierre = reg.get('end_shift')
                    status = reg.get('status_turn', '')
                    if fecha_inicio:
                        dia = int(fecha_inicio[8:10])
                        dias_info[dia] = {
                            "status": status,
                            "fecha_inicio": fecha_inicio,
                            "fecha_cierre": fecha_cierre
                        }
                        if reg.get('end_shift'):
                            dias_info[dia]["closed"] = True
                            
                    if reg.get('dias_libres_empleado'):
                        #! POSIBLE CAMBIO: Como considerariamos un cambio en los dias libres a mitad de mes?
                        dias_libres = reg['dias_libres_empleado']

                # Procesar días libres solicitados (vacaciones, permisos, etc.)
                dias_solicitados_info = {}
                if emp_id in free_days:
                    for solicitud in free_days[emp_id]:
                        fecha_inicio_str = solicitud['fecha_inicio']
                        fecha_fin_str = solicitud['fecha_fin']
                        tipo = solicitud['tipo']
                        
                        # Convertir strings a datetime
                        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
                        fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d')
                        
                        # Iterar sobre el rango de fechas
                        current_date = fecha_inicio
                        while current_date <= fecha_fin:
                            # Solo considerar días del mes actual
                            if current_date.year == year and current_date.month == month:
                                dia = current_date.day
                                dias_solicitados_info[dia] = tipo
                            current_date += timedelta(days=1)

                asistencia_mes = []
                resumen = {"asistencias": 0, "retardos": 0, "faltas": 0}
                for day in range(1, days_in_month + 1):
                    fecha_dia = datetime(year, month, day, tzinfo=timezone('America/Mexico_City'))
                    
                    # Verifica si es día libre
                    dia_semana = fecha_dia.strftime("%A").lower()
                    dia_map = {
                        "monday": "lunes", "tuesday": "martes", "wednesday": "miercoles",
                        "thursday": "jueves", "friday": "viernes", "saturday": "sabado", "sunday": "domingo"
                    }
                    dia_es = dia_map.get(dia_semana, dia_semana)
                    
                    fecha_inicio = None
                    fecha_cierre = None
                    closed = False
                    
                    # Primero verificar si hay un día libre solicitado
                    if day in dias_solicitados_info:
                        status = dias_solicitados_info[day]
                    elif dias_libres and dia_es in dias_libres:
                        status = "dia_libre"
                    elif day in dias_info:
                        info = dias_info[day]
                        status = info["status"]
                        fecha_inicio = info.get("fecha_inicio")
                        fecha_cierre = info.get("fecha_cierre")
                        closed = info.get("closed", False)
                    elif fecha_dia.date() < now.date():
                        status = "falta"
                    else:
                        status = "sin_registro"
                        
                    if status == "presente":
                        resumen["asistencias"] += 1
                    elif status == "retardo":
                        resumen["retardos"] += 1
                    elif status == "falta" or status == "falta_por_retardo":
                        resumen["faltas"] += 1
                        
                    asistencia_data = {
                        "dia": day,
                        "status": status,
                    }
                    if fecha_inicio:
                        asistencia_data["fecha_inicio"] = fecha_inicio
                    if fecha_cierre:
                        asistencia_data["fecha_cierre"] = fecha_cierre
                    if closed:
                        asistencia_data["closed"] = True

                    asistencia_mes.append(asistencia_data)

                result.append({
                    "employee_id": emp_id,
                    "nombre": id_to_name.get(emp_id, ""),
                    "ubicacion": location,
                    "asistencia_mes": asistencia_mes,
                    "resumen": resumen
                })
                
        empleados_con_registro = set(data.keys())
        for emp in employees_list:
            emp_id = emp['employee_id']
            if not emp_id or emp_id == 0:
                continue
            if emp_id not in empleados_con_registro and not locations:
                asistencia_mes = []
                resumen = {"asistencias": 0, "retardos": 0, "faltas": 0}
                for day in range(1, days_in_month + 1):
                    fecha_dia = datetime(year, month, day, tzinfo=timezone('America/Mexico_City'))
                    
                    if fecha_dia.date() < now.date():
                        status = "falta"
                        resumen["faltas"] += 1
                    else:
                        status = "sin_registro"
                    asistencia_mes.append({
                        "dia": day,
                        "status": status,
                    })
                result.append({
                    "employee_id": emp_id,
                    "nombre": emp['nombre'],
                    "ubicacion": "Sin ubicación",
                    "asistencia_mes": asistencia_mes,
                    "resumen": resumen
                })

        result = sorted(result, key=lambda x: x['nombre'])
        return result

    def get_guard_turn_details(self, user_ids=[], selected_day=None, location=None):
        now = datetime.now(timezone('America/Mexico_City'))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_of_month = now.replace(day=monthrange(now.year, now.month)[1], hour=23, minute=59, second=59, microsecond=999999)
        
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                "created_by_id": {"$in": user_ids},
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}": location,
                f"answers.{self.f['start_shift']}": {
                    "$gte": start_of_month.strftime("%Y-%m-%d %H:%M:%S"),
                    "$lte": end_of_month.strftime("%Y-%m-%d %H:%M:%S")
                },
            }},
            {"$sort": {
                f"answers.{self.f['start_shift']}": 1
            }},
            {"$project": {
                "_id": 0,
                "answers": 1,
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = self.format_guard_turn_details(response, selected_day=selected_day)
        return format_response

    def format_guard_turn_details(self, data, selected_day=None):
        now = datetime.now(timezone('America/Mexico_City'))
        today_str = now.strftime("%Y-%m-%d")
        asistencias = 0
        retardos = 0
        faltas = 0
        horas_trabajadas = 0.0
        days_in_month = monthrange(now.year, now.month)[1]
        dias_con_registro = {}
        today_item = None
        
        dias_libres = []
        #! POSIBLE CAMBIO: Se consideran solo los dias libres del primer registro del mes
        #! no sabemos si cambiaran a mitad del mes o en un momento random
        if data and data[0].get('dias_libres_empleado'):
            dias_libres = data[0]['dias_libres_empleado']
        
        for item in data:
            fecha_inicio = item.get('fecha_inicio_turno')
            status = item.get('status_turn', '')

            if fecha_inicio:
                dia = int(item['fecha_inicio_turno'][8:10])
                dias_con_registro[dia] = status
                if selected_day and dia == selected_day:
                    today_item = item
                    print(simplejson.dumps(today_item, indent=4))
            if item.get('status_turn') == 'presente':
                asistencias += 1
            if item.get('status_turn') == 'retardo':
                retardos += 1
            if item.get('status_turn') == 'falta_por_retardo':
                faltas += 1
            if item.get('horas_trabajadas'):
                horas_trabajadas += float(item.get('horas_trabajadas', 0.0))
        
        asistencia_mes = []
        for day in range(1, days_in_month + 1):
            # Verifica si es día libre
            dia_semana = datetime(now.year, now.month, day).strftime("%A").lower()
            dia_map = {
                "monday": "lunes", "tuesday": "martes", "wednesday": "miercoles",
                "thursday": "jueves", "friday": "viernes", "saturday": "sabado", "sunday": "domingo"
            }
            dia_es = dia_map.get(dia_semana, dia_semana)
            if dias_libres and dia_es in dias_libres:
                status = "dia_libre"
            elif day in dias_con_registro:
                status = dias_con_registro[day]
            elif day < now.day:
                status = "falta"
                faltas += 1
            else:
                status = "sin_registro"
            asistencia_mes.append({
                "dia": day,
                "status": status
            })
        
        format_response = {
            'guardia_generales': today_item if today_item else {},
            'asistencia_mes': asistencia_mes,
            'indicadores_generales': {
                'cantidad_asistencias': asistencias,
                'retardos': retardos,
                'horas_trabajadas': f"{round(horas_trabajadas)} / 168",
                'faltas': faltas
            },
        }
        return format_response

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
    date_range = data.get('date_range', 'mes')
    group_by = data.get('group_by', 'locations')
    locations = data.get('locations', [])
    option = data.get('option', 'get_guard_turn_details')
    turn_id = data.get('turn_id', '')
    names = data.get('names', [])
    user_ids = data.get('user_ids', [])
    selected_day = data.get('selected_day', 1)
    location = data.get('location', '')
    month = data.get('month', 1)
    year = data.get('year', 2026)

    response = {}
    if option == 'get_report':
        response = script_obj.get_employees_attendance(group_by=group_by, locations=locations, month=month, year=year)
    elif option == 'get_locations':
        response = script_obj.get_locations()
    elif option == 'get_guard_turn_details':
        response = script_obj.get_guard_turn_details(user_ids=user_ids, selected_day=selected_day, location=location)

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})

