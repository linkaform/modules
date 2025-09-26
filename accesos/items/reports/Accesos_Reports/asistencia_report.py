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
        
        self.default_shifts = {
            "T1": {
                "id": "T1",
                "name": "T1: 06:00 - 14:00 hrs",
                "entrada_hora": 6,
                "entrada_minuto": 0,
                "salida_hora": 14,
                "salida_minuto": 0,
                "timeRange": "06:00 - 14:00 hrs",
                "tolerancia": 15,
                "limite_retardo": 120,
                "minutos_entrada": 360,
                "minutos_salida": 840,
                "es_nocturno": False
            },
            "T2": {
                "id": "T2",
                "name": "T2: 14:00 - 22:00 hrs",
                "entrada_hora": 14,
                "entrada_minuto": 0,
                "salida_hora": 22,
                "salida_minuto": 0,
                "timeRange": "14:00 - 22:00 hrs",
                "tolerancia": 15,
                "limite_retardo": 120,
                "minutos_entrada": 840,
                "minutos_salida": 1320,
                "es_nocturno": False
            },
            "T3": {
                "id": "T3",
                "name": "T3: 22:00 - 06:00 hrs",
                "entrada_hora": 22,
                "entrada_minuto": 0,
                "salida_hora": 6,
                "salida_minuto": 0,
                "timeRange": "22:00 - 06:00 hrs",
                "tolerancia": 15,
                "limite_retardo": 120,
                "minutos_entrada": 1320,
                "minutos_salida": 360,
                "es_nocturno": True
            }
        }

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

    def get_employees_attendance(self, group_by="locations", locations=[]):
        employees_list = self.get_employees_list()
        employees_ids = list(i.get('employee_id', '') for i in employees_list)
        now = datetime.now(timezone('America/Mexico_City'))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.REGISTRO_ASISTENCIA,
            "user_id": {"$in": employees_ids},
            "created_at": {"$gte": start_of_month},
            f"answers.{self.f['start_shift']}": {"$exists": True},
            f"answers.{self.f['end_shift']}": {"$exists": True},
        }
        
        if locations:
            match.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": locations}
            })

        query = [
            {"$match": match},
            {"$group": {
                "_id": "$user_id",
                "registros": {"$push": {
                    "answers": "$answers",
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
        if group_by == "employees":
            format_response = self.format_employees_attendance(response, employees_list)
        elif group_by == "locations":
            format_response = self.format_locations_attendance(response, employees_list)
        return format_response
    
    def format_locations_attendance(self, data, employees_list):
        now = datetime.now(timezone('America/Mexico_City'))
        days_in_month = monthrange(now.year, now.month)[1]
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
                    dia_semana = datetime(now.year, now.month, dia).strftime("%A").lower()
                    dia_map = {
                        "monday": "lunes", "tuesday": "martes", "wednesday": "miercoles",
                        "thursday": "jueves", "friday": "viernes", "saturday": "sabado", "sunday": "domingo"
                    }
                    dia_es = dia_map.get(dia_semana, dia_semana)
                    if dias_libres and dia_es in dias_libres:
                        status = "dia_libre"
                    found["asistencia_mes"][dia - 1]["empleados"].append(f"{nombre_empleado}-{status}")
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
    
    def format_employees_attendance(self, data, employees_list):
        now = datetime.now(timezone('America/Mexico_City'))
        days_in_month = monthrange(now.year, now.month)[1]

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
                # Mapear status por día
                dias_con_registro = {}
                dias_libres = []
                for reg in regs:
                    fecha_inicio = reg.get('fecha_inicio_turno')
                    status = reg.get('status_turn', '')
                    if fecha_inicio:
                        dia = int(fecha_inicio[8:10])
                        dias_con_registro[dia] = status
                    if reg.get('dias_libres_empleado'):
                        #! POSIBLE CAMBIO: Como considerariamos un cambio en los dias libres a mitad de mes?
                        dias_libres = reg['dias_libres_empleado']

                asistencia_mes = []
                resumen = {"asistencias": 0, "retardos": 0, "faltas": 0}
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
                    else:
                        status = "sin_registro"
                        
                    if status == "presente":
                        resumen["asistencias"] += 1
                    elif status == "retardo":
                        resumen["retardos"] += 1
                    elif status == "falta" or status == "falta_por_retardo":
                        resumen["faltas"] += 1
                        
                    asistencia_mes.append({
                        "dia": day,
                        "status": status,
                    })

                result.append({
                    "employee_id": emp_id,
                    "nombre": id_to_name.get(emp_id, ""),
                    "ubicacion": location,
                    "asistencia_mes": asistencia_mes,
                    "resumen": resumen
                })
        return result
    
    def get_guard_turn_details(self):
        now = datetime.now(timezone('America/Mexico_City'))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_of_month = now.replace(day=monthrange(now.year, now.month)[1], hour=23, minute=59, second=59, microsecond=999999)
        
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                "user_id": self.user.get('user_id'),
                f"answers.{self.f['start_shift']}": {
                    "$gte": start_of_month.strftime("%Y-%m-%d %H:%M:%S"),
                    "$lte": end_of_month.strftime("%Y-%m-%d %H:%M:%S")
                }
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
        format_response = self.format_guard_turn_details(response)
        return format_response

    def format_guard_turn_details(self, data):
        now = datetime.now(timezone('America/Mexico_City'))
        today_str = now.strftime("%Y-%m-%d")
        porcentaje_asistencias = 0.0
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
                if fecha_inicio.startswith(today_str):
                    today_item = item
            if item.get('status_turn') == 'presente':
                porcentaje_asistencias += 1
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
                'porcentaje_asistencias': round((porcentaje_asistencias / days_in_month) * 100, 2),
                'retardos': retardos,
                'horas_trabajadas': f"{round(horas_trabajadas, 2)} / 168",
                'faltas': faltas
            },
        }
        return format_response

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

    response = {}
    if option == 'get_report':
        response = script_obj.get_employees_attendance(group_by=group_by, locations=locations)
    elif option == 'get_locations':
        response = script_obj.get_locations()
    elif option == 'get_guard_turn_details':
        response = script_obj.get_guard_turn_details()

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})

