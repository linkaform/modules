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
            'grupo_ubicaciones_horario': '68b6427cc8f94827ebfed699'
        })
        
        self.shifts = {}
        
        self.default_shifts = {
            "T1": {
                "id": "T1",
                "name": "T1: 06:00 - 14:00 hrs",
                "timeRange": "06:00 - 14:00 hrs"
            },
            "T2": {
                "id": "T2",
                "name": "T2: 14:00 - 22:00 hrs",
                "timeRange": "14:00 - 22:00 hrs"
            },
            "T3": {
                "id": "T3",
                "name": "T3: 22:00 - 06:00 hrs",
                "timeRange": "22:00 - 06:00 hrs"
            }
        }
        
    #! =========================================== Utils functions
    # Modificar la función get_shift para que reciba también la ubicación
    def get_shift(self, hour, location=None):
        """
        Determina el turno correspondiente a una hora y ubicación.
        Si la ubicación tiene turnos específicos, los usa.
        De lo contrario, usa los turnos predeterminados.
        """
        # Si no hay turnos personalizados o no se proporciona ubicación, usar los predeterminados
        if not self.shifts or not location:
            if 6 <= hour < 14:
                return self.default_shifts["T1"]
            elif 14 <= hour < 22:
                return self.default_shifts["T2"]
            else:  # 22-24 or 0-6
                return self.default_shifts["T3"]
        
        # Buscar turnos específicos para esta ubicación
        location_shifts = []
        for shift_id, shift_info in self.shifts.items():
            # Verificar si esta ubicación está en las ubicaciones del turno
            if location in shift_info.get("locations", []):
                # Extraer las horas de entrada y salida
                try:
                    entrada = shift_info.get("timeRange", "").split(" - ")[0]
                    salida = shift_info.get("timeRange", "").split(" - ")[1].replace(" hrs", "")
                    
                    # Convertir a horas enteras para comparación simple
                    hora_entrada = int(entrada.split(":")[0])
                    hora_salida = int(salida.split(":")[0])
                    
                    # Agregar el turno a los turnos de esta ubicación
                    location_shifts.append({
                        "shift": shift_info,
                        "hora_entrada": hora_entrada,
                        "hora_salida": hora_salida
                    })
                except (IndexError, ValueError):
                    continue
        
        # Si no hay turnos específicos para esta ubicación, usar los predeterminados
        if not location_shifts:
            if 6 <= hour < 14:
                return self.default_shifts["T1"]
            elif 14 <= hour < 22:
                return self.default_shifts["T2"]
            else:  # 22-24 or 0-6
                return self.default_shifts["T3"]
        
        # Buscar el turno que corresponda a esta hora
        for shift in location_shifts:
            entrada = shift["hora_entrada"]
            salida = shift["hora_salida"]
            
            # Caso normal (entrada < salida), por ejemplo 8:00 - 16:00
            if entrada < salida:
                if entrada <= hour < salida:
                    return shift["shift"]
            # Caso especial (entrada > salida), por ejemplo 22:00 - 6:00 (turno nocturno)
            else:
                if hour >= entrada or hour < salida:
                    return shift["shift"]
        
        # Si ningún turno específico coincide, usar los predeterminados
        if 6 <= hour < 14:
            return self.default_shifts["T1"]
        elif 14 <= hour < 22:
            return self.default_shifts["T2"]
        else:  # 22-24 or 0-6
            return self.default_shifts["T3"]
    #! ===========================================
    
    def get_guard_shifts(self):
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": 140067, #TODO: Moduralizar id 
            }},
            {"$project": {
                "_id": 1,
                "answers": 1
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        for record in response:
            self.shifts[record.get('nombre_horario', '')] = { #type: ignore
                "id": record.get('nombre_horario', ''),
                "name": f"{record.get('nombre_horario', '')}: {record.get('hora_entrada', '')[:5]} - {record.get('hora_salida', '')[:5]} hrs",
                "timeRange": f"{record.get('hora_entrada', '')[:5]} - {record.get('hora_salida', '')[:5]} hrs",
                "locations": [r.get('incidente_location', '') for r in record.get('grupo_ubicaciones_horario', [])],
                "days": record.get('dias_de_la_semana', []),
                "tolerancia": record.get('tolerancia_retardo', ''),
                "limite_retardo": record.get('retardo_maximo', '')
            }

    def get_employees_attendance(self, date_range="mes", group_by="employees", locations=[]):
        match = {
            "deleted_at": {"$exists": False},
            "form_id": 135386,
        }

        if locations:
            match.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": locations}
            })
        if date_range == 'mes':
            now = datetime.now(timezone('America/Mexico_City'))
            start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            match.update({
                "created_at": {"$gte": start_of_month}
            })

        query = [
            {"$match": match},
            {"$project": {
                "_id": 0,
                "created_at": 1,
                "user_name": 1,
                "answers": 1
            }},
        ]
        response = self.format_cr(self.cr.aggregate(query))
        print(simplejson.dumps(response, indent=4))
        format_response = {}
        if group_by == "employees":
            format_response = self.format_employees_attendance(data=response, date_range=date_range)
        elif group_by == "locations":
            format_response = self.format_locations_attendance(data=response, date_range=date_range)
        return format_response

    def format_employees_attendance(self, data, date_range="mes"):
        now = datetime.now(timezone('America/Mexico_City'))
        year = now.year
        month = now.month
        day = now.day
        days_in_month = monthrange(year, month)[1]
        
        employees_data = {}
        for record in data:
            user_name = record.get('user_name', '')
            if not user_name:
                continue
                
            if user_name not in employees_data:
                employees_data[user_name] = {
                    'records': [],
                    'attendance_dates': set()
                }
                
            employees_data[user_name]['records'].append(record)
            
            fecha_str = record.get('fecha_cierre_turno') or record.get('fecha_inicio_turno')
            if fecha_str:
                try:
                    fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                    attendance_date = fecha_dt.strftime('%Y-%m-%d')
                    employees_data[user_name]['attendance_dates'].add(attendance_date)
                except (ValueError, TypeError):
                    pass
        
        formatted_employees = []
        for user_name, emp_data in employees_data.items():
            # Si no hay fechas de asistencia completa, omitir este empleado
            if not emp_data['attendance_dates']:
                continue
            
            employee = {
                "id": user_name.replace(' ', '_').lower(),
                "name": user_name,
                "type": "employee",
                "attendance": {},
                "summary": {
                    "totalPresent": 0,
                    "totalLate": 0,
                    "totalAbsent": 0
                }
            }
            
            for day_num in range(1, days_in_month + 1):
                date_str = f"{year}-{month:02d}-{day_num:02d}"
                
                if date_str in emp_data['attendance_dates']:
                    status = "present"
                    employee["summary"]["totalPresent"] += 1
                else:
                    if day_num < day:
                        status = "absent"
                        employee["summary"]["totalAbsent"] += 1
                    else:
                        status = "noRecord"
                        
                employee["attendance"][str(day_num)] = {
                    "status": status,
                    "date": date_str
                }
                
            formatted_employees.append(employee)
        return formatted_employees
    
    def format_locations_attendance(self, data, date_range="mes"):
        now = datetime.now(timezone('America/Mexico_City'))
        year = now.year
        month = now.month
        day = now.day
        days_in_month = monthrange(year, month)[1]
        
        # Asegurarse de que los turnos por defecto estén cargados
        self.shifts = self.shifts or self.default_shifts
        
        # Extraer todas las ubicaciones únicas
        all_locations = set()
        for record in data:
            location = record.get('incidente_location', '')
            if location:
                all_locations.add(location)
        
        # Para cada ubicación, determinar qué turnos aplican
        location_shifts = {}
        for location in all_locations:
            # Buscar turnos específicos para esta ubicación
            location_specific_shifts = []
            for shift_id, shift_info in self.shifts.items():
                if location in shift_info.get("locations", []):
                    location_specific_shifts.append((shift_id, shift_info))
            
            # Si no hay turnos específicos, usar los predeterminados
            if not location_specific_shifts:
                location_shifts[location] = [
                    ("T1", self.default_shifts["T1"]),
                    ("T2", self.default_shifts["T2"]),
                    ("T3", self.default_shifts["T3"])
                ]
            else:
                location_shifts[location] = location_specific_shifts
    
        # Inicializar estructura para todas las ubicaciones y sus turnos aplicables
        location_shift_data = {}
        for location, shifts in location_shifts.items():
            for shift_id, shift_info in shifts:
                location_shift_key = f"{location.replace(' ', '_').lower()}-{shift_id}"
                location_shift_data[location_shift_key] = {
                    'locationName': location,
                    'shiftInfo': shift_info,
                    'attendance_dates': {},
                    'records': []
                }
    
        # Procesar los registros
        for record in data:
            location = record.get('incidente_location', '')
            if not location:
                continue
            
            fecha_str = record.get('fecha_inicio_turno') or record.get('fecha_cierre_turno')
            if not fecha_str:
                continue
                
            try:
                fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                attendance_date = fecha_dt.strftime('%Y-%m-%d')
                
                # Usar la nueva función get_shift pasando la ubicación
                shift_info = self.get_shift(fecha_dt.hour, location)
                shift_id = shift_info["id"]
                
                location_shift_key = f"{location.replace(' ', '_').lower()}-{shift_id}"
                user_name = record.get('user_name', '')
                
                # Si este turno no está en los configurados para esta ubicación, saltar
                if location_shift_key not in location_shift_data:
                    continue
                    
                location_shift_data[location_shift_key]['records'].append(record)
                
                if attendance_date not in location_shift_data[location_shift_key]['attendance_dates']:
                    location_shift_data[location_shift_key]['attendance_dates'][attendance_date] = []
                    
                if user_name and user_name not in location_shift_data[location_shift_key]['attendance_dates'][attendance_date]:
                    location_shift_data[location_shift_key]['attendance_dates'][attendance_date].append(user_name)
                
            except (ValueError, TypeError):
                pass
    
        # Formatear salida
        formatted_locations = []
        for location_shift_key, loc_data in location_shift_data.items():
            location_shift = {
                "id": location_shift_key,
                "type": "location",
                "locationName": loc_data['locationName'],
                "shiftInfo": loc_data['shiftInfo'],
                "attendance": {},
                "summary": {
                    "totalPresent": 0,
                    "totalLate": 0,
                    "totalAbsent": 0,
                    "totalNotApplicable": 0  # Nuevo contador para días no aplicables
                }
            }
            
            for day_num in range(1, days_in_month + 1):
                date_str = f"{year}-{month:02d}-{day_num:02d}"
                
                # Determinar si este día de la semana aplica para este turno
                day_date = datetime(year, month, day_num)
                day_of_week = day_date.strftime("%A").lower()  # Obtenemos el día de la semana en inglés
                
                # Convertir a formato español/número si es necesario para coincidir con el formato en shifts
                day_mapping = {
                    "monday": "lunes", 
                    "tuesday": "martes", 
                    "wednesday": "miercoles", 
                    "thursday": "jueves", 
                    "friday": "viernes", 
                    "saturday": "sabado", 
                    "sunday": "domingo"
                }
                
                day_es = day_mapping.get(day_of_week, day_of_week)
                
                # También podemos manejar el caso donde los días se almacenan como números (0=lunes, 6=domingo)
                day_num_week = day_date.weekday()  # 0 es lunes, 6 es domingo
                
                # Verificar si el día aplica para este turno
                days_config = loc_data['shiftInfo'].get('days', [])
                
                # Intentar distintos formatos de días para mayor compatibilidad
                day_applies = (
                    day_es in days_config or 
                    day_of_week in days_config or 
                    day_num_week in days_config or 
                    str(day_num_week) in days_config or
                    # Si no hay configuración de días, asumimos que aplica todos los días
                    not days_config
                )
                
                if not day_applies:
                    # Este día de la semana no aplica para este turno
                    status = "notApplicable"
                    location_shift["summary"]["totalNotApplicable"] += 1
                    location_shift["attendance"][str(day_num)] = {
                        "status": status,
                        "date": date_str
                    }
                    continue
                
                # Si llegamos aquí, el día sí aplica para este turno
                # Ahora verificamos si hay asistencia registrada
                if date_str in loc_data['attendance_dates'] and loc_data['attendance_dates'][date_str]:
                    status = "present"
                    location_shift["summary"]["totalPresent"] += 1
                    user_names = loc_data['attendance_dates'][date_str]
                    
                    # Enviar la lista completa de usuarios sin formatear
                    location_shift["attendance"][str(day_num)] = {
                        "status": status,
                        "date": date_str,
                        "userName": user_names  # Lista completa de nombres de usuario
                    }
                else:
                    if day_num < day:
                        status = "absent"
                        location_shift["summary"]["totalAbsent"] += 1
                    else:
                        status = "noRecord"
                        
                    location_shift["attendance"][str(day_num)] = {
                        "status": status,
                        "date": date_str
                    }
            
            formatted_locations.append(location_shift)
        return formatted_locations
    
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
    group_by = data.get('group_by', 'employees')
    locations = data.get('locations', [])
    option = data.get('option', 'get_report')

    response = {}
    if option == 'get_report':
        script_obj.get_guard_shifts()
        response = script_obj.get_employees_attendance(date_range='mes', group_by=group_by, locations=locations)
    elif option == 'get_locations':
        response = script_obj.get_locations()

    script_obj.HttpResponse({"data": response})

