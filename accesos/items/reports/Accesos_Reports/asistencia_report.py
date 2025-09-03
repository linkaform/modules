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
    def get_shift(self, hour, location=None, minute=0, anticipada=30):
        if not self.shifts or not location:
            if 6 <= hour < 14:
                return self.default_shifts["T1"]
            elif 14 <= hour < 22:
                return self.default_shifts["T2"]
            else:
                return self.default_shifts["T3"]

        location_shifts = []
        for shift_id, shift_info in self.shifts.items():
            if location in shift_info.get("locations", []):
                try:
                    entrada = shift_info.get("timeRange", "").split(" - ")[0]
                    salida = shift_info.get("timeRange", "").split(" - ")[1].replace(" hrs", "")
                    hora_entrada, minuto_entrada = map(int, entrada.split(":"))
                    hora_salida, minuto_salida = map(int, salida.split(":"))
                    location_shifts.append({
                        "shift": shift_info,
                        "hora_entrada": hora_entrada,
                        "minuto_entrada": minuto_entrada,
                        "hora_salida": hora_salida,
                        "minuto_salida": minuto_salida
                    })
                except (IndexError, ValueError):
                    continue

        if not location_shifts:
            if 6 <= hour < 14:
                return self.default_shifts["T1"]
            elif 14 <= hour < 22:
                return self.default_shifts["T2"]
            else:
                return self.default_shifts["T3"]

        minutos_actual = hour * 60 + minute

        # Ordenar turnos por hora de entrada para priorizar la ventana anticipada del turno siguiente
        location_shifts.sort(key=lambda s: (s["hora_entrada"], s["minuto_entrada"]))

        # 1. Buscar si está en la ventana anticipada de algún turno y retornar inmediatamente
        for shift in location_shifts:
            entrada = shift["hora_entrada"]
            min_entrada = shift["minuto_entrada"]
            minutos_entrada = entrada * 60 + min_entrada
            if minutos_entrada - anticipada <= minutos_actual < minutos_entrada:
                return shift["shift"]

        # 2. Si no está en ventana anticipada, buscar turno normal
        for shift in location_shifts:
            entrada = shift["hora_entrada"]
            salida = shift["hora_salida"]
            min_entrada = shift["minuto_entrada"]
            min_salida = shift["minuto_salida"]
            minutos_entrada = entrada * 60 + min_entrada
            minutos_salida = salida * 60 + min_salida

            if entrada < salida:
                # Turno normal (ej: 06:00-14:00)
                if minutos_entrada <= minutos_actual < minutos_salida:
                    return shift["shift"]
            else:
                # Turno nocturno (ej: 22:00-06:00)
                if minutos_actual >= minutos_entrada or minutos_actual < minutos_salida:
                    return shift["shift"]

        # Si ningún turno específico coincide, usar los predeterminados
        if 6 <= hour < 14:
            return self.default_shifts["T1"]
        elif 14 <= hour < 22:
            return self.default_shifts["T2"]
        else:
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
        print(simplejson.dumps(self.shifts, indent=4))

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
            last_day_prev_month = start_of_month - timedelta(days=1)
            match.update({
                "created_at": {"$gte": last_day_prev_month}
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
        
        # Asegurarse de que los turnos por defecto estén cargados
        self.shifts = self.shifts or self.default_shifts
        
        employees_data = {}
        for record in data:
            user_name = record.get('user_name', '')
            if not user_name:
                continue
                
            if user_name not in employees_data:
                employees_data[user_name] = {
                    'records': [],
                    'attendance_dates': {}  # Ahora será {fecha: {'status': 'on_time|late', 'location': 'ubicacion'}}
                }
                
            employees_data[user_name]['records'].append(record)
            
            # Procesar fecha de inicio para evaluar retrasos
            if record.get('fecha_inicio_turno'):
                fecha_str = record.get('fecha_inicio_turno')
                try:
                    fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                    attendance_date = fecha_dt.strftime('%Y-%m-%d')
                    hora_real = fecha_dt.hour
                    minuto_real = fecha_dt.minute
                    location = record.get('incidente_location', '')
                    
                    if not location:
                        continue
                    
                    # Obtener el turno para esta hora y ubicación
                    shift_info = self.get_shift(hora_real, location, minuto_real, anticipada=30)
                    
                    # Extraer la tolerancia del turno
                    tolerancia = shift_info.get('tolerancia', 0)
                    try:
                        tolerancia = int(tolerancia)
                    except (ValueError, TypeError):
                        tolerancia = 0
                    
                    # Evaluar si hay retraso
                    time_range = shift_info.get('timeRange', '')
                    if time_range:
                        try:
                            hora_planificada = time_range.split(" - ")[0]
                            hora_plan, minuto_plan = map(int, hora_planificada.split(":"))
                            
                            # Calcular minutos totales desde medianoche
                            minutos_planificados = hora_plan * 60 + minuto_plan
                            minutos_reales = hora_real * 60 + minuto_real
                            
                            # La diferencia es el retraso
                            minutos_retraso = minutos_reales - minutos_planificados
                            
                            # Determinar estado basado en retraso
                            if minutos_retraso > tolerancia:
                                status = "late"
                                print(f"DEBUG: Usuario: {user_name}, Hora plan: {hora_plan}:{minuto_plan}, Hora real: {hora_real}:{minuto_real}")
                                print(f"DEBUG: Minutos retraso: {minutos_retraso}, Tolerancia: {tolerancia}")
                                print(f"DEBUG: Usuario marcado como TARDE: {user_name}")
                            else:
                                status = "on_time"
                            
                            # Guardar en attendance_dates - priorizar "on_time" si ya existe entrada
                            if attendance_date not in employees_data[user_name]['attendance_dates']:
                                employees_data[user_name]['attendance_dates'][attendance_date] = {
                                    'status': status,
                                    'location': location
                                }
                            elif status == "on_time":  # Siempre priorizar "on_time" sobre "late"
                                employees_data[user_name]['attendance_dates'][attendance_date]['status'] = status
                                
                        except (IndexError, ValueError):
                            # Si no podemos calcular, asumir que está a tiempo
                            if attendance_date not in employees_data[user_name]['attendance_dates']:
                                employees_data[user_name]['attendance_dates'][attendance_date] = {
                                    'status': "on_time",
                                    'location': location
                                }
                except (ValueError, TypeError):
                    pass
            # Procesar también registros con fecha de cierre (para casos donde no hay inicio)
            elif record.get('fecha_cierre_turno'):
                fecha_str = record.get('fecha_cierre_turno')
                try:
                    fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                    attendance_date = fecha_dt.strftime('%Y-%m-%d')
                    location = record.get('incidente_location', '')
                    
                    # Solo si no existe ya un registro para esta fecha
                    if attendance_date not in employees_data[user_name]['attendance_dates']:
                        employees_data[user_name]['attendance_dates'][attendance_date] = {
                            'status': "on_time",  # Asumimos a tiempo si solo hay cierre
                            'location': location or ""
                        }
                except (ValueError, TypeError):
                    pass
    
        formatted_employees = []
        for user_name, emp_data in employees_data.items():
            # Si no hay fechas de asistencia, omitir este empleado
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
                    attendance_info = emp_data['attendance_dates'][date_str]
                    if attendance_info['status'] == "on_time":
                        status = "present"
                        employee["summary"]["totalPresent"] += 1
                    else:  # late
                        status = "halfDay"  # Nuevo estado para retrasos
                        employee["summary"]["totalLate"] += 1
                        
                    # Agregar la ubicación solo si existe
                    location_data = {"location": attendance_info.get('location', '')} if attendance_info.get('location') else {}
                    
                    employee["attendance"][str(day_num)] = {
                        "status": status,
                        "date": date_str,
                        **location_data  # Usar unpacking para agregar location solo si existe
                    }
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
                    'attendance_dates': {},  # Ahora será {fecha: {'on_time': [], 'late': []}}
                    'records': []
                }

        # Procesar los registros
        for record in data:
            location = record.get('incidente_location', '')
            if not location:
                continue
            
            # Solo procesar registros con fecha de inicio para evaluar retrasos
            if not record.get('fecha_inicio_turno'):
                continue
                
            fecha_str = record.get('fecha_inicio_turno')
            try:
                fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
                attendance_date = fecha_dt.strftime('%Y-%m-%d')
                hora_real = fecha_dt.hour
                minuto_real = fecha_dt.minute
                
                # Obtener el turno para esta hora y ubicación
                shift_info = self.get_shift(hora_real, location, minuto_real, anticipada=30)
                shift_id = shift_info["id"]
                
                location_shift_key = f"{location.replace(' ', '_').lower()}-{shift_id}"
                user_name = record.get('user_name', '')
                
                # Si este turno no está en los configurados para esta ubicación, saltar
                if location_shift_key not in location_shift_data:
                    continue
                    
                # Inicializar estructura para esta fecha si no existe
                if attendance_date not in location_shift_data[location_shift_key]['attendance_dates']:
                    location_shift_data[location_shift_key]['attendance_dates'][attendance_date] = {
                        'on_time': [],
                        'late': []
                    }
                
                # Verificar si hay retraso
                # Extraer la tolerancia del turno (en minutos)
                tolerancia = location_shift_data[location_shift_key]['shiftInfo'].get('tolerancia', 0)
                try:
                    tolerancia = int(tolerancia)
                except (ValueError, TypeError):
                    tolerancia = 0
                
                # Extraer la hora planificada de inicio
                time_range = location_shift_data[location_shift_key]['shiftInfo'].get('timeRange', '')
                if time_range:
                    try:
                        hora_planificada = time_range.split(" - ")[0]
                        hora_plan, minuto_plan = map(int, hora_planificada.split(":"))
                        
                        # Calcular minutos totales desde medianoche para ambas horas
                        minutos_planificados = hora_plan * 60 + minuto_plan
                        minutos_reales = hora_real * 60 + minuto_real
                        
                        # La diferencia es el retraso
                        minutos_retraso = minutos_reales - minutos_planificados
                        
                        # Registrar el usuario en la categoría correspondiente
                        if minutos_retraso > tolerancia:
                            # Usuario con retraso
                            if user_name and user_name not in location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['late']:
                                location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['late'].append(user_name)
                                print(f"DEBUG: Usuario: {user_name}, Hora plan: {hora_plan}:{minuto_plan}, Hora real: {hora_real}:{minuto_real}")
                                print(f"DEBUG: Minutos retraso: {minutos_retraso}, Tolerancia: {tolerancia}")
                                print(f"DEBUG: Usuario marcado como TARDE: {user_name}")
                        else:
                            # Usuario a tiempo
                            if user_name and user_name not in location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['on_time']:
                                location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['on_time'].append(user_name)
                    
                    except (IndexError, ValueError):
                        # Si no podemos calcular el retraso, asumimos que está a tiempo
                        if user_name and user_name not in location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['on_time']:
                            location_shift_data[location_shift_key]['attendance_dates'][attendance_date]['on_time'].append(user_name)
                
                # Guardar el registro completo
                location_shift_data[location_shift_key]['records'].append(record)
                    
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
                    "totalNotApplicable": 0
                }
            }
            
            for day_num in range(1, days_in_month + 1):
                date_str = f"{year}-{month:02d}-{day_num:02d}"
                
                # Determinar si este día de la semana aplica para este turno
                day_date = datetime(year, month, day_num)
                day_of_week = day_date.strftime("%A").lower()
                
                # Convertir a formato español/número para coincidir con el formato en shifts
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
                
                # Verificar si el día aplica para este turno
                days_config = loc_data['shiftInfo'].get('days', [])

                # Calcular el día de la semana como número (1-7)
                day_num_week = day_date.isoweekday()  # Lunes=1, Domingo=7

                # Intentar distintos formatos de días para compatibilidad
                day_applies = (
                    day_es in days_config or 
                    day_of_week in days_config or 
                    day_num_week in days_config or 
                    str(day_num_week) in days_config or
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
                # Verificamos si hay asistencia registrada
                if date_str in loc_data['attendance_dates']:
                    attendance_data = loc_data['attendance_dates'][date_str]
                    on_time_users = attendance_data.get('on_time', [])
                    late_users = attendance_data.get('late', [])
                    
                    # Primero verificamos si hay usuarios a tiempo
                    if on_time_users:
                        status = "present"
                        location_shift["summary"]["totalPresent"] += len(on_time_users)
                        
                        location_shift["attendance"][str(day_num)] = {
                            "status": status,
                            "date": date_str,
                            "userName": on_time_users
                        }
                    # Luego verificamos si hay usuarios con retraso
                    elif late_users:
                        status = "late"
                        location_shift["summary"]["totalLate"] += len(late_users)
                        
                        location_shift["attendance"][str(day_num)] = {
                            "status": status,
                            "date": date_str,
                            "userName": late_users
                        }
                    else:
                        # No hay usuarios registrados para este día
                        if day_num < day:
                            status = "absent"
                            location_shift["summary"]["totalAbsent"] += 1
                        else:
                            status = "noRecord"
                        
                        location_shift["attendance"][str(day_num)] = {
                            "status": status,
                            "date": date_str
                        }
                else:
                    # No hay registros para este día
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

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})

