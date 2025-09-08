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
            try:
                # Extraer datos base
                shift_id = record.get('nombre_horario', '')
                hora_entrada = record.get('hora_entrada', '00:00')
                hora_salida = record.get('hora_salida', '00:00')
                
                # Parsear horas y minutos una sola vez
                entrada_hora, entrada_minuto = map(int, hora_entrada.split(':')[:2]) if ':' in hora_entrada else (0, 0)
                salida_hora, salida_minuto = map(int, hora_salida.split(':')[:2]) if ':' in hora_salida else (0, 0)
                
                # Convertir valores numéricos a int
                tolerancia = int(record.get('tolerancia_retardo', '0') or 0)
                limite_retardo = int(record.get('retardo_maximo', '0') or 0)
                
                # Guardar estructura optimizada
                self.shifts[shift_id] = { #type: ignore
                    "id": shift_id,
                    "name": f"{shift_id}: {hora_entrada[:5]} - {hora_salida[:5]} hrs",
                    # Valores pre-parseados para cálculos
                    "entrada_hora": entrada_hora,
                    "entrada_minuto": entrada_minuto,
                    "salida_hora": salida_hora, 
                    "salida_minuto": salida_minuto,
                    # Mantener timeRange para compatibilidad
                    "timeRange": f"{hora_entrada[:5]} - {hora_salida[:5]} hrs",
                    "locations": [r.get('incidente_location', '') for r in record.get('grupo_ubicaciones_horario', []) or []],
                    "days": record.get('dias_de_la_semana', []),
                    "tolerancia": tolerancia,
                    "limite_retardo": limite_retardo,
                    # Agregar valores calculados útiles
                    "minutos_entrada": entrada_hora * 60 + entrada_minuto,
                    "minutos_salida": salida_hora * 60 + salida_minuto,
                    "es_nocturno": salida_hora < entrada_hora  # Detecta automáticamente turnos nocturnos
                }
            except Exception as e:
                print(f"Error procesando turno {record.get('nombre_horario', '')}: {str(e)}")
                continue
        print(simplejson.dumps(self.shifts, indent=4))

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
                "dias_libres": {"$ifNull": [f"$answers.{self.f['dias_libres_empleado']}", []]}
            }}
        ]
        response = self.format_cr(self.cr.aggregate(query))
        format_response = list({'nombre': i.get('nombre_usuario'), 'dias_libres': i.get('dias_libres'), 'employee_id': self.unlist(i.get('employee_id', 0))} for i in response)
        return format_response

    def get_employees_attendance(self, date_range="mes", group_by="employees", locations=[]):
        employees_list = self.get_employees_list()
        employees_ids = list({i.get('employee_id', '') for i in employees_list})
        match = {
            "deleted_at": {"$exists": False},
            "form_id": 135386, #TODO: Modulariar ID
            "user_id": {"$in": employees_ids}
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
                "user_id": 1,
                "created_at": 1,
                "user_name": 1,
                "answers": 1
            }},
        ]
        response = self.format_cr(self.cr.aggregate(query))
        response = self.reformat_employees_attendance(data=response)
        format_response = {}
        if group_by == "employees":
            format_response = self.evaluate_attendance_status(combined_records=response, employees_list=employees_list)
        elif group_by == "locations":
            format_response = self.evaluate_locations_attendance(combined_records=response, employees_list=employees_list, locations=locations)
        return format_response
    
    def reformat_employees_attendance(self, data):
        """
        Prepara los datos de asistencia para su procesamiento:
        - Filtra registros sin fecha de inicio/cierre
        - Combina registros del mismo día, empleado Y UBICACIÓN
        - Preserva registros en diferentes ubicaciones para el mismo día
        """
        # 1. Filtrar registros que tengan al menos una fecha (inicio o cierre)
        valid_records = []
        for record in data:
            if record.get('fecha_inicio_turno') or record.get('fecha_cierre_turno'):
                valid_records.append(record)
        
        # 2. Organizar por empleado, día y ubicación para combinar registros relacionados
        employee_day_records = {}
        
        now = datetime.now(timezone('America/Mexico_City'))
        current_month = now.month
        current_year = now.year
        
        for record in valid_records:
            user_name = record.get('user_name')
            location = record.get('incidente_location', '')
            if not user_name:
                continue
                
            # Determinar la fecha clave y manejar turnos que cruzan de mes
            fecha_key = None
            is_cross_month = False
            record_month = None
            
            # Obtener fecha desde inicio o cierre
            if record.get('fecha_inicio_turno'):
                try:
                    fecha_dt = datetime.strptime(record.get('fecha_inicio_turno'), '%Y-%m-%d %H:%M:%S')
                    fecha_key = fecha_dt.strftime('%Y-%m-%d')
                    record_month = fecha_dt.month
                    
                    # Si es un registro del último día del mes anterior y es nocturno
                    if record_month != current_month and fecha_dt.hour >= 22:
                        is_cross_month = True
                        # Ajustar la fecha clave al primer día del mes actual para turnos nocturnos
                        fecha_key = f"{current_year}-{current_month:02d}-01"
                except (ValueError, TypeError):
                    pass
            
            if not fecha_key and record.get('fecha_cierre_turno'):
                try:
                    fecha_dt = datetime.strptime(record.get('fecha_cierre_turno'), '%Y-%m-%d %H:%M:%S')
                    fecha_key = fecha_dt.strftime('%Y-%m-%d')
                    record_month = fecha_dt.month
                    
                    # Si es un registro del primer día del mes actual pero con hora temprana
                    if record_month == current_month and fecha_dt.day == 1 and fecha_dt.hour < 8:
                        # Podría ser un cierre de turno nocturno que empezó el mes anterior
                        is_cross_month = True
                except (ValueError, TypeError):
                    pass
            
            if not fecha_key:
                continue
            
            # Para registros que cruzan de mes, asignarlos al primer día del mes actual
            if is_cross_month and record_month != current_month:
                fecha_key = f"{current_year}-{current_month:02d}-01"
                
            # Clave compuesta para identificar registros del mismo empleado, día y ubicación
            # IMPORTANTE: La ubicación es parte esencial de la clave
            employee_day_key = f"{user_name}|{fecha_key}|{location}"
            
            if employee_day_key not in employee_day_records:
                employee_day_records[employee_day_key] = {
                    "user_name": user_name,
                    "fecha": fecha_key,
                    "location": location,
                    "records": []
                }
            
            employee_day_records[employee_day_key]["records"].append(record)
        
        # 3. Combinar registros relacionados
        combined_records = []
        
        for employee_day_key, data_group in employee_day_records.items():
            records = data_group["records"]
            
            # Si solo hay un registro, usarlo directamente
            if len(records) == 1:
                combined_records.append(records[0])
                continue
            
            # Si hay múltiples registros DE LA MISMA UBICACIÓN, combinarlos
            combined_record = {
                "user_name": data_group["user_name"],
                "incidente_location": data_group["location"],  # Preservar la ubicación específica
                "incidente_area": None,
                "fecha_inicio_turno": None,
                "fecha_cierre_turno": None,
                "foto_inicio_turno": None,
                "foto_cierre_turno": None,
                "comentario_inicio_turno": None,
                "comentario_cierre_turno": None,
                "tipo_guardia": None,
                "estatus_guardia": None,
                "created_at": data_group["fecha"]
            }
            
            # Buscar la fecha de inicio más temprana y la fecha de cierre más tardía
            earliest_inicio = None
            latest_cierre = None
            
            for record in records:
                # Fechas y fotos de inicio
                if record.get('fecha_inicio_turno'):
                    inicio_time = record.get('fecha_inicio_turno')
                    if not earliest_inicio or inicio_time < earliest_inicio:
                        earliest_inicio = inicio_time
                        combined_record["foto_inicio_turno"] = record.get('foto_inicio_turno')
                        combined_record["comentario_inicio_turno"] = record.get('comentario_inicio_turno')
                
                # Fechas y fotos de cierre
                if record.get('fecha_cierre_turno'):
                    cierre_time = record.get('fecha_cierre_turno')
                    if not latest_cierre or cierre_time > latest_cierre:
                        latest_cierre = cierre_time
                        combined_record["foto_cierre_turno"] = record.get('foto_cierre_turno')
                        combined_record["comentario_cierre_turno"] = record.get('comentario_cierre_turno')
                
                # Otros campos relevantes (tomar el primero que encontremos)
                if record.get('incidente_area') and not combined_record["incidente_area"]:
                    combined_record["incidente_area"] = record.get('incidente_area')
                if record.get('tipo_guardia') and not combined_record["tipo_guardia"]:
                    combined_record["tipo_guardia"] = record.get('tipo_guardia')
                if record.get('estatus_guardia') and not combined_record["estatus_guardia"]:
                    combined_record["estatus_guardia"] = record.get('estatus_guardia')
            
            # Asignar las fechas optimizadas
            combined_record["fecha_inicio_turno"] = earliest_inicio
            combined_record["fecha_cierre_turno"] = latest_cierre
            
            combined_records.append(combined_record)
        # 4. Verificar que tengamos turnos cargados para el cálculo
        if not self.shifts:
            self.shifts = self.default_shifts
        
        return combined_records
    
    def evaluate_attendance_status(self, combined_records, employees_list=[]):
        """
        Evalúa el estado de asistencia usando user_id como clave principal
        para evitar problemas con cambios en nombres de usuario.
        """
        now = datetime.now(timezone('America/Mexico_City'))
        day = now.day
        
        # Crear diccionario para acceso rápido por ID
        employees_by_id = {}
        for employee in employees_list:
            emp_id = employee.get('employee_id')
            if emp_id:  # Solo agregar si tiene ID válido
                employees_by_id[emp_id] = {
                    "id": emp_id,
                    "name": employee.get('nombre', ''),
                    "locations": [],
                    "attendance": [],
                    "summary": {"present": 0, "late": 0, "absent": 0},
                    "dias_libres": employee.get('dias_libres', [])
                }
        
        # Crear un mapa de user_name a user_id para registros sin ID
        name_to_id_map = {emp.get('nombre', ''): emp.get('employee_id') for emp in employees_list if emp.get('employee_id')}
        
        # Evaluar cada registro combinado
        for record in combined_records:
            user_id = record.get('user_id')
            user_name = record.get('user_name')
            
            # Si no tiene ID, intentar buscar por nombre
            if not user_id and user_name:
                user_id = name_to_id_map.get(user_name)
            
            # Si no hay ID o el ID no está en nuestros empleados, omitir
            if not user_id or user_id not in employees_by_id:
                continue
            
            location = record.get('incidente_location', '')
            if location and location not in employees_by_id[user_id]["locations"]:
                employees_by_id[user_id]["locations"].append(location)
            
            # Continuar con la misma lógica de evaluación...
            if record.get('fecha_inicio_turno'):
                try:
                    fecha_inicio = record.get('fecha_inicio_turno')
                    fecha_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')
                    day_num = fecha_dt.day
                    hora_real = fecha_dt.hour
                    minuto_real = fecha_dt.minute
                    
                    if not location:
                        continue
                    
                    # Obtener el turno correspondiente (con 10 min de anticipación)
                    shift_info = self.get_shift(hora_real, location, minuto_real, anticipada=10)
                    
                    # Extraer la tolerancia y límite de retardo
                    tolerancia = int(shift_info.get('tolerancia', 0) or 0)
                    limite_retardo = int(shift_info.get('limite_retardo', 0) or 0)
                    
                    # Evaluar retraso
                    time_range = shift_info.get('timeRange', '')
                    if time_range:
                        try:
                            hora_planificada = time_range.split(" - ")[0]
                            hora_plan, minuto_plan = map(int, hora_planificada.split(":"))
                            
                            # Calcular retraso en minutos
                            minutos_planificados = hora_plan * 60 + minuto_plan
                            minutos_reales = hora_real * 60 + minuto_real
                            minutos_retraso = minutos_reales - minutos_planificados
                            
                            # Determinar estado según retraso
                            if minutos_retraso <= tolerancia:
                                status = "present"
                                employees_by_id[user_id]["summary"]["present"] += 1
                            elif minutos_retraso <= limite_retardo:
                                status = "halfDay"
                                employees_by_id[user_id]["summary"]["late"] += 1
                            else:
                                status = "absentTimeOff"
                                employees_by_id[user_id]["summary"]["absent"] += 1
                            
                            # Actualizar o agregar el día a la asistencia
                            self._update_attendance_entry(employees_by_id[user_id]["attendance"], day_num, status, location)
                            
                        except (IndexError, ValueError):
                            status = "present"
                            employees_by_id[user_id]["summary"]["present"] += 1
                            self._update_attendance_entry(employees_by_id[user_id]["attendance"], day_num, status, location)
                            
                except (ValueError, TypeError):
                    pass
                    
            # Si solo hay fecha de cierre, registrar como presente
            elif record.get('fecha_cierre_turno'):
                try:
                    fecha_cierre = record.get('fecha_cierre_turno')
                    fecha_dt = datetime.strptime(fecha_cierre, '%Y-%m-%d %H:%M:%S')
                    day_num = fecha_dt.day
                    
                    day_exists = any(att.get("day") == day_num for att in employees_by_id[user_id]["attendance"])
                    if not day_exists:
                        status = "present"
                        employees_by_id[user_id]["summary"]["present"] += 1
                        self._update_attendance_entry(employees_by_id[user_id]["attendance"], day_num, status, location)
                        
                except (ValueError, TypeError):
                    pass
    
        # Marcar ausencias para días pasados sin registro
        for user_id, emp_data in employees_by_id.items():
            days_registered = {att.get("day") for att in emp_data["attendance"]}
            for day_num in range(1, day):
                if day_num not in days_registered:
                    status = "absent"
                    emp_data["summary"]["absent"] += 1
                    default_location = emp_data["locations"][0] if emp_data["locations"] else ""
                    self._update_attendance_entry(emp_data["attendance"], day_num, status, default_location)
    
        # Formatear resultado final
        result = {"employees": []}
        for _, emp_data in employees_by_id.items():
            # Ordenar asistencias por día
            emp_data["attendance"].sort(key=lambda x: x["day"])
            # Solo incluir empleados con al menos un registro
            if emp_data["attendance"]:
                result["employees"].append(emp_data)
        
        return result

    def _update_attendance_entry(self, attendance_list, day, status, location):
        """
        Actualiza o agrega una entrada de asistencia, evitando duplicados
        y priorizando los estados mejores (present > halfDay > absent)
        """
        for i, att in enumerate(attendance_list):
            if att["day"] == day:
                # Si el status actual es mejor que el nuevo, mantenerlo
                current_status = att["status"]
                if (current_status == "present" or 
                    (current_status == "halfDay" and status == "absent")):
                    return
                # Actualizar con el nuevo status
                attendance_list[i]["status"] = status
                if location:
                    attendance_list[i]["location"] = location
                return
        
        # Si no existe, agregar nueva entrada
        entry = {"day": day, "status": status}
        if location:
            entry["location"] = location
        attendance_list.append(entry)

    def evaluate_locations_attendance(self, combined_records, employees_list=[], locations=[]):
        """
        Evalúa el estado de asistencia para cada registro agrupado por ubicación y turno.
        Respeta estrictamente las ubicaciones configuradas para cada turno.
        """
        now = datetime.now(timezone('America/Mexico_City'))
        year = now.year
        month = now.month
        day = now.day
        days_in_month = monthrange(year, month)[1]
        
        # Asegurarse de que los turnos estén cargados
        if not self.shifts:
            self.get_guard_shifts()
        
        # Si no se especifican ubicaciones, extraer todas las ubicaciones de los registros
        if not locations:
            locations_set = set()
            for record in combined_records:
                location = record.get('incidente_location', '')
                if location:
                    locations_set.add(location)
            locations = list(locations_set)
    
        # Inicializar estructura para todas las ubicaciones y sus turnos
        location_shift_data = {}
    
        # Para cada ubicación, determinar qué turnos aplican
        for location in locations:
            applicable_shifts = []
            for shift_id, shift_info in self.shifts.items():
                shift_locations = shift_info.get("locations", [])
                if location in shift_locations:
                    applicable_shifts.append((shift_id, shift_info, False))  # False = no es default

            # Si no hay turnos específicos para esta ubicación, usar los predeterminados
            if not applicable_shifts:
                for shift_id, shift_info in self.default_shifts.items():
                    shift_info_copy = shift_info.copy()
                    shift_info_copy["locations"] = []
                    shift_info_copy["days"] = []  # Todos los días aplican
                    applicable_shifts.append((shift_id, shift_info_copy, True))  # True = es default

            for shift_id, shift_info, is_default in applicable_shifts:
                location_shift_key = f"{location.replace(' ', '_').lower()}_{shift_id.lower()}"
                location_shift_data[location_shift_key] = {
                    "id": location_shift_key,
                    "locationName": location,
                    "shiftId": shift_id,
                    "shiftName": shift_info.get("name", f"Turno {shift_id}"),
                    "attendance": {},
                    "summary": {"present": 0, "late": 0, "absent": 0, "totalNotApplicable": 0},
                    "is_default": is_default  # <-- NUEVO
                }
    
        # Evaluar cada registro combinado
        for record in combined_records:
            location = record.get('incidente_location', '')
            user_name = record.get('user_name', '')
            
            if not location or not user_name:
                continue
            
            # Evaluar registro con fecha de inicio
            if record.get('fecha_inicio_turno'):
                try:
                    fecha_inicio = record.get('fecha_inicio_turno')
                    fecha_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d %H:%M:%S')
                    day_num = fecha_dt.day
                    hora_real = fecha_dt.hour
                    minuto_real = fecha_dt.minute
                    
                    # Obtener el turno correspondiente (con 10 min de anticipación)
                    shift_info = self.get_shift(hora_real, location, minuto_real, anticipada=10)
                    shift_id = shift_info["id"]
                    
                    # Construir la clave para este par ubicación-turno
                    location_shift_key = f"{location.replace(' ', '_').lower()}_{shift_id.lower()}"
                    
                    # Verificar si esta combinación existe
                    # Si no existe, crearla con los valores del turno devuelto por get_shift
                    if location_shift_key not in location_shift_data:
                        # Esto puede ocurrir si get_shift devuelve un turno predeterminado
                        # que no se consideró inicialmente para esta ubicación
                        location_shift_data[location_shift_key] = {
                            "id": location_shift_key,
                            "locationName": location,
                            "shiftId": shift_id,
                            "shiftName": shift_info.get("name", f"Turno {shift_id}"),
                            "attendance": {},
                            "summary": {"present": 0, "late": 0, "absent": 0, "totalNotApplicable": 0}
                        }
                    
                    # Evaluar retraso
                    tolerancia = int(shift_info.get('tolerancia', 0) or 0)
                    limite_retardo = int(shift_info.get('limite_retardo', 0) or 0)
                    
                    time_range = shift_info.get('timeRange', '')
                    if time_range:
                        try:
                            hora_planificada = time_range.split(" - ")[0]
                            hora_plan, minuto_plan = map(int, hora_planificada.split(":"))
                            
                            # Calcular retraso en minutos
                            minutos_planificados = hora_plan * 60 + minuto_plan
                            minutos_reales = hora_real * 60 + minuto_real
                            minutos_retraso = minutos_reales - minutos_planificados
                            
                            # Determinar estado según retraso
                            if minutos_retraso <= tolerancia:
                                status = "present"
                                location_shift_data[location_shift_key]["summary"]["present"] += 1
                            elif minutos_retraso <= limite_retardo:
                                status = "late"
                                location_shift_data[location_shift_key]["summary"]["late"] += 1
                            else:
                                # Si supera el límite de retardo, se considera ausente
                                status = "absent"
                                location_shift_data[location_shift_key]["summary"]["absent"] += 1
                        
                        except (IndexError, ValueError):
                            # Si no podemos calcular el retraso, asumimos presente
                            self._update_location_attendance(location_shift_data[location_shift_key], day_num, 
                                                            "present", user_name, fecha_dt.strftime('%Y-%m-%d'))
                            
                except (ValueError, TypeError):
                    pass
                    
            # Si solo hay fecha de cierre y no hay registro para este día, registrar como presente
            elif record.get('fecha_cierre_turno'):
                try:
                    fecha_cierre = record.get('fecha_cierre_turno')
                    fecha_dt = datetime.strptime(fecha_cierre, '%Y-%m-%d %H:%M:%S')
                    day_num = fecha_dt.day
                    hora_real = fecha_dt.hour
                    
                    # Inferir el turno basado en la hora de cierre
                    shift_info = self.get_shift(hora_real, location, fecha_dt.minute)
                    shift_id = shift_info["id"]
                    location_shift_key = f"{location.replace(' ', '_').lower()}_{shift_id.lower()}"
                    
                    # Si esta combinación no existe, crearla
                    if location_shift_key not in location_shift_data:
                        location_shift_data[location_shift_key] = {
                            "id": location_shift_key,
                            "locationName": location,
                            "shiftId": shift_id,
                            "shiftName": shift_info.get("name", f"Turno {shift_id}"),
                            "attendance": {},
                            "summary": {"present": 0, "late": 0, "absent": 0, "totalNotApplicable": 0}
                        }
                    
                    if str(day_num) not in location_shift_data[location_shift_key]["attendance"]:
                        self._update_location_attendance(location_shift_data[location_shift_key], day_num, 
                                                        "present", user_name, fecha_dt.strftime('%Y-%m-%d'))
                except (ValueError, TypeError):
                    pass
    
        # Procesar todos los días del mes para cada ubicación-turno
        for location_shift_key, loc_data in location_shift_data.items():
            shift_id = loc_data["shiftId"]
            is_default = loc_data.get("is_default", False)
            # Buscar primero en shifts y luego en default_shifts
            shift_info = None
            for shift_dict in [self.shifts, self.default_shifts]:
                if shift_id in shift_dict:
                    shift_info = shift_dict[shift_id]
                    break
            
            # Si no se encontró el turno (no debería ocurrir), usar un diccionario vacío
            if not shift_info:
                shift_info = {}
                
            # Si es default, forzar days_config vacío
            days_config = [] if is_default else shift_info.get('days', [])
            
            # Procesar cada día del mes
            for day_num in range(1, days_in_month + 1):
                if str(day_num) not in loc_data["attendance"]:
                    day_date = datetime(year, month, day_num)
                    day_of_week = day_date.strftime("%A").lower()
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
                    day_applies = (
                        not days_config or
                        day_es in days_config or 
                        day_of_week in days_config or 
                        day_date.isoweekday() in days_config or 
                        str(day_date.isoweekday()) in days_config
                    )
                    if not day_applies:
                        status = "notApplicable"
                        loc_data["summary"]["totalNotApplicable"] += 1
                    elif day_num < day:
                        status = "absent"
                        loc_data["summary"]["absent"] += 1
                    else:
                        status = "noRecord"
                    loc_data["attendance"][str(day_num)] = {
                        "day": day_num,
                        "status": status,
                        "date": f"{year}-{month:02d}-{day_num:02d}"
                    }
        
        # Formatear el resultado final: convertir diccionario de attendance a lista ordenada
        result = []
        for _, location_shift in location_shift_data.items():
            # Convertir diccionario de attendance a lista ordenada por día
            attendance_list = []
            for day_num in sorted(map(int, location_shift["attendance"].keys())):
                attendance_list.append(location_shift["attendance"][str(day_num)])
            
            # Reemplazar el diccionario con la lista
            location_shift["attendance"] = attendance_list
            
            # Solo incluir ubicaciones con al menos un día registrado
            if attendance_list:
                result.append(location_shift)
        
        return result

    def _update_location_attendance(self, location_data, day_num, status, user_name, date_str):
        """
        Actualiza o agrega una entrada de asistencia para una ubicación-turno
        """
        if str(day_num) not in location_data["attendance"]:
            location_data["attendance"][str(day_num)] = {
                "day": day_num,
                "status": status,
                "date": date_str
            }
            if status == "present":
                location_data["summary"]["present"] += 1
                location_data["attendance"][str(day_num)]["userName"] = [user_name]
            elif status == "late":
                location_data["summary"]["late"] += 1
                location_data["attendance"][str(day_num)]["userName"] = [user_name]
        else:
            current_entry = location_data["attendance"][str(day_num)]
            
            # Solo actualizar si el estado actual es peor que el nuevo
            if (current_entry["status"] == "absent" and status in ["present", "late"]) or \
            (current_entry["status"] == "late" and status == "present"):
                current_entry["status"] = status
                
                # Actualizar el contador correspondiente
                if status == "present":
                    if current_entry["status"] == "late":
                        location_data["summary"]["late"] -= 1
                    elif current_entry["status"] == "absent":
                        location_data["summary"]["absent"] -= 1
                    location_data["summary"]["present"] += 1
                elif status == "late":
                    if current_entry["status"] == "absent":
                        location_data["summary"]["absent"] -= 1
                    location_data["summary"]["late"] += 1
            
            # Agregar nombre de usuario si es necesario
            if status in ["present", "late"]:
                if "userName" not in current_entry:
                    current_entry["userName"] = [user_name]
                elif user_name not in current_entry["userName"]:
                    current_entry["userName"].append(user_name)

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
        response = response.get('employees', []) if group_by == 'employees' else response
    elif option == 'get_locations':
        response = script_obj.get_locations()

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})

