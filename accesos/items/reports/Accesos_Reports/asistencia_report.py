# -*- coding: utf-8 -*-
import re
import sys, simplejson, math
from math import ceil
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
        self.HORARIOS = self.lkm.form_id('horarios', 'id')
        
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
            'dias_libres_empleado': '68bb20095035e61c5745de05',
            'grupo_turnos': '68b6427cc8f94827ebfed699',
            'free_day_hora_inicio': '61af7e19b7b4b6d6731ffddc',
            'free_day_hora_fin': '61af7e19b7b4b6d6731ffddd',
            'free_day_dias': '55887b7e01a4de2ea71c5ab6',
            'free_day_comentario': '59513ddbf851c214cf24430c',
            'usuario_email': '638a9a7767c332f5d459fc82',
            'usuario_nombre': '638a9a7767c332f5d459fc81',
        })
        
        self.shifts = {}

    def get_employees_list(self):
        puesto_cat = self.CONF_DEPARTAMENTOS_PUESTOS_CAT_OBJ_ID
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.EMPLEADOS,
            }},
            {"$project": {
                "_id": 0,
                "employee_id": f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['id_usuario']}",
                "nombre_usuario": f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['nombre_usuario']}",
                "departamento": f"$answers.{puesto_cat}.{self.f['departamento_empleado']}",
                "puesto": f"$answers.{puesto_cat}.{self.f['puesto_empleado']}",
                "email": f"$answers.{self.USUARIOS_OBJ_ID}.{self.f['usuario_email']}",
                "dias_libres": f"$answers.{self.f['dias_libres_empleado']}",
            }}
        ]
        response = self.cr.aggregate(query)
        ubicaciones = self.get_employees_locations()
        format_response = []
        for i in response:
            employee_id = self.unlist(i.get('employee_id', 0))
            format_response.append({
                'nombre': self.unlist(i.get('nombre_usuario')),
                'employee_id': employee_id,
                'departamento': self.unlist(i.get('departamento')) or '',
                'puesto': self.unlist(i.get('puesto')) or '',
                'dias_libres': i.get('dias_libres') or [],
                'email': (self.unlist(i.get('email')) or '').strip().lower(),
                'ubicaciones': ubicaciones.get(employee_id, []),
            })
        return format_response

    def get_employees_locations(self):
        """{user_id: [ubicaciones]} desde Configuración de áreas y empleados (la default primero)."""
        areas_path = self.f['areas_group']
        cat_obj_id = self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID
        user_path = f"{self.EMPLOYEE_OBJ_ID}.{self.employee_fields['user_id_id']}"
        result = {}
        query = {"deleted_at": {"$exists": False}, "form_id": self.CONF_AREA_EMPLEADOS}
        for doc in self.cr.find(query, {f"answers.{user_path}": 1, f"answers.{areas_path}": 1}):
            answers = doc.get('answers', {})
            user_id = self.unlist(answers.get(self.EMPLOYEE_OBJ_ID, {}).get(self.employee_fields['user_id_id']))
            if not user_id:
                continue
            locations = result.setdefault(user_id, [])
            for item in answers.get(areas_path, []) or []:
                location = self.unlist((item.get(cat_obj_id, {}) or {}).get(self.f['location']))
                if not location or location in locations:
                    continue
                if item.get(self.f['area_default']) == 'default':
                    locations.insert(0, location)
                else:
                    locations.append(location)
        return result

    def get_schedules(self):
        """Horarios (jornadas) con las ubicaciones a las que aplican."""
        query = {"deleted_at": {"$exists": False}, "form_id": self.HORARIOS}
        horarios = []
        for doc in self.cr.find(query, {"answers": 1}):
            answers = doc.get('answers', {})
            ubicaciones = []
            for item in answers.get(self.f['grupo_turnos'], []) or []:
                for value in item.values():
                    if isinstance(value, dict) and value.get(self.f['location']):
                        ubicaciones.append(self.unlist(value[self.f['location']]))
            horarios.append({
                "nombre": answers.get(self.f['nombre_horario'], ''),
                "hora_entrada": answers.get(self.f['hora_entrada'], ''),
                "hora_salida": answers.get(self.f['hora_salida'], ''),
                "tolerancia_retardo": answers.get(self.f['tolerancia_retardo']),
                "ubicaciones": ubicaciones,
            })
        return sorted(horarios, key=lambda h: (h['hora_entrada'], h['nombre']))

    def employee_schedule(self, ubicaciones, horarios):
        """Jornada del empleado: el primer horario (por hora de entrada) de su primera ubicación con horario."""
        for ubicacion in ubicaciones:
            for horario in horarios:
                if ubicacion in horario['ubicaciones']:
                    return {k: horario[k] for k in ('nombre', 'hora_entrada', 'hora_salida', 'tolerancia_retardo')}
        return None

    def get_free_days(self, date_from, date_to, employees_list=[]):
        """Solicitudes autorizadas de FORMATO_VACACIONES que caen en el periodo, por employee_id.
        Se cruza por el id de usuario del catálogo; los registros viejos no lo traen y se cruzan
        por correo, luego por nombre y al final por quien la creó."""
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.FORMATO_VACACIONES,
                f"answers.{self.f['free_day_start']}": {"$lte": date_to},
                f"answers.{self.f['free_day_end']}": {"$gte": date_from},
                f"answers.{self.f['free_day_autorization']}": "autorizado"
            }},
            {"$project": {
                "_id": 0,
                "folio": 1,
                "created_by_id": 1,
                "fecha_inicio": f"$answers.{self.f['free_day_start']}",
                "fecha_fin": f"$answers.{self.f['free_day_end']}",
                "tipo": f"$answers.{self.f['free_day_type']}",
                "hora_inicio": f"$answers.{self.f['free_day_hora_inicio']}",
                "hora_fin": f"$answers.{self.f['free_day_hora_fin']}",
                "dias": f"$answers.{self.f['free_day_dias']}",
                "comentario": f"$answers.{self.f['free_day_comentario']}",
                "id_usuario": f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.mf['id_usuario']}",
                "email": f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.f['usuario_email']}",
                "nombre": f"$answers.{self.EMPLOYEE_OBJ_ID}.{self.f['usuario_nombre']}",
            }},
        ]
        por_email = {e['email']: e['employee_id'] for e in employees_list if e.get('email')}
        por_nombre = {(e.get('nombre') or '').strip().lower(): e['employee_id'] for e in employees_list if e.get('nombre')}
        ids = {e['employee_id'] for e in employees_list}

        dias_por_usuario = {}
        for item in self.cr.aggregate(query):
            email = (self.unlist(item.get('email')) or '').strip().lower()
            nombre = (self.unlist(item.get('nombre')) or '').strip().lower()
            id_usuario = self.unlist(item.get('id_usuario'))
            emp_id = (id_usuario if id_usuario in ids else None) or por_email.get(email) or por_nombre.get(nombre)
            if not emp_id and item.get('created_by_id') in ids:
                emp_id = item['created_by_id']
            if not emp_id or not item.get('fecha_inicio') or not item.get('fecha_fin'):
                continue
            # Por horas: solicitud de un solo día con horas distintas (las vacaciones a veces traen horas de relleno)
            parcial = bool(
                item['fecha_inicio'][:10] == item['fecha_fin'][:10]
                and item.get('hora_inicio') and item.get('hora_fin')
                and item['hora_inicio'] != item['hora_fin']
            )
            dias_por_usuario.setdefault(emp_id, []).append({
                "folio": item.get('folio'),
                "parcial": parcial,
                "fecha_inicio": item['fecha_inicio'][:10],
                "fecha_fin": item['fecha_fin'][:10],
                "tipo": item.get('tipo'),
                "hora_inicio": item.get('hora_inicio'),
                "hora_fin": item.get('hora_fin'),
                "dias": item.get('dias'),
                "comentario": item.get('comentario'),
            })
        return dias_por_usuario

    def get_employees_attendance(self, group_by="locations", locations=[], month=1, year=2026, date_from=None, date_to=None):
        employees_list = self.get_employees_list()
        employees_ids = list(i.get('employee_id', '') for i in employees_list)
        
        # Rango YYYY-MM-DD; sin rango se usa el mes completo de month/year
        if not date_from or not date_to:
            days_in_month = monthrange(year, month)[1]
            date_from = f"{year:04d}-{month:02d}-01"
            date_to = f"{year:04d}-{month:02d}-{days_in_month:02d}"
        
        match = {
            "deleted_at": {"$exists": False},
            "form_id": self.REGISTRO_ASISTENCIA,
            "created_by_id": {"$in": employees_ids},
            f"answers.{self.f['start_shift']}": {
                "$gte": date_from,
                "$lte": f"{date_to} 23:59:59"
            },
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
                    "caseta": f"$doc.answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['area']}",
                    "comentario_cierre": f"$doc.answers.{self.f['comentario_cierre_turno']}",
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
        free_days = self.get_free_days(date_from, date_to, employees_list)
        if group_by == "employees":
            format_response = self.format_employees_attendance(response, employees_list, locations, date_from, date_to, free_days)
        elif group_by == "locations":
            format_response = self.format_locations_attendance(response, employees_list, date_from, date_to)
        return format_response
    
    def format_locations_attendance(self, data, employees_list, date_from=None, date_to=None):
        """Turnos por ubicación, caseta y horario: quién abrió cada turno cada día."""
        ahora = datetime.now(timezone('America/Mexico_City'))
        hoy = ahora.date()
        hora_actual = ahora.strftime('%H:%M:%S')
        dias_semana = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]
        inicio = datetime.strptime(date_from, '%Y-%m-%d').date()
        fin = datetime.strptime(date_to, '%Y-%m-%d').date()
        fechas = [inicio + timedelta(days=i) for i in range((fin - inicio).days + 1)]
        id_to_name = {emp['employee_id']: emp['nombre'] for emp in employees_list}

        # {(ubicacion, caseta, horario, entrada, salida): {"fecha": [guardias], "dias_libres": [[...], ...]}}
        turnos = {}
        for emp_id, registros in data.items():
            for reg in registros:
                fecha_inicio = reg.get('fecha_inicio_turno')
                if not fecha_inicio:
                    continue
                key = (
                    reg.get('ubicacion') or 'Sin ubicación',
                    reg.get('caseta') or 'Sin caseta',
                    reg.get('nombre_horario') or 'Sin horario',
                    reg.get('hora_entrada') or '',
                    reg.get('hora_salida') or '',
                )
                turno = turnos.setdefault(key, {"guardias_por_fecha": {}, "dias_libres": {}})
                turno["dias_libres"][emp_id] = reg.get('dias_libres_empleado') or []
                turno["guardias_por_fecha"].setdefault(fecha_inicio[:10], []).append({
                    "employee_id": emp_id,
                    "nombre": id_to_name.get(emp_id, str(emp_id)),
                    "status": reg.get('status_turn', ''),
                    "fecha_inicio": fecha_inicio,
                    "fecha_cierre": reg.get('fecha_cierre_turno'),
                    "horas_trabajadas": reg.get('horas_trabajadas'),
                    "tolerancia_retardo": reg.get('tolerancia_retardo'),
                    "cierre_automatico": (reg.get('comentario_cierre') or '') if (reg.get('comentario_cierre') or '').startswith("Cierre de turno automatico") else None,
                })

        result = []
        for (ubicacion, caseta, horario, entrada, salida), turno in turnos.items():
            # Guardias sin días libres configurados no deciden si el día es laboral, salvo que ninguno los tenga
            dias_libres = [libres for libres in turno["dias_libres"].values() if libres]
            dias = []
            resumen = {"programados": 0, "a_tiempo": 0, "tarde": 0, "sin_abrir": 0, "sin_cierre": 0, "en_curso": 0}
            for fecha_dia in fechas:
                fecha = fecha_dia.strftime('%Y-%m-%d')
                guardias = sorted(turno["guardias_por_fecha"].get(fecha, []), key=lambda g: g["fecha_inicio"])
                dia_es = dias_semana[fecha_dia.weekday()]
                # Día laboral si al menos uno de los guardias que cubren el turno no descansa ese día
                laboral = not dias_libres or any(dia_es not in libres for libres in dias_libres)
                if guardias:
                    abrio = guardias[0]
                    if not abrio["fecha_cierre"]:
                        status = "en_curso" if fecha_dia == hoy else "sin_cierre"
                    elif abrio["status"] in ("retardo", "falta_por_retardo"):
                        status = "tarde"
                    else:
                        status = "a_tiempo"
                elif fecha_dia > hoy:
                    status = "pendiente"
                elif not laboral:
                    status = "descanso"
                elif fecha_dia == hoy and entrada and hora_actual < entrada:
                    status = "por_abrir"
                else:
                    status = "sin_abrir"

                if status in ("a_tiempo", "tarde", "sin_cierre", "en_curso", "sin_abrir"):
                    resumen["programados"] += 1
                if status in resumen:
                    resumen[status] += 1
                dias.append({"fecha": fecha, "status": status, "guardias": guardias})

            result.append({
                "ubicacion": ubicacion,
                "caseta": caseta,
                "horario": horario,
                "hora_entrada": entrada,
                "hora_salida": salida,
                "dias": dias,
                "resumen": resumen,
            })

        result.sort(key=lambda t: (t["ubicacion"], t["caseta"], t["hora_entrada"], t["horario"]))
        return result

    def new_resumen(self):
        return {"asistencias": 0, "retardos": 0, "faltas": 0, "sin_salida": 0, "vacaciones": 0, "incapacidades": 0, "permisos": 0, "fuera_oficina": 0}

    def sumar_resumen(self, resumen, status):
        conteo = {
            "presente": "asistencias", "en_turno": "asistencias",
            "retardo": "retardos",
            "falta": "faltas",
            "sin_salida": "sin_salida",
            "vacaciones": "vacaciones",
            "incapacidad": "incapacidades",
            "permiso": "permisos",
            "home_office": "fuera_oficina",
            "trabajo_fuera": "fuera_oficina",
        }.get(status)
        if conteo:
            resumen[conteo] += 1

    def format_employees_attendance(self, data, employees_list, locations=[], date_from=None, date_to=None, free_days={}):
        ahora = datetime.now(timezone('America/Mexico_City'))
        hoy = ahora.date()
        hora_actual = ahora.strftime('%H:%M:%S')
        # Tipos de FORMATO_VACACIONES agrupados en los estatus del reporte
        tipo_dia_libre_status = {
            "vacaciones": "vacaciones",
            "enfermedad": "incapacidad",
            "accidente": "incapacidad",
            "días_personales": "permiso",
            "vuelta_personal": "permiso",
            "home_office": "home_office",
            "trabajo_fuera_de_oficina": "trabajo_fuera",
        }
        # Con estos tipos, si ese día hay check-in manda el check-in (la persona sí trabajó)
        tipos_con_checkin = ("home_office", "trabajo_fuera_de_oficina")
        dias_semana = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]
        inicio = datetime.strptime(date_from, '%Y-%m-%d').date()
        fin = datetime.strptime(date_to, '%Y-%m-%d').date()
        fechas = [inicio + timedelta(days=i) for i in range((fin - inicio).days + 1)]
        horarios = self.get_schedules()
        empleados = {emp['employee_id']: emp for emp in employees_list if emp.get('employee_id')}

        # Empleados sin check-in en el periodo también salen (solo cuando no se filtra por ubicación)
        if not locations:
            for emp_id in empleados:
                data.setdefault(emp_id, [])

        result = []
        for emp_id, registros in data.items():
            emp = empleados.get(emp_id, {})

            # Agrupar registros por ubicación; sin registros, la ubicación configurada del empleado
            ubicaciones = {}
            for reg in registros:
                # format_cr deja la ubicación del catálogo como 'ubicacion' (mismo id que incidente_location)
                location = reg.get('ubicacion') or 'Sin ubicación'
                ubicaciones.setdefault(location, []).append(reg)
            if not ubicaciones:
                ubicaciones[(emp.get('ubicaciones') or ['Sin ubicación'])[0]] = []

            # Días libres solicitados (vacaciones, permisos, home office, etc.)
            dias_solicitados_info = {}
            for solicitud in free_days.get(emp_id, []):
                current_date = datetime.strptime(solicitud['fecha_inicio'], '%Y-%m-%d')
                fecha_fin = datetime.strptime(solicitud['fecha_fin'], '%Y-%m-%d')
                while current_date <= fecha_fin:
                    dias_solicitados_info[current_date.strftime('%Y-%m-%d')] = solicitud
                    current_date += timedelta(days=1)

            for location, regs in ubicaciones.items():
                # Mapeo status por día
                dias_info = {}
                dias_libres = []
                jornada = None
                for reg in regs:
                    fecha_inicio = reg.get('fecha_inicio_turno')
                    fecha_cierre = reg.get('fecha_cierre_turno')
                    if fecha_inicio:
                        dias_info[fecha_inicio[:10]] = {
                            "status": reg.get('status_turn', ''),
                            "fecha_inicio": fecha_inicio,
                            "fecha_cierre": fecha_cierre,
                            "closed": bool(fecha_cierre),
                            "comentario_cierre": reg.get('comentario_cierre') or '',
                        }
                    if reg.get('dias_libres_empleado'):
                        #! POSIBLE CAMBIO: Como considerariamos un cambio en los dias libres a mitad de mes?
                        dias_libres = reg['dias_libres_empleado']
                    if not jornada and reg.get('nombre_horario'):
                        # Los registros vienen del más reciente al más antiguo
                        jornada = {
                            "nombre": reg.get('nombre_horario'),
                            "hora_entrada": reg.get('hora_entrada', ''),
                            "hora_salida": reg.get('hora_salida', ''),
                            "tolerancia_retardo": reg.get('tolerancia_retardo'),
                        }
                # Sin datos en el check-in se usa la configuración del empleado y los horarios de su ubicación
                dias_libres = dias_libres or emp.get('dias_libres') or []
                jornada = jornada or self.employee_schedule([location] + (emp.get('ubicaciones') or []), horarios)
                limite_llegada = None
                if jornada and jornada.get('hora_entrada'):
                    entrada = datetime.strptime(jornada['hora_entrada'][:8], '%H:%M:%S')
                    limite_llegada = (entrada + timedelta(minutes=int(jornada.get('tolerancia_retardo') or 0))).strftime('%H:%M:%S')

                asistencia_mes = []
                resumen = self.new_resumen()
                for fecha_dia in fechas:
                    fecha = fecha_dia.strftime('%Y-%m-%d')
                    es_hoy = fecha_dia == hoy
                    dia_es = dias_semana[fecha_dia.weekday()]

                    fecha_inicio = None
                    fecha_cierre = None
                    closed = False
                    comentario_cierre = ""
                    tipo_dia_libre = None

                    solicitud = dias_solicitados_info.get(fecha)
                    # Permisos por horas y home office no tapan un check-in de ese día
                    manda_checkin = solicitud and fecha in dias_info and (
                        solicitud.get('parcial') or solicitud.get('tipo') in tipos_con_checkin
                    )

                    # El descanso gana; luego la solicitud autorizada; luego el check-in
                    if dias_libres and dia_es in dias_libres:
                        status = "dia_libre"
                    elif solicitud and not manda_checkin:
                        tipo_dia_libre = solicitud.get('tipo')
                        status = tipo_dia_libre_status.get(tipo_dia_libre, tipo_dia_libre)
                    elif fecha in dias_info:
                        info = dias_info[fecha]
                        status = info["status"]
                        fecha_inicio = info.get("fecha_inicio")
                        fecha_cierre = info.get("fecha_cierre")
                        closed = info.get("closed", False)
                        comentario_cierre = info.get("comentario_cierre", "")
                        if status == "falta_por_retardo":
                            status = "falta"
                        if not closed:
                            if not es_hoy:
                                status = "sin_salida"
                            elif status == "presente":
                                status = "en_turno"
                    elif fecha_dia < hoy:
                        status = "falta"
                    elif es_hoy:
                        # Pasada la hora de entrada más la tolerancia de su jornada ya es falta
                        status = "falta" if limite_llegada and hora_actual > limite_llegada else "por_llegar"
                    else:
                        status = "sin_registro"

                    self.sumar_resumen(resumen, status)

                    asistencia_data = {
                        "fecha": fecha,
                        "dia": fecha_dia.day,
                        "status": status,
                    }
                    if tipo_dia_libre:
                        asistencia_data["tipo_dia_libre"] = tipo_dia_libre
                    if solicitud:
                        asistencia_data["solicitud"] = solicitud
                    if fecha_inicio:
                        asistencia_data["fecha_inicio"] = fecha_inicio
                    if fecha_cierre:
                        asistencia_data["fecha_cierre"] = fecha_cierre
                    if closed:
                        asistencia_data["closed"] = True
                    if comentario_cierre.startswith("Cierre de turno automatico"):
                        asistencia_data["cierre_automatico"] = comentario_cierre

                    asistencia_mes.append(asistencia_data)

                result.append({
                    "employee_id": emp_id,
                    "nombre": emp.get('nombre', ''),
                    "departamento": emp.get('departamento', ''),
                    "puesto": emp.get('puesto', ''),
                    "ubicacion": location,
                    "jornada": jornada,
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

    def get_attendance_day_detail(self, user_id=None, fecha=None):
        """Registro de asistencia de un empleado en una fecha (YYYY-MM-DD) para el panel de detalle."""
        conf = self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID
        query = [
            {"$match": {
                "deleted_at": {"$exists": False},
                "form_id": self.REGISTRO_ASISTENCIA,
                "created_by_id": user_id,
                f"answers.{self.f['start_shift']}": {"$gte": fecha, "$lte": f"{fecha} 23:59:59"},
            }},
            {"$sort": {"created_at": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "folio": 1,
                "fecha_inicio": f"$answers.{self.f['start_shift']}",
                "fecha_cierre": f"$answers.{self.f['end_shift']}",
                "status": f"$answers.{self.f['status_turn']}",
                "nombre_horario": f"$answers.{self.f['nombre_horario']}",
                "hora_entrada": f"$answers.{self.f['hora_entrada']}",
                "hora_salida": f"$answers.{self.f['hora_salida']}",
                "tolerancia_retardo": f"$answers.{self.f['tolerancia_retardo']}",
                "horas_trabajadas": f"$answers.{self.f['horas_trabajadas']}",
                "foto_inicio": f"$answers.{self.f['foto_inicio_turno']}",
                "foto_cierre": f"$answers.{self.f['foto_cierre_turno']}",
                "comentario_inicio": f"$answers.{self.f['comentario_inicio_turno']}",
                "comentario_cierre": f"$answers.{self.f['comentario_cierre_turno']}",
                "ubicacion": f"$answers.{conf}.{self.Location.f['location']}",
                "area": f"$answers.{conf}.{self.Location.f['area']}",
            }},
        ]
        response = list(self.cr.aggregate(query))
        return response[0] if response else {}

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

    def get_attendance_data(self, locations=[], limit=100, offset=0):
        """
        Se obtiene la informacion de la asistencia de los empleados en forma Registro de Asistencia
        """
        custom_match_query = {
            "deleted_at": {"$exists": False},
            "form_id": self.REGISTRO_ASISTENCIA,
        }
        
        if locations:
            custom_match_query.update({
                f"answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": locations}
            })

        query = [
            {"$match": custom_match_query},
            {"$project": {
                "_id": 1,
                "folio": 1,
                "attendance_status": f"$answers.{self.f['status_turn']}",
                "attendance_start_pic": f"$answers.{self.f['foto_inicio_turno']}",
                "attendance_end_pic": f"$answers.{self.f['foto_cierre_turno']}",
                "attendance_name": "$created_by_name",
                "attendance_sup_name": f"$answers.{self.f['nombre_guardia_suplente']}",
                "attendance_horario": f"$answers.{self.f['nombre_horario']}",
                "attendance_position": f"$answers.{self.f['tipo_guardia']}",
                "attendance_location": f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['location']}",
                "attendance_area": f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.Location.f['area']}",
                "attendance_start_time": f"$answers.{self.f['fecha_inicio_turno']}",
                "attendance_end_time": f"$answers.{self.f['fecha_cierre_turno']}",
                "attendance_start_comment": f"$answers.{self.f['comentario_inicio_turno']}",
                "attendance_end_comment": f"$answers.{self.f['comentario_cierre_turno']}",
                "attendance_work_hours": f"$answers.{self.f['horas_trabajadas']}",
            }}
        ]

        count_query = [
            {"$match": custom_match_query},
            {"$count": "total"}
        ]
        
        count_result = self.format_cr(self.cr.aggregate(count_query))
        total_count = count_result[0]['total'] if count_result else 0
        current_page = (offset // limit) + 1 if limit else 1
        total_pages = ceil(total_count / limit) if limit else 1

        query.append({'$skip': offset})
        query.append({'$limit': limit})

        data = self.format_cr(self.cr.aggregate(query))
        format_data = []
        if data:
            for item in data:
                format_data.append({
                    **item,
                    "attendance_start_pic": self.unlist(item.get("attendance_start_pic", [])),
                    "attendance_end_pic": self.unlist(item.get("attendance_end_pic", [])),
                    "attendance_status": item.get("attendance_status", "").replace("_", " ").capitalize(),
                    "attendance_position": item.get("attendance_position", "").replace("_", " ").capitalize(),
                })
        
        return {
            "records": format_data,
            "total_records": total_count,
            "total_pages": total_pages,
            "actual_page": current_page,
            "records_on_page": len(format_data)
        }

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
    date_from = data.get('date_from')
    date_to = data.get('date_to')
    month = data.get('month', 1)
    year = data.get('year', 2026)
    limit = data.get('limit', 100)
    offset = data.get('offset', 0)

    response = {}
    if option == 'get_report':
        response = script_obj.get_employees_attendance(group_by=group_by, locations=locations, month=month, year=year, date_from=date_from, date_to=date_to)
    elif option == 'get_attendance_data':
        response = script_obj.get_attendance_data(locations=locations, limit=limit, offset=offset)
    elif option == 'get_locations':
        response = script_obj.get_locations()
    elif option == 'get_attendance_day_detail':
        response = script_obj.get_attendance_day_detail(user_id=data.get('user_id'), fecha=data.get('fecha'))
    elif option == 'get_guard_turn_details':
        response = script_obj.get_guard_turn_details(user_ids=user_ids, selected_day=selected_day, location=location)

    print(simplejson.dumps(response, indent=4))
    script_obj.HttpResponse({"data": response})

