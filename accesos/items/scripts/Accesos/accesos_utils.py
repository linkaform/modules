# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from linkaform_api import base
from lkf_addons.addons.accesos.app import Accesos
import sys, simplejson, json, pytz
import pytz


class Accesos( Accesos):
    print('Entra a acceos utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.f.update({
            'duracion_rondin':'6639b47565d8e5c06fe97cf3',
            'duracion_traslado_area':'6760a9581e31b10a38a22f1f',
            'fecha_inspeccion_area':'6760a908a43b1b0e41abad6b',
            'fecha_inicio_rondin':'6760a8e68cef14ecd7f8b6fe',
            'status_rondin':'6639b2744bb44059fc59eb62',
            'grupo_areas_visitadas':'66462aa5d4a4af2eea07e0d1',
            'nombre_recorrido':'6644fb97e14dcb705407e0ef',
            
            'option_checkin': '663bffc28d00553254f274e0',
            'image_checkin': '6855e761adab5d93274da7d7',
            'comment_checkin': '66a5b9bed0c44910177eb724',
            'comment_checkout': '68798dd1205f333d8f53a1c7',
            'start_shift': '6879828d0234f02649cad390',
            'end_shift': '6879828d0234f02649cad391',
            'foto_end': '6879823d856f580aa0e05a3b',
            
            'dias_libres': '68bb20095035e61c5745de05',
            'nombre_horario': '68b6427cc8f94827ebfed695',
            'hora_entrada': '68b6427cc8f94827ebfed696',
            'hora_salida': '68b6427cc8f94827ebfed697',
            'tolerancia_retardo': '68b6427cc8f94827ebfed698',
            'retardo_maximo': '68b642e2bc17e2713cabe019',
            'grupo_turnos': '68b6427cc8f94827ebfed699',
            'horas_trabajadas': '68d6b0d5f7865907a86c37d7',
            'status_turn': '68d5bbb57691dec5a7640358',
            
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
            'duracion_estimada': '6854459836ea891d9d2be7d9',
            
            'grupo_comentarios_generales': '6927a0cdc03f0f8e5355437a',
            'grupo_comentarios_generales_fecha': '6927a0ea1c378cbd7f60a135',
            'grupo_comentarios_generales_texto': '6927a0ea1c378cbd7f60a136',
            'nombre_suplente': '6927a1176c60848998a157a2',
            'documento_check': '692a1b4e005c84ce5cd5167f'
        })

        #BORRAR
        self.CONFIGURACION_RECORRIDOS = self.lkm.catalog_id('configuracion_de_recorridos')
        self.CONFIGURACION_RECORRIDOS_ID = self.CONFIGURACION_RECORRIDOS.get('id')
        self.CONFIGURACION_RECORRIDOS_OBJ_ID = self.CONFIGURACION_RECORRIDOS.get('obj_id')
        self.REGISTRO_ASISTENCIA = self.lkm.form_id('registro_de_asistencia','id')

        # self.bitacora_fields.update({
        #     "catalogo_pase_entrada": "66a83ad652d2643c97489d31",
        #     "gafete_catalog": "66a83ace56d1e741159ce114"
        # })

        # self.consecionados_fields.update({
        #     "catalogo_ubicacion_concesion": "66a83a74de752e12018fbc3c",
        # })

        self.CONFIGURACION_DE_RECORRIDOS_FORM = self.lkm.form_id('configuracion_de_recorridos','id')

        self.f.update({
            'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            'comentario_area_rondin': '66462b9d7124d1540f962088',
            'comentario_check_area': '681144fb0d423e25b42818d4',
            'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            'fecha_programacion':'6760a8e68cef14ecd7f8b6fe',
            'foto_evidencia_area': '681144fb0d423e25b42818d2',
            'foto_evidencia_area_rondin': '66462b9d7124d1540f962087',
            'grupo_de_areas_recorrido': '6645052ef8bc829a5ccafaf5',
            'nombre_area':'663e5d44f5b8a7ce8211ed0f',
            'nombre_del_recorrido': '6645050d873fc2d733961eba',
            'nombre_del_recorrido_en_catalog': '6644fb97e14dcb705407e0ef',
            'ubicacion_recorrido': '663e5c57f5b8a7ce8211ed0b',
            'fecha_inicio_rondin': '6818ea068a7f3446f1bae3b3',
            'fecha_fin_rondin': '6760a8e68cef14ecd7f8b6ff',
            'check_status': '681fa6a8d916c74b691e174b',
            'grupo_incidencias_check': '681144fb0d423e25b42818d3',
            'incidente_open': '6811455664dc22ecae83f75b',
            'incidente_comentario': '681145323d9b5fa2e16e35cc',
            'incidente_area': '663e5d44f5b8a7ce8211ed0f',
            'incidente_location': '663e5c57f5b8a7ce8211ed0b',
            'incidente_evidencia': '681145323d9b5fa2e16e35cd',
            'incidente_documento': '685063ba36910b2da9952697',
            'url_registro_rondin': '6750adb2936622aecd075607',
            'bitacora_rondin_incidencias': '686468a637d014b9e0ab5090',
            'tipo_de_incidencia': '663973809fa65cafa759eb97'
        })
        
        self.checkin_fields.update({
            'nombre_suplente':'6927a1176c60848998a157a2'
        })

    def set_boot_status(self, checkin_type):
        if checkin_type == 'in':
            set_boot_status = 'abierta'
        elif checkin_type == 'out':
            set_boot_status = 'cerrada'
        return set_boot_status

    def is_boot_available(self, location, area):
        self.last_check_in = self.get_last_checkin(location, area)
        last_status = self.last_check_in.get('checkin_type')
        if last_status in ['entrada','apertura', 'disponible', 'abierta']:
            return False, last_status
        else:
            return True, last_status

    def get_guard_last_checkin(self, user_ids):
        '''
            Se realiza busqued del ulisto registro de checkin de un usuario
        '''
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.CHECKIN_CASETAS,
            }
        unwind_query = {
            f"answers.{self.f['guard_group']}.{self.checkin_fields['checkin_status']}": "entrada"
        }
        if user_ids and type(user_ids) == list:
            if len(user_ids) == 1:
                #hace la busqueda por directa, para optimizar recuros
                user_ids = user_ids[0]
            else:
                #hace busqueda en lista de opciones
                match_query.update({
                    f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['user_id_jefes']}":{'$in':user_ids}
                    })
        if user_ids and type(user_ids) == int:
            unwind_query.update({
                f"answers.{self.f['guard_group']}.{self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID}.{self.f['user_id_jefes']}":user_ids
                })
        if not unwind_query:
            return self.LKFException({"msg":f"Algo salio mal al intentar buscar el checkin del los ids: {user_id}"})
        query = [
            {'$match': match_query },
            {'$unwind': f"$answers.{self.f['guard_group']}"},
            {'$match':unwind_query},
            {'$project': self.project_format(self.checkin_fields)},
            {'$sort':{'created_at':-1}},
            {'$limit':1}
            ]
        return self.format_cr_result(self.cr.aggregate(query), get_one=True)

    def get_booth_status(self, booth_area, location):
        last_chekin = self.get_last_checkin(location, booth_area)
        booth_status = {
            "status":'Cerrada',
            "guard_on_dutty":'',
            "user_id":'',
            "stated_at":'',
            "fotografia_inicio_turno":[],
            "fotografia_cierre_turno":[],
            }
        if last_chekin.get('checkin_type') in ['entrada','apertura','disponible', 'abierta']:
            #todo
            #user_id 
            booth_status['status'] = 'Abierta'
            booth_status['guard_on_dutty'] = last_chekin.get('employee') 
            booth_status['stated_at'] = last_chekin.get('boot_checkin_date')
            booth_status['checkin_id'] = last_chekin['_id']
            booth_status['fotografia_inicio_turno'] = last_chekin.get('fotografia_inicio_turno',[]) 
            booth_status['fotografia_cierre_turno'] = last_chekin.get('fotografia_cierre_turno',[]) 
        return booth_status

    def get_shift_data(self, booth_location=None, booth_area=None, search_default=True):
        """
        Obtiene informacion del turno del usuario logeado
        """

        load_shift_json = { }
        username = self.user.get('username')
        user_id = self.user.get('user_id')
        config_accesos_user="" #get_config_accesos(user_id)
        user_status = self.get_employee_checkin_status(user_id, as_shift=True,  available=False)
        this_user = user_status.get(user_id)
        if not this_user:
            this_user =  self.get_employee_data(email=self.user.get('email'), get_one=True)
            this_user['name'] = this_user.get('worker_name','')
        user_booths = []
        guards_positions = self.config_get_guards_positions()
        if not guards_positions:
            self.LKFException({"status_code":400, "msg":'No Existen puestos de guardias configurados.'})

        if this_user and this_user.get('status') == 'out':
            check_aux_guard = self.check_in_aux_guard()

            for user_id_aux, each_user in check_aux_guard.items():
                if user_id_aux == user_id:
                    this_user = each_user
                    this_user['status'] = 'in' if each_user.get('status') == 'in' else 'out'
                    this_user['location'] = each_user.get('location')
                    this_user['area'] = each_user.get('area')
                    this_user['checkin_date'] = each_user.get('checkin_date')
                    this_user['checkout_date'] = each_user.get('checkout_date')
                    this_user['checkin_position'] = each_user.get('checkin_position')

        if this_user and this_user.get('status') == 'in':
            location_employees = {self.chife_guard:{},self.support_guard:[]}
            booth_area = this_user['area']
            booth_location = this_user['location']
            for u_id, each_user in user_status.items():
                if u_id == user_id:
                    location_employees[self.support_guard].append(each_user)
                    guard = each_user
                else:
                    if each_user.get('status') == 'in':
                        location_employees[self.support_guard].append(each_user)
        else:
            # location_employees = {}
            default_booth , user_booths = self.get_user_booth(search_default=False)
            # location = default_booth.get('location')
            if not booth_location:
                booth_area = default_booth.get('area')
            if not booth_location:
                booth_location = default_booth.get('location')
            if not default_booth:
                return self.LKFException({"status_code":400, "msg":'No booth found or configure for user'})
            location_employees = self.get_booths_guards(booth_location, booth_area, solo_disponibles=True)
            guard = self.get_user_guards(location_employees=location_employees)
            if not guard:
                return self.LKFException({
                    "status_code":400, 
                    "msg":f"Usuario {self.user['user_id']} no confgurado como guardia, favor de revisar su configuracion."}) 
        location_employees = self.set_employee_pic(location_employees)
        support_guards = location_employees.get('guardia_de_apoyo', [])
        user_id = self.user.get('user_id')
        for idx, guard in enumerate(support_guards):
            if guard.get('user_id') == user_id:
                support_guards.pop(idx)
                break
        location_employees['guardia_de_apoyo'] = support_guards
        booth_address = self.get_area_address(booth_location, booth_area)
        notes = self.get_list_notes(booth_location, booth_area, status='abierto')
        load_shift_json["location"] = {
            "name":  booth_location,
            "area": booth_area,
            "city": booth_address.get('city'),
            "state": booth_address.get('state'),
            "address": booth_address.get('address'),
            }
        # guards_online = self.get_guards_booths(booth_location, booth_area)
        load_shift_json["booth_stats"] = self.get_page_stats( booth_area, booth_location, "Turnos")
        load_shift_json["booth_status"] = self.get_booth_status(booth_area, booth_location)
        # load_shift_json["support_guards"] = location_employees[self.support_guard]
        load_shift_json["support_guards"] = location_employees.get(self.support_guard, "")
        load_shift_json["guard"] = self.update_guard_status(guard, this_user)
        load_shift_json["notes"] = notes
        load_shift_json["user_booths"] = user_booths
        load_shift_json['config_accesos_user']=config_accesos_user
        # load_shift_json["guards_online"] = guards_online
        print(simplejson.dumps(load_shift_json, indent=4))
        return load_shift_json


    def do_checkin(self, location, area, employee_list=[], fotografia=[], check_in_manual={}, nombre_suplente="", checkin_id=""):
        # Realiza el check-in en una ubicación y área específica.
        resp, last_status = self.is_boot_available(location, area)
        if not resp and last_status == 'abierta':
            user = self.lkf_api.get_user_by_id(self.user.get('user_id'))
            res = self.update_guards_checkin([{'user_id': self.user.get('user_id'), 'name': user.get('name', '')}], checkin_id, location, area)
            format_res = self.unlist(res)
            if format_res.get('status_code') in [200, 201, 202]:
                return format_res
            else:
                self.LKFException({'title': 'Error al hacer check-in', 'msg': format_res.get('json')})
        if employee_list:
            user_id = [self.user.get('user_id'),] + [x['user_id'] for x in employee_list]
        else:
            user_id = self.user.get('user_id')
        boot_config = self.get_users_by_location_area(
            location_name=location, 
            area_name=area, 
            user_id=user_id)
        if not boot_config:
            msg = f"User can not login to this area : {area} at location: {location} ."
            msg += f"Please check your configuration."
            self.LKFException(msg)
        else:
            allowed_users = [x['user_id'] for x in boot_config]
            if type(user_id) == int:
                user_id=[user_id]
            common_values = list(set(user_id) & set(allowed_users))
            not_allowed = [value for value in user_id if value not in common_values]
        if not_allowed:
            msg = f"Usuarios con ids {not_allowed}. "
            msg += f"No estan permitidos de hacer checking en esta area : {area} de la ubicacion {location} ."
            self.LKFException({'msg':msg,"title":'Error de Configuracion'})

        validate_status = self.get_employee_checkin_status(user_id)
        not_allowed = [uid for uid, u_data in validate_status.items() if u_data['status'] =='in']
        if not_allowed:
            msg = f"El usuario(s) con ids {not_allowed}. Se encuentran actualmente logeado en otra caseta."
            msg += f"Es necesario primero salirse de cualquier caseta antes de querer entrar a una casta"
            self.LKFException({'msg':msg,"title":'Accion Requerida!!!'})

        employee = self.get_employee_data(email=self.user.get('email'), get_one=True)
        if not employee:
            msg = f"Ningun empleado encontrado con email: {self.user.get('email')}"
            self.LKFException(msg)
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        employee['timezone'] = user_data.get('timezone','America/Monterrey')
        employee['name'] = employee['worker_name']
        employee['position'] = self.chife_guard
        employee['nombre_suplente'] = nombre_suplente

        timezone = employee.get('cat_timezone', employee.get('timezone', 'America/Monterrey'))
        data = self.lkf_api.get_metadata(self.CHECKIN_CASETAS)
        now_datetime =self.today_str(timezone, date_format='datetime')
        checkin = self.checkin_data(employee, location, area, 'in', now_datetime)
        employee_list.insert(0,employee)

        checkin = self.check_in_out_employees('in', now_datetime, checkin=checkin, employee_list=employee_list)

        data.update({
                'properties': {
                    "device_properties":{
                        "system": "Modulo Accesos",
                        "process": 'Checkin-Checkout',
                        "action": 'do_checkin',
                        "archive": "accesos_utils.py"
                    }
                },
                'answers': checkin
            })
        if check_in_manual:
            checkin.update({
                self.checkin_fields['checkin_image']: check_in_manual.get('image', []),
                self.checkin_fields['commentario_checkin_caseta']: check_in_manual.get('comment', '')
            })
        if fotografia:
            checkin.update({
                self.checkin_fields['fotografia_inicio_turno']: fotografia
            })

        resp_create = self.lkf_api.post_forms_answers(data)
        #TODO agregar nombre del Guardia Quien hizo el checkin
        if resp_create.get('status_code') == 201:
            resp_create['json'].update({'boot_status':{'guard_on_duty':user_data['name']}})
            asistencia_answers = {
                self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID: {
                    self.Location.f['location']: location,
                    self.Location.f['area']: area
                },
                self.f['tipo_guardia']: 'guardia_regular',
                self.checkin_fields['checkin_type']: 'iniciar_turno',
                self.f['image_checkin']: fotografia
            }

            if nombre_suplente:
                asistencia_answers.update({
                    self.f['tipo_guardia']: 'guardia_suplente',
                    self.f['nombre_guardia_suplente']: nombre_suplente
                })

            metadata = self.lkf_api.get_metadata(form_id=self.REGISTRO_ASISTENCIA)
            metadata.update({
                "properties": {
                    "device_properties":{
                        "System": "Script",
                        "Module": 'Accesos',
                        "Process": 'Inicio de turno',
                        "Action": 'asistencia',
                        "File": 'accesos/app.py',
                    }
                },
            })
            metadata.update({'answers':asistencia_answers})
            response = self.lkf_api.post_forms_answers(metadata)
            if response.get('status_code') in [200, 201, 202]:
                resp_create.update({'registro_de_asistencia': 'Correcto'})
            else:
                resp_create.update({'registro_de_asistencia': 'Error'})
        return resp_create

    def do_checkout(self, checkin_id=None, location=None, area=None, guards=[], forzar=False, comments=False, fotografia=[]):
        # self.get_answer(keys)
        employee =  self.get_employee_data(email=self.user.get('email'), get_one=True)
        timezone = employee.get('cat_timezone', employee.get('timezone', 'America/Monterrey'))
        now_datetime =self.today_str(timezone, date_format='datetime')
        last_chekin = {}
        if not checkin_id:
            if guards:
                last_chekin = self.get_guard_last_checkin(guards)
            elif location or area:
                last_chekin = self.get_last_checkin(location, area)
            checkin_id = last_chekin.get('_id')
        if not checkin_id:
            self.LKFException({
                "msg":"No encontramos un checking valido del cual podemos hacer checkout...", 
                "title":"Una Disculpa!!!"})
        record = self.get_record_by_id(checkin_id)
        checkin_answers = record['answers']
        folio = record['folio']
        area = checkin_answers.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID,{}).get(self.f['area'])
        location = checkin_answers.get(self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID,{}).get(self.f['location'])
        rec_guards = checkin_answers.get(self.checkin_fields['guard_group'])
        
        guards_in = [idx for idx, guard in enumerate(rec_guards) if not guard.get(self.checkin_fields['checkout_date'])]
        guards_ids = []
        for guard in rec_guards:
            fecha_cierre_turno = guard.get(self.checkin_fields['checkout_date'])
            guard_id = self.unlist(guard.get(self.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID, {}).get(self.employee_fields['user_id_jefes']))
            actual_guard_id = self.unlist(employee.get('usuario_id'))
            guards_ids.append(guard_id)
            if not fecha_cierre_turno and len(guards_in) > 1 and guard_id == actual_guard_id:
                resp = self.do_checkout_aux_guard(guards=[actual_guard_id], location=location, area=area)
                return resp

        if not guards:
            checkin_answers[self.checkin_fields['commentario_checkin_caseta']] = \
                checkin_answers.get(self.checkin_fields['commentario_checkin_caseta'],'')
            # Si no especifica guardas va a cerrar toda la casta
            checkin_answers[self.checkin_fields['checkin_type']] = 'cerrada'
            checkin_answers[self.checkin_fields['boot_checkout_date']] = now_datetime
            checkin_answers[self.checkin_fields['forzar_cierre']] = 'regular'
            if comments:
                checkin_answers[self.checkin_fields['commentario_checkin_caseta']] += comments + ' '
            if forzar:
                checkin_answers[self.checkin_fields['commentario_checkin_caseta']] += f"Cerrado por: {employee.get('worker_name')}"
                checkin_answers[self.checkin_fields['forzar_cierre']] = 'forzar'
        resp, last_status = self.is_boot_available(location, area)
        if resp:
            msg = f"Can not make a CHEKOUT on a boot that hasn't checkin. Location: {location} at the area {area}."
            msg += f"You need to checkin first."
            self.LKFException(msg)
        if not checkin_id:
            msg = f"No checking found for this  Location: {location} at the area {area}."
            msg += f"You need to checkin first."
            self.LKFException(msg)

        data = self.lkf_api.get_metadata(self.CHECKIN_CASETAS)
        checkin_answers = self.check_in_out_employees('out', now_datetime, checkin=checkin_answers, employee_list=guards)
        # response = self.lkf_api.patch_multi_record( answers=checkin, form_id=self.CHECKIN_CASETAS, folios=[folio,])
        data['answers'] = checkin_answers

        if fotografia:
            checkin_answers.update({
                self.checkin_fields['fotografia_cierre_turno']: fotografia
            })

        response = self.lkf_api.patch_record( data=data, record_id=checkin_id)
        if response.get('status_code') in [200, 201, 202]:
            if employee:
                record_id = self.search_guard_asistance(location, area, self.unlist(employee.get('usuario_id')))
                asistencia_answers = {
                    self.f['foto_cierre_turno']: fotografia,
                    self.checkin_fields['checkin_type']: 'cerrar_turno',
                }
                res = self.lkf_api.patch_multi_record(answers=asistencia_answers, form_id=self.REGISTRO_ASISTENCIA, record_id=record_id)
                if res.get('status_code') in [200, 201, 202]:
                    response.update({'registro_de_asistencia': 'Correcto'})
                else:
                    response.update({'registro_de_asistencia': 'Error'})
        elif response.get('status_code') == 401:
            return self.LKFException({
                "title":"Error de Configuracion",
                "msg":"El guardia NO tiene permisos sobre el formulario de cierre de casetas"})
        return response

    def get_cantidades_de_pases(self, x_empresa=False):
        print('entra a get_cantidades_de_pases')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        post_project_match = {}

        group_by = {
                '_id':{
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
                }
        
        if x_empresa:
            post_project_match = {
                "$and": [
                    {'empresa': {"$ne": None}},
                    {'empresa': {"$ne": ""}}
                ]
            }

            group_by = {
                '_id':{
                    'empresa':'$empresa',
                    'estatus': '$estatus',
                    },
                'cantidad': {'$sum': 1},
            }

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$match': post_project_match},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    
    def get_cantidades_de_pases_x_persona(self, contratista=None):
        print('entra a get_cantidades_de_pases_x_persona')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PASE_ENTRADA,
        }

        if contratista:
            match_query.update({f"answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}":contratista})

        proyect_fields = {
            '_id':1,
            'folio': f"$folio",
            'estatus':f"$answers.{self.pase_entrada_fields['status_pase']}",
            'empresa': { "$first" : f"$answers.{self.VISITA_AUTORIZADA_CAT_OBJ_ID}.{self.mf['empresa']}"},
            'nombre': f"$answers.{self.mf['nombre_pase']}",
            'nombre_perfil': f"$answers.{self.pase_entrada_fields['nombre_perfil']}",
            'fecha_hasta_pase': f"$answers.{self.pase_entrada_fields['fecha_hasta_pase']}",
            'created_at': 1
        }

        match_query.update({f"answers.{self.pase_entrada_fields['status_pase']}":{'$exists': True}})

        group_by = {
                '_id':{
                    'folio':'$folio',
                    'nombre': '$nombre',
                    'empresa': '$empresa',
                    'nombre_perfil': '$nombre_perfil',
                    'fecha_hasta_pase': '$fecha_hasta_pase',
                    }
                }
        

        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$group': group_by}
        ]

        records = self.format_cr(self.cr.aggregate(query))
        print('/////////records', records)
        return  records
    
    def get_catalogo_paquetes(self):
        catalog_id = self.PROVEEDORES_CAT_ID
        form_id= self.PAQUETERIA
        return self.lkf_api.catalog_view(catalog_id, form_id) 

    def create_paquete(self, data_paquete):
        metadata = self.lkf_api.get_metadata(form_id=self.PAQUETERIA)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de Paquetes",
                    "Action": "nuevo_paquete",
                    "File": "accesos/app.py"
                }
            },
        })
        answers = {}
        for key, value in data_paquete.items():
            if key == 'ubicacion_paqueteria':
                answers[self.UBICACIONES_CAT_OBJ_ID] = { self.mf['ubicacion']: value}
            elif  key == 'area_paqueteria':
                 answers[self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID] = { self.mf['nombre_area']: value}
            elif  key == 'guardado_en_paqueteria':
                answers[self.LOCKERS_CAT_OBJ_ID] ={self.mf['locker_id']:value} 
            elif key == 'proveedor':
                answers[self.PROVEEDORES_CAT_OBJ_ID] = {self.paquetes_fields['proveedor']:value}
            elif key == 'quien_recibe_paqueteria':
                answers[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {self.mf['nombre_empleado']:value}
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        metadata.update({'answers':answers})
        res=self.lkf_api.post_forms_answers(metadata)
        return res

    def update_paquete(self, data_paquete, folio):
        #---Define Answers
        answers = {}
        for key, value in data_paquete.items():
            if  key == 'ubicacion_perdido':
                answers[self.consecionados_fields['ubicacion_catalog_concesion']] = { self.mf['ubicacion']: value}
            elif  key == 'area_paqueteria':
                 answers[self.consecionados_fields['area_catalog_concesion']] = { self.mf['nombre_area_salida']: value}
            elif  key == 'guardado_en_paqueteria':
                answers[self.LOCKERS_CAT_OBJ_ID] ={self.mf['locker_id']:value} 
            elif key == 'proveedor':
                answers[self.PROVEEDORES_CAT_OBJ_ID] = {self.paquetes_fields['proveedor']:value}
            elif key == 'quien_recibe_paqueteria':
                answers[self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID] = {self.mf['nombre_empleado']:value}
            else:
                answers.update({f"{self.paquetes_fields[key]}":value})
        if answers or folio:
            return self.lkf_api.patch_multi_record( answers = answers, form_id=self.PAQUETERIA, folios=[folio])
        else:
            self.LKFException('No se mandarón parametros para actualizar')

   

    def get_list_bitacora2(self, location=None, area=None, prioridades=[], dateFrom='', dateTo='', filterDate=""):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.BITACORA_ACCESOS
        }
        if location:
            match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}":location})
        if area:
            match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}":area})
        if prioridades:
            match_query[f"answers.{self.bitacora_fields['status_visita']}"] = {"$in": prioridades}
  
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        zona = user_data.get('timezone','America/Monterrey')

        if filterDate != "range":
            dateFrom, dateTo = self.get_range_dates(filterDate,zona)

            if dateFrom:
                dateFrom = str(dateFrom)
            if dateTo:
                dateTo = str(dateTo)

        if dateFrom and dateTo:
           match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$gte": dateFrom, "$lte": dateTo},
            })
        elif dateFrom:
            match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$gte": dateFrom}
            })
        elif dateTo:
            match_query.update({
                f"answers.{self.mf['fecha_entrada']}": {"$lte": dateTo}
            })
        
        proyect_fields ={
            '_id': 1,
            'folio': "$folio",
            'created_at': "$created_at",
            'updated_at': "$updated_at",
            'a_quien_visita':f"$answers.{self.CONF_AREA_EMPLEADOS_CAT_OBJ_ID}.{self.mf['nombre_empleado']}",
            'documento': f"$answers.{self.mf['documento']}",
            'caseta_entrada':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}",
            'codigo_qr':f"$answers.{self.mf['codigo_qr']}",
            'comentarios':f"$answers.{self.bitacora_fields['grupo_comentario']}",
            'fecha_salida':f"$answers.{self.mf['fecha_salida']}",
            'fecha_entrada':f"$answers.{self.mf['fecha_entrada']}",
            'foto_url': {"$arrayElemAt": [f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['foto']}.file_url", 0]},
            'equipos':f"$answers.{self.mf['grupo_equipos']}",
            'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",
            'id_gafet': f"$answers.{self.GAFETES_CAT_OBJ_ID}.{self.gafetes_fields['gafete_id']}",
            'id_locker': f"$answers.{self.LOCKERS_CAT_OBJ_ID}.{self.lockers_fields['locker_id']}",
            'identificacion':  {"$first":f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['identificacion']}"},
            'pase_id':{"$toObjectId":f"$answers.{self.mf['codigo_qr']}"},
            'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
            'nombre_area_salida':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.mf['nombre_area_salida']}",
            'nombre_visitante':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_visita']}",
            'contratista':f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['empresa']}",
            'perfil_visita':{'$arrayElemAt': [f"$answers.{self.PASE_ENTRADA_OBJ_ID}.{self.mf['nombre_perfil']}",0]},
            'status_gafete':f"$answers.{self.mf['status_gafete']}",
            'status_visita':f"$answers.{self.mf['tipo_registro']}",
            'ubicacion':f"$answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}",
            'vehiculos':f"$answers.{self.mf['grupo_vehiculos']}",
            'visita_a': f"$answers.{self.mf['grupo_visitados']}"
            }
        lookup = {
         'from': 'form_answer',
         'localField': 'pase_id',
         'foreignField': '_id',
         "pipeline": [
                {'$match':{
                    "deleted_at":{"$exists":False},
                    "form_id": self.PASE_ENTRADA,
                    }
                },
                {'$project':{
                    "_id":0, 
                    'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
                    'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",                    
                    }
                },
                ],
         'as': 'pase',
        }
       
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            {'$lookup': lookup},
        ]
        # if not filterDate:
        #     query.append(
        #         {"$limit":1}
        #     )
        if dateFrom:
            query.append(
                {'$sort':{'folio':-1}},
            )
        else:
            query.append(
                {'$sort':{'folio':-1}},
            )
           
        records = self.format_cr(self.cr.aggregate(query))
        # print( simplejson.dumps(records, indent=4))
        for r in records:
            pase = r.pop('pase')
            r.pop('pase_id')
            if len(pase) > 0 :
                pase = pase[0]
                r['motivo_visita'] = self.unlist(pase.get('motivo_visita',''))
                r['grupo_areas_acceso'] = self._labels_list(pase.get('grupo_areas_acceso',[]), self.mf)
            r['id_gafet'] = r.get('id_gafet','')
            r['status_visita'] = r.get('status_visita','').title().replace('_', ' ')
            r['contratista'] = self.unlist(r.get('contratista',[]))
            r['status_gafete'] = r.get('status_gafete','').title().replace('_', ' ')
            r['documento'] = r.get('documento','')
            r['grupo_areas_acceso'] = self._labels_list(r.pop('grupo_areas_acceso',[]), self.mf)
            r['comentarios'] = self.format_comentarios(r.get('comentarios',[]))
            r['vehiculos'] = self.format_vehiculos(r.get('vehiculos',[]))
            r['equipos'] = self.format_equipos(r.get('equipos',[]))
            r['visita_a'] = self.format_visita(r.get('visita_a',[]))
        return  records

    def get_pdf_seg(self, qr_code, template_id=491, name_pdf='Pase de Entrada'):
        return self.lkf_api.get_pdf_record(qr_code, template_id = template_id, name_pdf =name_pdf, send_url=True)

    def get_list_rondines(self, prioridades=[], dateFrom='', dateTo='', filterDate=""):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.BITACORA_RONDINES
        }
        # if location:
        #     match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['ubicacion']}":location})
        # if area:
        #     match_query.update({f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.mf['nombre_area']}":area})
        # if prioridades:
        #     match_query[f"answers.{self.bitacora_fields['status_visita']}"] = {"$in": prioridades}
  
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        zona = user_data.get('timezone','America/Monterrey')

        if filterDate != "range":
            dateFrom, dateTo = self.get_range_dates(filterDate,zona)

            if dateFrom:
                dateFrom = str(dateFrom)
            if dateTo:
                dateTo = str(dateTo)

        if dateFrom and dateTo:
           match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$gte": dateFrom, "$lte": dateTo},
            })
        elif dateFrom:
            match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$gte": dateFrom}
            })
        elif dateTo:
            match_query.update({
                f"answers.{self.f['fecha_inicio_rondin']}": {"$lte": dateTo}
            })
        
        proyect_fields ={
            '_id': 1,
            'folio': "$folio",
            'duracion_rondin': f"$answers.{self.f['duracion_rondin']}",
            'duracion_traslado_area':f"$answers.{self.f['duracion_traslado_area']}",
            'fecha_inspeccion_area':f"$answers.{self.f['fecha_inspeccion_area']}",
            'fecha_programacion':f"$answers.{self.f['fecha_programacion']}",
            'fecha_inicio_rondin':f"$answers.{self.f['fecha_inicio_rondin']}",
            'grupo_areas_visitadas':f"$answers.{self.f['grupo_areas_visitadas']}",
            
            # 'areas_del_rondin': '66462aa5d4a4af2eea07e0d1',
            # 'comentario_area_rondin': '66462b9d7124d1540f962088',
            # 'comentario_check_area': '681144fb0d423e25b42818d4',
            # 'estatus_del_recorrido': '6639b2744bb44059fc59eb62',
            # 'fecha_hora_inspeccion_area': '6760a908a43b1b0e41abad6b',
            # 'fecha_programacion':'6760a8e68cef14ecd7f8b6fe',
            # 'foto_evidencia_area': '681144fb0d423e25b42818d2',
            # 'foto_evidencia_area_rondin': '66462b9d7124d1540f962087',
            # 'grupo_de_areas_recorrido': '6645052ef8bc829a5ccafaf5',
            # 'nombre_area':'663e5d44f5b8a7ce8211ed0f',
            # 'nombre_del_recorrido': '6645050d873fc2d733961eba',
            # 'nombre_del_recorrido_en_catalog': '6644fb97e14dcb705407e0ef',
            # 'ubicacion_recorrido': '663e5c57f5b8a7ce8211ed0b',
            # 'fecha_inicio_rondin': '6818ea068a7f3446f1bae3b3',
            # 'fecha_fin_rondin': '6760a8e68cef14ecd7f8b6ff',
            # 'check_status': '681fa6a8d916c74b691e174b',
            # 'grupo_incidencias_check': '681144fb0d423e25b42818d3',
            # 'incidente_open': '6811455664dc22ecae83f75b',
            # 'incidente_comentario': '681145323d9b5fa2e16e35cc',
            # 'incidente_area': '663e5d44f5b8a7ce8211ed0f',
            # 'incidente_location': '663e5c57f5b8a7ce8211ed0b',
            # 'incidente_evidencia': '681145323d9b5fa2e16e35cd',
            # 'incidente_documento': '685063ba36910b2da9952697',
            # 'url_registro_rondin': '6750adb2936622aecd075607',
            # 'bitacora_rondin_incidencias': '686468a637d014b9e0ab5090',
            # 'tipo_de_incidencia': '663973809fa65cafa759eb97'
            }
        # lookup = {
        #  'from': 'form_answer',
        #  'localField': 'pase_id',
        #  'foreignField': '_id',
        #  "pipeline": [
        #         {'$match':{
        #             "deleted_at":{"$exists":False},
        #             "form_id": self.PASE_ENTRADA,
        #             }
        #         },
        #         {'$project':{
        #             "_id":0, 
        #             'motivo_visita':f"$answers.{self.CONFIG_PERFILES_OBJ_ID}.{self.mf['motivo']}",
        #             'grupo_areas_acceso': f"$answers.{self.mf['grupo_areas_acceso']}",                    
        #             }
        #         },
        #         ],
        #  'as': 'pase',
        # }
       
        query = [
            {'$match': match_query },
            {'$project': proyect_fields},
            # {'$lookup': lookup},
        ]
        # if not filterDate:
        #     query.append(
        #         {"$limit":1}
        #     )
        if dateFrom:
            query.append(
                {'$sort':{'folio':-1}},
            )
        else:
            query.append(
                {'$sort':{'folio':-1}},
            )
           
        records = self.format_cr(self.cr.aggregate(query))
        # print( simplejson.dumps(records, indent=4))
        # for r in records:
        #     pase = r.pop('pase')
        #     r.pop('pase_id')
        #     if len(pase) > 0 :
        #         pase = pase[0]
        #         r['motivo_visita'] = self.unlist(pase.get('motivo_visita',''))
        #         r['grupo_areas_acceso'] = self._labels_list(pase.get('grupo_areas_acceso',[]), self.mf)
        #     r['id_gafet'] = r.get('id_gafet','')
        #     r['status_visita'] = r.get('status_visita','').title().replace('_', ' ')
        #     r['contratista'] = self.unlist(r.get('contratista',[]))
        #     r['status_gafete'] = r.get('status_gafete','').title().replace('_', ' ')
        #     r['documento'] = r.get('documento','')
        #     r['grupo_areas_acceso'] = self._labels_list(r.pop('grupo_areas_acceso',[]), self.mf)
        #     r['comentarios'] = self.format_comentarios(r.get('comentarios',[]))
        #     r['vehiculos'] = self.format_vehiculos(r.get('vehiculos',[]))
        #     r['equipos'] = self.format_equipos(r.get('equipos',[]))
        #     r['visita_a'] = self.format_visita(r.get('visita_a',[]))
        print("rondines", simplejson.dumps( records,indent=4))
        return  records

    # def get_page_stats(self, booth_area, location, page=''):
    #     print('entra a get_booth_stats')
    #     print('booth_area', booth_area)
    #     print('location', location)
    #     today = datetime.today().strftime("%Y-%m-%d")
    #     res={}

    #     if page == 'Turnos':
    #         #Visitas dentro, Gafetes pendientes y Vehiculos estacionados
    #         match_query_visitas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_ACCESOS,
    #             f"answers.{self.bitacora_fields['status_visita']}": "entrada",
    #             f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
    #             f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
    #             f"answers.{self.bitacora_fields['ubicacion']}": location,
    #         }

    #         proyect_fields_visitas = {
    #             '_id': 1,
    #             'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
    #             'id_gafete': f"$answers.{self.bitacora_fields['gafete_catalog']}.{self.gafetes_fields['gafete_id']}",
    #             'status_gafete': f"$answers.{self.mf['status_gafete']}"
    #         }

    #         group_by_visitas = {
    #             '_id': None,
    #             'total_visitas_dentro': {'$sum': 1},
    #             'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
    #             'gafetes_info': {
    #                 '$push': {
    #                     'id_gafete':'$id_gafete',
    #                     'status_gafete':'$status_gafete'
    #                 }
    #             }
    #         }

    #         query_visitas = [
    #             {'$match': match_query_visitas},
    #             {'$project': proyect_fields_visitas},
    #             {'$group': group_by_visitas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_visitas))
    #         total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
    #         total_visitas_dentro = resultado[0]['total_visitas_dentro'] if resultado else 0
    #         gafetes_info = resultado[0]['gafetes_info'] if resultado else []
    #         gafetes_pendientes = sum(1
    #             for gafete in gafetes_info
    #                 if gafete.get('id_gafete') and gafete.get('status_gafete', '').lower() != 'entregado'
    #         )
            
    #         res['total_vehiculos_dentro'] = total_vehiculos_dentro
    #         res['in_invitees'] = total_visitas_dentro
    #         res['gafetes_pendientes'] = gafetes_pendientes

    #         #Articulos concesionados
    #         match_query_concesionados = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.CONCESSIONED_ARTICULOS,
    #             f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
    #         }

    #         proyect_fields_concesionados = {
    #             '_id': 1,
    #         }

    #         group_by_concesionados = {
    #             '_id': None,
    #             'articulos_concesionados': {'$sum': 1}
    #         }

    #         query_concesionados = [
    #             {'$match': match_query_concesionados},
    #             {'$project': proyect_fields_concesionados},
    #             {'$group': group_by_concesionados}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_concesionados))
    #         articulos_concesionados = resultado[0]['articulos_concesionados'] if resultado else 0
            
    #         res['articulos_concesionados'] = articulos_concesionados

    #         #Incidentes pendientes
    #         match_query_incidentes = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_INCIDENCIAS,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location
    #         }

    #         proyect_fields_incidentes = {
    #             '_id': 1,
    #             'acciones_tomadas_incidencia': f"$answers.{self.incidence_fields['acciones_tomadas_incidencia']}",
    #         }

    #         group_by_incidentes = {
    #             '_id': None,
    #             'incidentes_pendientes': {'$sum': {'$cond': [{'$or': [{'$eq': [{'$size': {'$ifNull': ['$acciones_tomadas_incidencia', []]}}, 0]},{'$eq': ['$acciones_tomadas_incidencia', None]}]}, 1, 0]}}
    #         }

    #         query_incidentes = [
    #             {'$match': match_query_incidentes},
    #             {'$project': proyect_fields_incidentes},
    #             {'$group': group_by_incidentes}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_incidentes))
    #         incidentes_pendientes = resultado[0]['incidentes_pendientes'] if resultado else 0
            
    #         res['incidentes_pendites'] = incidentes_pendientes
    #     elif page == 'Accesos' or page == 'Bitacoras':
    #         #Visitas en el dia, personal dentro, vehiculos dentro y salidas registradas
    #         match_query_visitas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_ACCESOS,
    #             # f"answers.{self.bitacora_fields['status_visita']}": "entrada",
    #             f"answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.pase_entrada_fields['status_pase']}": {"$in": ["Activo"]},
    #             f"answers.{self.bitacora_fields['caseta_entrada']}": booth_area,
    #             f"answers.{self.bitacora_fields['ubicacion']}": location,
    #             f"answers.{self.mf['fecha_entrada']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_visitas = {
    #             '_id': 1,
    #             'vehiculos': {"$ifNull": [f"$answers.{self.mf['grupo_vehiculos']}", []]},
    #             'perfil': f"$answers.{self.bitacora_fields['catalogo_pase_entrada']}.{self.mf['nombre_perfil']}",
    #             'status_visita': f"$answers.{self.bitacora_fields['status_visita']}"
    #         }

    #         group_by_visitas = {
    #             '_id': None,
    #             'visitas_en_dia': {'$sum': 1},
    #             'total_vehiculos_dentro': {'$sum': {'$size': '$vehiculos'}},
    #             'detalle_visitas': {
    #                 '$push': {
    #                     'perfil': '$perfil',
    #                     'status_visita': '$status_visita'
    #                 }
    #             }
    #         }

    #         query_visitas = [
    #             {'$match': match_query_visitas},
    #             {'$project': proyect_fields_visitas},
    #             {'$group': group_by_visitas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_visitas))
    #         # print('resultadooooooooooooooooo',resultado)
    #         total_vehiculos_dentro = resultado[0]['total_vehiculos_dentro'] if resultado else 0
    #         visitas_en_dia = resultado[0]['visitas_en_dia'] if resultado else 0
    #         detalle_visitas = resultado[0]['detalle_visitas'] if resultado else []
    #         personal_dentro = sum(1 for visita in detalle_visitas if visita['perfil'][0].lower() != "visita general")
    #         salidas = sum(1 for visita in detalle_visitas if visita['status_visita'].lower() == "salida")

    #         res['total_vehiculos_dentro'] = total_vehiculos_dentro
    #         res['visitas_en_dia'] = visitas_en_dia
    #         res['personal_dentro'] = personal_dentro
    #         res['salidas_registradas'] = salidas
    #     elif page == 'Incidencias':
    #         #Incidentes por dia
    #         match_query_incidentes = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_INCIDENCIAS,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['area_incidencia_catalog']}.{self.incidence_fields['area_incidencia']}": booth_area,
    #             f"answers.{self.incidence_fields['ubicacion_incidencia_catalog']}.{self.incidence_fields['ubicacion_incidencia']}": location,
    #             f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_incidentes = {
    #             '_id': 1,
    #         }

    #         group_by_incidentes = {
    #             '_id': None,
    #             'incidentes_x_dia': {'$sum': 1}
    #         }

    #         query_incidentes = [
    #             {'$match': match_query_incidentes},
    #             {'$project': proyect_fields_incidentes},
    #             {'$group': group_by_incidentes}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_incidentes))
    #         incidentes_x_dia = resultado[0]['incidentes_x_dia'] if resultado else 0

    #         res['incidentes_x_dia'] = incidentes_x_dia

    #         #Fallas pendientes
    #         match_query_fallas = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_FALLAS,
    #             f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_caseta']}": booth_area,
    #             f"answers.{self.fallas_fields['falla_ubicacion_catalog']}.{self.fallas_fields['falla_ubicacion']}": location,
    #             f"answers.{self.fallas_fields['falla_estatus']}": 'abierto',
    #             # f"answers.{self.incidence_fields['fecha_hora_incidencia']}": {"$gte": today,"$lt": f"{today}T23:59:59"}
    #         }

    #         proyect_fields_fallas = {
    #             '_id': 1,
    #         }

    #         group_by_fallas = {
    #             '_id': None,
    #             'fallas_pendientes': {'$sum': 1}
    #         }

    #         query_fallas = [
    #             {'$match': match_query_fallas},
    #             {'$project': proyect_fields_fallas},
    #             {'$group': group_by_fallas}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_fallas))
    #         fallas_pendientes = resultado[0]['fallas_pendientes'] if resultado else 0

    #         res['fallas_pendientes'] = fallas_pendientes
    #     elif page == 'Articulos':
    #         #Articulos concesionados pendientes
    #         match_query_concesionados = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.CONCESSIONED_ARTICULOS,
    #             f"answers.{self.consecionados_fields['catalogo_ubicacion_concesion']}.{self.mf['ubicacion']}": location,
    #             f"answers.{self.consecionados_fields['status_concesion']}": "abierto",
    #         }

    #         proyect_fields_concesionados = {
    #             '_id': 1,
    #         }

    #         group_by_concesionados = {
    #             '_id': None,
    #             'articulos_concesionados_pendientes': {'$sum': 1}
    #         }

    #         query_concesionados = [
    #             {'$match': match_query_concesionados},
    #             {'$project': proyect_fields_concesionados},
    #             {'$group': group_by_concesionados}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_concesionados))
    #         articulos_concesionados_pendientes = resultado[0]['articulos_concesionados_pendientes'] if resultado else 0
            
    #         res['articulos_concesionados_pendientes'] = articulos_concesionados_pendientes

    #         #Articulos perdidos
    #         match_query_perdidos = {
    #             "deleted_at": {"$exists": False},
    #             "form_id": self.BITACORA_OBJETOS_PERDIDOS,
    #             f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['ubicacion_perdido']}": location,
    #             f"answers.{self.AREAS_DE_LAS_UBICACIONES_SALIDA_OBJ_ID}.{self.perdidos_fields['area_perdido']}": booth_area,
    #         }

    #         proyect_fields_perdidos = {
    #             '_id': 1,
    #             'status_perdido': f"$answers.{self.perdidos_fields['estatus_perdido']}",
    #         }

    #         group_by_perdidos = {
    #             '_id': None,
    #             'perdidos_info': {
    #                 '$push': {
    #                     'status_perdido':'$status_perdido'
    #                 }
    #             }
    #         }

    #         query_perdidos = [
    #             {'$match': match_query_perdidos},
    #             {'$project': proyect_fields_perdidos},
    #             {'$group': group_by_perdidos}
    #         ]

    #         resultado = self.format_cr(self.cr.aggregate(query_perdidos))
    #         perdidos_info = resultado[0]['perdidos_info'] if resultado else []

    #         articulos_perdidos = 0
    #         for perdido in perdidos_info:
    #             status_perdido = perdido.get('status_perdido', '').lower()
    #             if status_perdido not in ['entregado', 'donado']:
    #                 articulos_perdidos += 1

    #         res['articulos_perdidos'] = articulos_perdidos

    #     # res ={
    #     #         "in_invitees":0,
    #     #         "articulos_concesionados":0,
    #     #         "incidentes_pendites": incidentes_pendientes,
    #     #         "vehiculos_estacionados": total_vehiculos,
    #     #         "gefetes_pendientes": 0,
    #     #     }
    #     return res
    
    def get_rondines_by_status(self, status_list=['programado', 'en_proceso']):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.BITACORA_RONDINES,
                f"answers.{self.f['estatus_del_recorrido']}": {"$in": status_list},
            }},
            {'$project': {
                '_id': 1,
                'timezone': 1,
                'fecha_programacion': f"$answers.{self.f['fecha_programacion']}",
                'rondinero_id': f"$answers.{self.USUARIOS_OBJ_ID}.{self.mf['id_usuario']}",
                'answers': f"$answers"
            }},
        ]

        rondines = self.format_cr(self.cr.aggregate(query))
        return rondines

    def close_rondines(self, list_of_rondines, timezone='America/Mexico_City'):
        #- Expirados son lo que esta en status programados y que tienen mas de 24 de programdos
        # - en progreso son lo que estan con status progreso y tienen mas de 1 hr de su ultimo check.
        answers = {}
        tiz = pytz.timezone(timezone)
        ahora_cierre = datetime.now(tiz)

        rondines_expirados = []
        rondines_en_proceso_vencidos = []

        for rondin in list_of_rondines:
            estatus = rondin.get('estatus_del_recorrido')
            fecha_programacion_str = rondin.get('fecha_programacion')
            user_id = self.unlist(rondin.get('rondinero_id', 0))
            user_data = self.lkf_api.get_user_by_id(user_id)
            user_timezone = user_data.get('timezone', 'America/Mexico_City')
            tz = pytz.timezone(user_timezone)
            ahora = datetime.now(tz)

            if estatus == 'programado' and fecha_programacion_str:
                fecha_programacion = tz.localize(datetime.strptime(fecha_programacion_str, '%Y-%m-%d %H:%M:%S'))
                if ahora > fecha_programacion + timedelta(hours=24):
                    rondines_expirados.append(rondin)
            elif estatus == 'en_proceso':
                areas = rondin.get('areas_del_rondin', [])
                ultima_fecha = None
                for area in areas:
                    fecha_str = area.get('fecha_hora_inspeccion_area', '')
                    if fecha_str:
                        fecha = tz.localize(datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S'))
                        if not ultima_fecha or fecha > ultima_fecha:
                            ultima_fecha = fecha
                if ultima_fecha and ahora > ultima_fecha + timedelta(minutes=15):
                    rondines_en_proceso_vencidos.append(rondin)

        rondines_ids = []
        rondines_expirados = rondines_expirados + rondines_en_proceso_vencidos
        for rondin in rondines_expirados:
            rondines_ids.append(rondin.get('_id'))

        answers[self.f['estatus_del_recorrido']] = 'cerrado'
        answers[self.f['fecha_fin_rondin']] = ahora_cierre.strftime('%Y-%m-%d %H:%M:%S')

        # print(stop)
        if answers:
            res = self.lkf_api.patch_multi_record(answers=answers, form_id=self.BITACORA_RONDINES, record_id=rondines_ids)
            if res.get('status_code') == 201 or res.get('status_code') == 202:
                return res
            else: 
                return res