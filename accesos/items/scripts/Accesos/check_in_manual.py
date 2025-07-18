# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos
from datetime import datetime
import pytz

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
        self.f.update({
            'option_checkin': '663bffc28d00553254f274e0',
            'image_checkin': '6855e761adab5d93274da7d7',
            'comment_checkin': '66a5b9bed0c44910177eb724',
            'start_shift': '6879828d0234f02649cad390',
            'end_shift': '6879828d0234f02649cad391',
        })
        
    def do_checkin(self, location, area, employee_list=[], check_in_manual={}):
        if not self.is_boot_available(location, area):
            msg = f"No se puede hacer check in en la caseta en la ubicación {location} y área {area}."
            msg += f"Porque '{self.last_check_in.get('employee')}' ya está registrado en la caseta."
            raise Exception(msg)
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
        user_data = self.lkf_api.get_user_by_id(self.user.get('user_id'))
        employee['timezone'] = user_data.get('timezone','America/Monterrey')
        employee['name'] = employee['worker_name']
        employee['position'] = self.chife_guard
        if not employee:
            msg = f"Ningun empleado encontrado con email: {self.user.get('email')}"
            self.LKFException(msg)
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
        resp_create = self.lkf_api.post_forms_answers(data)
        if resp_create.get('status_code') == 201:
            resp_create['json'].update({'boot_status':{'guard_on_duty':user_data['name']}})
        return resp_create

    def check_in_manual(self):
        if self.answers.get(self.f['start_shift']):
            msg = 'Ya se ha registrado el inicio del turno.'
            self.LKFException({'msg': msg, 'title': 'Turno ya iniciado'})
            
        tz = pytz.timezone('America/Mexico_City')
        now = datetime.now(tz)
        fecha_actual = now.strftime('%Y-%m-%d %H:%M:%S')
        
        self.answers.update({
            self.f['start_shift']: fecha_actual,
        })
        
    def check_out_manual(self):
        if self.answers.get(self.f['end_shift']):
            msg = 'Ya se ha registrado el fin del turno.'
            self.LKFException({'msg': msg, 'title': 'Turno ya finalizado'})

        tz = pytz.timezone('America/Mexico_City')
        now = datetime.now(tz)
        fecha_actual = now.strftime('%Y-%m-%d %H:%M:%S')
        
        self.answers.update({
            self.f['end_shift']: fecha_actual,
        })
        

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    option = acceso_obj.answers.get(acceso_obj.f['option_checkin'], '')
    
    if acceso_obj.answers.get(acceso_obj.f['start_shift']) and acceso_obj.answers.get(acceso_obj.f['end_shift']):
        msg = 'Este check in ya ha sido registrado con inicio y cierre, crea uno nuevo.'
        acceso_obj.LKFException({'msg': msg, 'title': 'Turno ya registrado'})

    response = {}
    if option == 'iniciar_turno':
        acceso_obj.check_in_manual()
    elif option == 'cerrar_turno':
        acceso_obj.check_out_manual()

    print('ANSWERSSSSSSSSSSSSS', simplejson.dumps(acceso_obj.answers, indent=3))
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': acceso_obj.answers
    }))