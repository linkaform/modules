# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from app import Accesos

class Accesos(Accesos):

    def get_access_notes(self, location_name, area_name):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ACCESOS_NOTAS,
            f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['location']}":location_name,
            f"answers.{self.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['area']}":area_name
            }
        query = [
            {'$match': match_query },
            {'$project': self.proyect_format(self.notes_project_fields)},
            {'$sort':{self.f['note_open_date']:1}}
            ]
        return self.format_cr_result(self.cr.aggregate(query))

    def get_boot_stats(self, booth_area, location):
        res ={
                "in_invitees":11,
                "articulos_concesionados":12,
                "incidentes_pendites": 13,
                "vehiculos_estacionados": 14,
                "gefetes_pendientes": 15,
            }
        return res

    def get_boot_status(self, booth_area, location):
        last_chekin = self.get_last_checkin(location, booth_area)
        boot_status = {
            "status":'Disponible',
            "guard_on_dutty":'',
            "user_id":'',
            "stated_at":'',
            }
        if last_chekin.get('checkin_type') == 'entrada':
            #todo
            #user_id 
            boot_status['status'] = 'No Disponible'
            boot_status['guard_on_dutty'] = last_chekin.get('employee') 
            boot_status['stated_at'] = last_chekin.get('checkin_date')
            boot_status['checkin_id'] = last_chekin['_id']

        return boot_status

    def get_shift_data(self, search_default=True):
        """
        Obtiene informacion del turno del usuario logeado
        """
        load_shift_json = { }
        username = self.user.get('username')
        user_id = self.user.get('user_id')
        booth = self.get_user_boot(search_default=True)
        location = booth.get('location')
        if not booth:
            return self.LKFException({"status_code":400, "msg":'No booth found or configure for user'})
        booth_area = booth['area']
        booth_location = booth['location']
        booth_addres = self.get_area_address(booth_location, booth_area)
        location_employees = self.get_users_by_location_area(booth_location, booth_area)
        guard, support_guards = self.get_support_guards(location_employees)
        notes = self.get_access_notes(booth_location, booth_area)
        load_shift_json["location"] = {
            "name":  booth_location,
            "area": booth_area,
            "city": booth_addres.get('city'),
            "state": booth_addres.get('state'),
            "address": booth_addres.get('address'),
            }
        load_shift_json["boot_stats"] = self.get_boot_stats( booth_area, location)
        load_shift_json["boot_status"] = self.get_boot_status(booth_area, location)
        load_shift_json["support_guards"] = support_guards
        #Agregar
        # 'status_turn' y 'turn_start_datetime'
        load_shift_json["guard"] = guard
        load_shift_json["notes"] = notes

        return load_shift_json

    def get_support_guards(self, location_employees):
        user_ids = []
        for employee in location_employees:
            if employee.get('user_id'):
                user_ids.append(employee['user_id'])
        # user_ids.append(self.user.get('user_id'))
        employee_data = self.get_employee_data(user_id=user_ids)
        support_guards = []
        for x in employee_data:
            if x.get('user_id',0) != self.user.get('user_id'):
                support_guards.append(x)
            else:
                guard = x
        return guard, support_guards

    # def do_checkin(self, location, area):
    #     username = self.user.get('username')
    #     user_id = self.user.get('user_id')
    #     self.do_checkin(location, area, employee_list=[])


if __name__ == "__main__":
    # print('sysarg',sys.argv)
    #acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True) 
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')
    #option = 'load_shift'
    #option = 'checkin'
    #option = 'checkout'
    #option = 'search_access_pass'
    location = data.get("location", "Planta Monterrey")
    area = data.get("area","Caseta Vigilancia Norte 3")
    employee_list = data.get("employee_list",[])
    checkin_id = data.get("checkin_id","")
    qr_code = data.get('qr_code',"")
    #-FUNCTIONS
    #location = "Planta Durango"
    #area = "Almacen de equipos industriales"
    print('option', option)
    if option == 'load_shift':
        print('-------------------------------------- aqui')
        response = acceso_obj.get_shift_data()
        sys.stdout.write(simplejson.dumps({"data":response}))
    elif option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'checkin':
        response = acceso_obj.do_checkin(location, area, employee_list)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'checkout':
        response = acceso_obj.do_checkout(checkin_id=checkin_id, location=location, area= area)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'search_access_pass':
        response = acceso_obj.search_access_pass(qr_code=qr_code, location=location)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'do_access':
        response = acceso_obj.do_access(qr_code, location, area)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'list_chiken_guards':
        location = 'Planta Monterrey'
        booth = 'Caseta Vigilancia Poniente 7'
        response = acceso_obj.get_guard_list(location, booth)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'notes_guard':
        location = 'Planta Puebla'
        booth = 'Caseta Vigilancia Norte 8'
        response = acceso_obj.get_guard_notes(location, booth)
        sys.stdout.write(simplejson.dumps({"data": response}))
    else :
        sys.stdout.write(simplejson.dumps({"msg": "Empty"}))
