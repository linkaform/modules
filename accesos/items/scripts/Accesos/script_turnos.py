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
        default_booth , user_booths = self.get_user_boot(search_default=False)
        location = default_booth.get('location')
        if not default_booth:
            return self.LKFException({"status_code":400, "msg":'No booth found or configure for user'})
        booth_area = default_booth['area']
        booth_location = default_booth['location']
        booth_addres = self.get_area_address(booth_location, booth_area)
        guards_positions = self.config_get_guards_positions()
        location_employees = {}
        for guard_type in guards_positions:
            puesto = guard_type['tipo_de_guardia']
            location_employees[puesto] = location_employees.get(puesto,
                self.get_users_by_location_area(booth_location, booth_area, **{'position': guard_type['puestos']})
                )
            if guard_type['tipo_de_guardia'] == 'guardia_de_apoyo':
                support_positions = guard_type['puestos']
        guard = self.get_user_guards(location_employees['guardia'])
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
        load_shift_json["support_guards"] = location_employees['guardia_de_apoyo']
        load_shift_json["guard"] = self.update_guard_status(guard)
        load_shift_json["notes"] = notes
        load_shift_json["user_booths"] = user_booths
        return load_shift_json

    def get_user_guards(self, location_employees):
        for employee in location_employees:
            if employee.get('user_id',0) == self.user.get('user_id'):
                    return employee
        self.LKFException(f"El usuario con id {self.user['user_id']}, no se ecuentra configurado como guardia")


    def update_guard_status(self, guard):
        last_checkin = self.get_user_last_checkin(guard['user_id'])
        status_turn = 'Turno Cerrado'
        if last_checkin.get('checkin_type') == 'entrada':
            status_turn = 'Turno Abierto'

        guard['turn_start_datetime'] =  last_checkin.get('checkin_date','')
        guard['status_turn'] =  status_turn
        return guard


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
    #option = 'do_access'
    location = data.get("location", "Planta Monterrey")
    area = data.get("area","Caseta Vigilancia Norte 3")
    employee_list = data.get("employee_list",[])
    checkin_id = data.get("checkin_id","")
    qr_code = data.get('qr_code',"")
    vehiculo = data.get('vehiculo',"")
    equipo = data.get('equipo',"")
    #-FUNCTIONS
    #location = "Planta Durango"
    #area = "Almacen de equipos industriales"
    print('option', option)
    if option == 'load_shift':
        response = acceso_obj.get_shift_data()
        acceso_obj.HttpResponse({"data":response})
    elif option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
        acceso_obj.HttpResponse({"data": response})
    elif option == 'checkin':
        response = acceso_obj.do_checkin(location, area, employee_list)
        acceso_obj.HttpResponse({"data": response})
    elif option == 'checkout':
        response = acceso_obj.do_checkout(checkin_id=checkin_id, location=location, area= area)
        acceso_obj.HttpResponse({"data": response})
    elif option == 'search_access_pass':
        response = acceso_obj.search_access_pass(qr_code=qr_code, location=location)
        acceso_obj.HttpResponse({"data": response}, indent=4)
    elif option == 'do_access':
        response = acceso_obj.do_access(qr_code, location, area, vehiculo, equipo)
        acceso_obj.HttpResponse({"data": response})
    elif option == 'list_chiken_guards':
        location = 'Planta Monterrey'
        booth = 'Caseta Vigilancia Poniente 7'
        response = acceso_obj.get_guard_list(location, booth)
        acceso_obj.HttpResponse({"data": response})
    elif option == 'notes_guard':
        location = 'Planta Puebla'
        booth = 'Caseta Vigilancia Norte 8'
        response = acceso_obj.get_guard_notes(location, booth)
        acceso_obj.HttpResponse({"data": response})
    else :
        acceso_obj.HttpResponse({"msg": "Empty"})
