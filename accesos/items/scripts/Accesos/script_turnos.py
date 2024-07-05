# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from app import Accesos

class Accesos(Accesos):
    pass
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')
    # option = 'do_access'
    # option = 'do_out'
    #option = 'list_bitacora'
    #option = 'new_note'
    #option = 'load_shift'
    #option = 'checkin'
    #option = 'checkout'
    #option = 'search_access_pass'
    #option = 'get_user_booths'
    name_visit = data.get("name_visit", "Leticia Hernández Hernández")
    location = data.get("location", "Planta Monterrey")
    area = data.get("area","Caseta Vigilancia Norte 3")
    employee_list = data.get("employee_list",[])
    checkin_id = data.get("checkin_id","")
    qr_code = data.get('qr_code',"667c70ae36d4fc26bc12dfaa")
    vehiculo = data.get('vehiculo',"")
    equipo = data.get('equipo',"")
    #-FUNCTIONS
    print('option', option)
    if option == 'load_shift':
        response = acceso_obj.get_shift_data()
    elif option == 'list_bitacora':
        response = acceso_obj.get_list_bitacora(location,   area)
    elif option == 'get_user_booths':
        response = acceso_obj.get_user_booths_availability()
    elif option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
    elif option == 'checkin':
        response = acceso_obj.do_checkin(location, area, employee_list)
    elif option == 'checkout':
        response = acceso_obj.do_checkout(checkin_id=checkin_id, location=location, area= area)
    elif option == 'search_access_pass':
        response = acceso_obj.search_access_pass(qr_code=qr_code, location=location)
        #acceso_obj.HttpResponse({"data": response}, indent=4)
    elif option == 'do_out':
        response = acceso_obj.do_out(qr_code)
    elif option == 'do_access':
        response = acceso_obj.do_access(qr_code, location, area, vehiculo, equipo)
    elif option == 'list_chiken_guards':
        location = 'Planta Monterrey'
        booth = 'Caseta Vigilancia Poniente 7'
        response = acceso_obj.get_guard_list(location, booth)
    elif option == 'notes_guard':
        location = 'Planta Puebla'
        booth = 'Caseta Vigilancia Norte 8'
        response = acceso_obj.get_guard_notes(location, booth)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})

