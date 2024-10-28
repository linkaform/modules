# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):
    pass
    
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')
    location = data.get("location",'')
    access_pass = data.get("access_pass",{})

    if option == 'assets_access_pass':
        # used
        response = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
    elif option == 'create_access_pass' or option == 'crear_pase':
        response = acceso_obj.create_access_pass(location, access_pass)
    elif option == 'area_by_location':
        response = acceso_obj.catalago_area_location(location)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})