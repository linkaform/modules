# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):
    pass
    
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    jwt = acceso_obj.lkf_api.get_jwt(user='seguridad@linkaform.com', api_key='58c62328de6b38d6d039122a9f0f7577f6f70ce2')
    settings.config['JWT_KEY'] = jwt
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')
    location = data.get("location",'')
    folio = data.get("folio",'')
    access_pass = data.get("access_pass",{})
    data_msj=data.get("data_msj", {})
    data_cel_msj=data.get("data_cel_msj", {})

    if option == 'assets_access_pass':
        response = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
    elif option == 'create_access_pass' or option == 'crear_pase':
        response = acceso_obj.create_access_pass(location, access_pass) 
    elif option == 'update_pass':
        response = acceso_obj.update_pass(access_pass,folio)
    elif option == 'area_by_location':
        response = acceso_obj.catalago_area_location(location)
    elif option == 'enviar_msj':
        response = acceso_obj.create_enviar_msj_pase(data_msj=data_msj, data_cel_msj=data_cel_msj, folio=folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})