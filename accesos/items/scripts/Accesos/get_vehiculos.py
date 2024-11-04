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
    marca = data.get('marca',"")
    tipo = data.get('tipo',"")
    option = data.get("option","")
    #-FUNCTIONS
    if option == 'catalago_vehiculo':
        if tipo and marca:
            response = acceso_obj.vehiculo_modelo(tipo, marca)
        elif tipo:
            response = acceso_obj.vehiculo_marca(tipo)
        else:
            response = acceso_obj.vehiculo_tipo()
    elif option == 'catalago_estados':
        response = acceso_obj.catalogo_estados()
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})

