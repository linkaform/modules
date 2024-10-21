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
    marca = data.get('marca',"")
    tipo = data.get('tipo',"")

    #-FUNCTIONS
    if tipo and marca:
        response = acceso_obj.vehiculo_modelo(tipo, marca)
    elif tipo:
        response = acceso_obj.vehiculo_marca(tipo)
    else:
        response = acceso_obj.vehiculo_tipo()
    print('================ END RETURN =================')
    print(simplejson.dumps(response, indent=3))
    acceso_obj.HttpResponse({"data":response})

