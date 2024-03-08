import sys, simplejson
from datetime import datetime

from linkaform_api import settings, network, utils
from account_settings import *

from accesos_utils import Accesos


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    all_data = simplejson.loads(sys.argv[2])
    data = all_data.get("data", {})
    option = data.get("option",'')
    location = data.get("location",'')
    #-CONFIGURATION
    #-EXECUTION
    if option !='':
        if option == 'query_alerts':
            response = acceso_obj.get_query_alerts('planta1')
            sys.stdout.write(simplejson.dumps({"json": response}))
        elif option == 'get_users_information':
            response = acceso_obj.get_user_information('planta1')
            print('Hola','Ejemplo')
        elif option == 'get_users_items':
            response = acceso_obj.get_user_information('planta1')
            print('Hola','Ejemplo')
        elif option == 'get_bitacora_users':
            print('Hola','Ejemplo')
    else:
        sys.stdout.write(simplejson.dumps({"msg": "Failed"}))
