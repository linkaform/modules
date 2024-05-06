import sys, simplejson
from linkaform_api import settings, network, utils
from account_settings import *

from accesos_utils import Accesos

if __name__ == "__main__":
    #acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True) 
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    all_data = simplejson.loads(sys.argv[2])
    data = all_data.get("data", {})
    option = data.get("option",'')
    location = data.get("location",'')
    #-FUNCTIONS
    if option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
        sys.stdout.write(simplejson.dumps({"data": response}))
    elif option == 'location_guard':
        email = 'guardia1@linkaform.com'
        response = acceso_obj.get_guard_location(email)
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
