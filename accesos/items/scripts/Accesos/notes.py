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

    data_notes = data.get("data_notes",{})
    data_update = data.get("data_update",{})
    location = data.get("location",'Planta Monterrey')
    area = data.get("area",'Caseta Principal')
    status = data.get("status",'abierto')
    folio = data.get("folio",'588-10')
    #-FUNCTIONS
    if option == 'new_notes':
        response = acceso_obj.create_note(location, area, data_notes)
    elif option == 'get_notes':
        response = acceso_obj.get_list_notes(location, area, status=status)
    elif option == 'update_note':
        response = acceso_obj.update_notes(data_update, folio)
    elif option == 'delete_note':
        response = acceso_obj.LKFException({'msg':'No hay permisos para borrar notas'})
        #response = acceso_obj.delete_notes(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})