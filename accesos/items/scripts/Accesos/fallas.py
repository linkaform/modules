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

    data_failure = data.get("data_failure",{
        'falla_status':'abierto',
        'falla_fecha':'2024-07-08 12:36:00',
        'falla_ubicacion':'Planta Durango',
        'falla_area':'Sala de servidores',
        'falla':'Problemas con la climatización o el sistema de aire acondicionado',
        'falla_comments':' El clima de la sala de servidores no se encuentra trabajando desde hace una  hora.',
        'falla_guard':'Miranda Cervantes Loza',
        'falla_guard_solution':'Salvador Olmos Pancracio',
        'falla_fecha_solucion':'2024-07-08 12:36:00',
    })
    data_failure_update = data.get("data_failure_update",{
        'falla_status':'resuelto',
    })
    location = data.get("location",'')
    area = data.get("area",'')
    folio = data.get("folio",'')
    status = data.get("status",'')
    tipo = data.get("tipo",'')
    #-FUNCTIONS
    #option = 'new_failure';
    #option = 'get_failures';
    #option = 'update_failure';
    #option = 'delete_failure';
    print('option', option)
    if option == 'new_failure':
        response = acceso_obj.create_failure(data_failure)
    elif option == 'get_failures':
        response = acceso_obj.get_list_fallas(location, area, status=status)
    elif option == 'update_failure':
        response = acceso_obj.update_failure(data_failure_update, folio)
    elif option == 'delete_failure':
        response = acceso_obj.delete_failure(folio)
    elif option =='catalogo_area_empleado_apoyo':
        response = acceso_obj.catalogo_config_area_empleado_apoyo()
    elif option == 'catalogo_fallas':
        if tipo:
            response = acceso_obj.catalogo_falla(tipo)
        else:
            response = acceso_obj.catalogo_falla()
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})