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

    data_incidence = data.get("data_incidence",{
        'date_incidence':'2024-05-29 12:36:00',
        'ubicacion_incidence':'Planta Monterrey',
        'area_incidence':'Caseta Vigilancia Poniente 7',
        'incidence':'Accidentes laborales, como resbalones, tropiezos o caídas',
        'guard_incident':'Jacinto Sánchez Hil',
        'comments_incidence':'Se partierón toda la cara',
    })
    data_incidence_update = data.get("data_incidence_update",{
        'incidence':'Se detuvierón las escaleras',
    })
    location = data.get("locacion",'Planta Monterrey')
    area = data.get("area",'Caseta Vigilancia Poniente 7')
    folio = data.get("folio",'474-10')
    #-FUNCTIONS
    #option = 'new_incidence';
    #option = 'update_incidence';
    #option = 'get_incidences';
    #option = 'delete_incidence';
    print('option', option)
    if option == 'new_incidence':
        response = acceso_obj.create_incidence(data_incidence)
    elif option == 'get_incidences':
        response = acceso_obj.get_list_incidences(location, area)
    elif option == 'update_incidence':
        response = acceso_obj.update_incidence(data_incidence_update, folio)
    elif option == 'delete_incidence':
        response = acceso_obj.delete_incidence(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})
