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
    data_gafete = data.get("data_gafete",{
        'status_gafete':'asignar_gafete',
        'ubicacion_gafete':'Planta Durango',
        'caseta_gafete':'Caseta Vigilancia Av 16',
        'visita_gafete':'Leticia Hernández Hernández',
        'id_gafete':'00001',
        'documento_gafete':['INE'],
    })
    location = data.get("location",'Planta Durango')
    folio = data.get("folio",'512-10')
    #-FUNCTIONS
    #option = 'new_badge';
    #option = 'get_badge';
    #option = 'deliver_badge';
    if option == 'new_badge':
        response = acceso_obj.create_badge(data_gafete)
    elif option == 'get_badge':
        response = acceso_obj.get_list_badge(location)
    elif option == 'deliver_badge':
        response = acceso_obj.deliver_badge(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})