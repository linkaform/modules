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
    option = data.get("option","")

    data_incidence = data.get("data_incidence",{
        'reporta_incidencia': "",
        'fecha_hora_incidencia': "",
        'ubicacion_incidencia': "",
        'area_incidencia': "",
        'incidencia': "",
        'comentario_incidencia': "",
        'tipo_dano_incidencia': [],
        'dano_incidencia':"",
        'personas_involucradas_incidencia':[],
        'acciones_tomadas_incidencia':[],
        'evidencia_incidencia':[],
        'documento_incidencia':[],
        'prioridad_incidencia':"",
        'notificacion_incidencia':"",
        'total_deposito_incidencia':"",
        'datos_deposito_incidenica':[]
    })
    data_incidence_update = data.get("data_incidence_update",{
        'incidence':'Se detuvierón las escaleras'
    })
    location = data.get("location","")
    area = data.get("area","")
    prioridades = data.get("prioridades",[])
    folio = data.get("folio")
    #-FUNCTIONS
    #option = 'new_incidence';
    #option = 'update_incidence';
    #option = 'get_incidences';
    #option = 'delete_incidence';
    print('option', option)
    if option == 'nueva_incidencia':
        response = acceso_obj.create_incidence(data_incidence)
    elif option == 'get_incidences':
        response = acceso_obj.get_list_incidences(location, area, prioridades= prioridades)
    elif option == 'update_incidence':
        response = acceso_obj.update_incidence(data_incidence_update, folio)
    elif option == 'delete_incidence':
        response = acceso_obj.delete_incidence(folio)
    elif option == 'catalogo_area_empleado':
        response = acceso_obj.catalogo_config_area_empleado()
    elif option == 'catalogo_incidencias':
        response = acceso_obj.catalogo_incidencias()
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})
