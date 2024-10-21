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
    data_article = data.get("data_article",{
        'status_concesion':'abierto',
        'ubicacion_concesion':'Planta Monterrey',
        'solicita_concesion':'compartida',
        'nombre_concesion':'Fernando Sntibañez',
        'caseta_concesion':'Caseta Vigilancia Poniente 7',
        'fecha_concesion':'2024-07-25 19:31:54',
        'area_concesion':'Recursos eléctricos',
        'equipo_concesion':'Ventilador torre',
        'observacion_concesion':'Observaciones',
        'fecha_devolucion_concesion':'2024-07-28 19:31:54', 
    })
    data_article_update = data.get("data_article_update",{
        'observacion_concesion':'hola actualizado',
    })
    location = data.get("location",'Planta Monterrey')
    #folio = data.get("folio",'')
    folio = data.get("folio",['553-10'])
    #-FUNCTIONS
    #option = 'new_article';
    #option = 'get_articles';
    #option = 'update_article';
    #option = 'delete_article';
    if option == 'new_article':
        response = acceso_obj.create_article_concessioned(data_article)
    elif option == 'get_articles':
        response = acceso_obj.get_list_article_concessioned(location)
    elif option == 'update_article':
        response = acceso_obj.update_article_concessioned(data_article_update, folio)
    elif option == 'delete_article':
        response = acceso_obj.delete_article_concessioned(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})