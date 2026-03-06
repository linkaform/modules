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
    data_article = data.get("data_article",{})


    data_article_update = data.get("data_article_update",{
        'observacion_concesion':'hola actualizado',
    })
    location = data.get("location",'Planta Monterrey')
    area = data.get("area","")
    status= data.get("status", "")
    folio = data.get("folio")
    tipo = data.get("tipo","")

    dateFrom = data.get("dateFrom", "")
    dateTo = data.get("dateTo", "")
    filterDate = data.get("filterDate", "")
   
    if option == 'new_article':
        response = acceso_obj.create_article_concessioned(data_article)
    elif option == 'get_articles':
        response = acceso_obj.get_list_articulos_concesionados(location, area, status, dateFrom=dateFrom, dateTo=dateTo, filterDate=filterDate)
    elif option == 'update_article':
        if data.get('data'):
            data = data['data']
        record_id = data.get("record_id")
        response = acceso_obj.update_article_concessioned(data, record_id)
    elif option == 'delete_article':
        response = acceso_obj.delete_article_concessioned(folio)
    elif option == 'catalogo_tipo_concesion':
        if tipo:
            response = acceso_obj.catalogo_tipo_concesion(location, tipo)
        else:
            response = acceso_obj.catalogo_tipo_concesion(location, tipo="")
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})