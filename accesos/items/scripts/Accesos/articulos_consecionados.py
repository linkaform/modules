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
    locations = data.get("locations", [])
    area = data.get("area","")
    status= data.get("status", "")
    folio = data.get("folio")
    tipo = data.get("tipo","")
    limit = data.get("limit", 25)
    skip = data.get("skip", 0)
    search = data.get("search", "")
    search_fields = data.get("search_fields", [])

    dateFrom = data.get("dateFrom", "")
    dateTo = data.get("dateTo", "")
    filterDate = data.get("filterDate", "")

    if option == 'new_article':
        response = acceso_obj.create_article_concessioned(data_article)
    elif option == 'get_articles':
        response = acceso_obj.get_list_articulos_concesionados(location, area, status, dateFrom=dateFrom, dateTo=dateTo, filterDate=filterDate, limit=limit, skip=skip, locations=locations, search=search, search_fields=search_fields)
    elif option == 'update_article':
        if data.get('data'):
            data = data['data']
        record_id = data.get("record_id")
        response = acceso_obj.update_article_concessioned(data, record_id)
    elif option == 'delete_article':
        response = acceso_obj.delete_article_concessioned(folio)
    elif option == 'catalogo_tipo_concesion':
        response = acceso_obj.catalogo_tipo_concesion(tipo=tipo)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})