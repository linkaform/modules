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

    data_article = data.get("data_article",{
        'status_perdido':'pendiente',
        'date_hallazgo_perdido':'2024-07-08 19:43:01',
        'ubicacion_perdido':'Planta Monterrey',
        'area_perdido':'Recursos eléctricos',
        'articulo_perdido':'Suéter',
        'photo_perdido':[{
            'file_url':'https://f001.backblazeb2.com/file/app-linkaform/public-client-126/71202/60b81349bde5588acca320e1/65dd1061092cd19498857933.jpg',
            'file_name':'ejemploidentificacion.jpg',
        }],
        'comments_perdido':'Se perdio Suetercito',
        'guard_perdido':'Pedro Cervantes',
        'recibe_perdido':'Karla Perez',
        'phone_recibe_perdido':'1234567891',
        'identification_perdido':[],
        'date_entrega_perdido':'',
    })
    data_article_update = data.get("data_article_update",{
        'status_perdido':'entregado',
        'date_entrega_perdido':'2024-07-09 19:43:01',
    })
    location = data.get("location",'Planta Monterrey')
    area = data.get("area",'Recursos eléctricos')
    folio = data.get("folio",'481-10')

    #-FUNCTIONS
    #option = 'new_article';
    #option = 'get_articles';
    #option = 'update_article';
    #option = 'delete_article';
    if option == 'new_article':
        response = acceso_obj.create_article_lost(data_article)
    elif option == 'get_articles':
        response = acceso_obj.get_list_article_lost(location, area)
    elif option == 'update_article':
        response = acceso_obj.update_article_lost(data_article_update, folio)
    elif option == 'delete_article':
        response = acceso_obj.delete_article_lost(folio)
    else :
        response = {"msg": "Empty"}
    acceso_obj.HttpResponse({"data":response})