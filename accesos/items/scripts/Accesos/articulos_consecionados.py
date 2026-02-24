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

    mock_crea_consecion = {
        "ubicacion_concesion":"Planta Monterrey",
        "area_concesion":"Almacén de inventario",
        "caseta_concesion":"Almacén de inventario",#legacy
        "status_concesion":"abierto",
        "persona_nombre_concesion":"Armando Mendoza Sáenz",
        "persona_email_concesion":"email@emplado.com", # str or list ["email..."]
        "persona_id_concesion":126, #int or list [126]
        "persona_nombre_otro":"Armando OTRO Mendoza Sáenz",
        "persona_email_otro":"email@otro.com",
        "persona_identificacion_otro":[
            {"file_name":"identificacion.jpg",
            "file_url":"https://f001.backblazeb2.com/file/app-linkaform/public-client-126/116852/660459dde2b2d414bce9cf8f/6978d5c1ca8635304ad27889.jpeg"
        }],
        "fecha_concesion":"2026-01-28 00:43:39",
        "equipos":[{
            "categoria_equipo_concesion":"Artiuclos de Oficina",
            "nombre_equipo":"Dragonsito",
            "costo_equipo_concesion":300.00, # int or list [300,]
            "imagen_equipo_concesion":[{
                "file_name":"drangosito.jpg",
                "file_url":"https://f001.backblazeb2.com/file/app-linkaform/public-client-126/116852/660459dde2b2d414bce9cf8f/697a208dca2f8309b2306fce.jpeg", 
            }],
            "cantidad_equipo_concesion":3,
            "evidencia_entrega":[{
            "file_name":"Articulo.jpg",
            "file_url":"https://f001.backblazeb2.com/file/app-linkaform/public-client-126/116852/660459dde2b2d414bce9cf8f/697a208dca2f8309b2306fce.jpeg"
            }],
            "comentario_entrega":"Comentario de la Entrega",
        }],
        "observacion_concesion":"observacion",
        "evidencia":[],
        "firma":
            {"file_name":"Nombre de la Fimra.png", 
            "file_url":"https://f001.backblazeb2.com/file/app-linkaform/public-client-126/561/573e09e523d3fd5a35bbd9ef/a3ca395cde2343ac9eef5f9239b48344.jpg"
            },
        }

    mock_lista_articulos = {
        "ubicacion_concesion":"Planta Monterrey",
        "area_concesion":"Almacén de inventario",
        "caseta_concesion":"Almacén de inventario",#legacy
        "status_concesion":"abierto",
        "persona_nombre_concesion":"Armando Mendoza Sáenz",
        "persona_nombre_otro":"Armando Mendoza Sáenz",
        "persona_email_concesion":"email@emplado.com",
        "email_otro":"email@otro.com",
        "identificacion_otro":[],
        "fecha_concesion":"2026-01-28 00:43:39",
        "equipos":[{
            "categoria_equipo_concesion":"Nombre de la Categoria",
            "nombre_equipo":"Nombre Equipo",
            "costo_equipo_concesion":3,
            "imagen_equipo_concesion":"Nombre Equipo",
            "cantidad_equipo_concesion":3,
            "evidencia_entrega":[],
            "comentario_entrega":"",
        }],
        "observacion_concesion":"observacion",
        "evidencia":[],
        "firma":[],
        }


    data_article_update = data.get("data_article_update",{
        'observacion_concesion':'hola actualizado',
    })
    location = data.get("location",'Planta Monterrey')
    area = data.get("area","")
    status= data.get("status", "")
    folio = data.get("folio",['553-10'])
    tipo = data.get("tipo","")

    dateFrom = data.get("dateFrom", "")
    dateTo = data.get("dateTo", "")
    filterDate = data.get("filterDate", "")
   
    if option == 'new_article':
        response = acceso_obj.create_article_concessioned(data_article)
    elif option == 'get_articles':
        response = acceso_obj.get_list_articulos_concesionados(location, area, status, dateFrom=dateFrom, dateTo=dateTo, filterDate=filterDate)
    elif option == 'update_article':
        response = acceso_obj.update_article_concessioned(data_article_update, folio)
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