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
    # option = 'do_access'
    # option = 'do_out'
    #option = 'list_bitacora'
    #option = 'new_note'
    #option = 'load_shift'
    #option = 'checkin'
    #option = 'checkout'
    #option = 'search_access_pass'
    #option = 'get_user_booths'
    #option = 'get_detail_user'
    #option = 'create_pase'
    #option = 'get_support_guards'
    #option = 'update_guards'
    area = data.get("area","Caseta Vigilancia Sur 5")

    comments = data.get('comments',"")
    checkin_id = data.get("checkin_id","")
    curp_code = data.get('curp_code',"")
    # data_pase = data.get('data_pase',{
    #     #-opcion
    #     "visitante_pase":'alta_de_nuevo_visitante',
    #     #-catalog
    #     "ubicacion_pase":'Planta Monterrey',
    #     "nombre_pase":'Josue',
    #     "email_pase":'Josue@linkaform.com',
    #     "telefono_pase":'89921232',
    #     "empresa_pase":'Linkaform',
    #     #-catalog
    #     "perfil_pase":'Visita General',
    #     #-set
    #     'visita_a_pase':[{
    #         #-catalog
    #         'nombre_completo':'Josue De Jesus',
    #     }],
    #     #-catalog
    #     'authorized_pase':'Pedro Cervante',
    #     #-opcion
    #     'visita_de_pase':'fecha_fija',
    #     'fecha_desde_pase':'2024-07-01 14:00:48',
    #     #-set
    #     'areas_group_pase':['Caseta Vigilancia Norte 3'],
    #     #-set
    #     'vehiculos_group_pase':[
    #         {
    #             'tipo':'Automóvil',
    #             'marca':'ALFA ROMEO',
    #             'modelo':'Alfa Romeo 166',
    #             'estado':'Ciudad de México',
    #             'placas':'Azul',
    #             'color':'AL1234',
    #         }
    #     ],
    #     #-set
    #     'equipo_group_pase':[
    #         {
    #             'nombre':'Laptop',
    #             'marca':'Lenovo',
    #             'color':'Negro',
    #             'tipo':'Computo',
    #             'serie':'123556',
    #         }
    #     ],
    #     #-set
    #     'instrucciones_group_pase':['Sin comentarios'],
    #     #-opcion
    #     'status_pase':'activo'
    # })
    employee_list = data.get("employee_list",[])
    folio = data.get('folio')
    guards = data.get('guards',[])
    equipo = data.get('equipo',"")
    checkin_id = data.get('checkin_id')
    name_visit = data.get("name_visit")
    forzar = data.get('forzar')
    id_checkin = data.get('id_checkin')
    id_catalog = data.get('id_catalog')
    location = data.get("location")
    area = data.get("area")
    support_guards = data.get('support_guards')
    qr_code = data.get('qr_code')
    vehiculo = data.get('vehiculo',"")
    tipo = data.get('tipo',"")
    marca = data.get('marca',"")
    visita_a = data.get('visita_a',"")

    #-FUNCTIONS
    print('option', option)
    if option == 'load_shift':
        response = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
    elif option == 'list_bitacora':
        response = acceso_obj.get_list_bitacora(location,  area)
    elif option == 'get_user_booths':
        response = acceso_obj.get_user_booths_availability()
    elif option == 'get_boot_guards' or option == 'guardias_de_apoyo':
        response = acceso_obj.get_booths_guards(location, area, solo_disponibles=True, **{'position':acceso_obj.support_guard})
    elif option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
    elif option == 'checkin':
        response = acceso_obj.do_checkin(location, area, employee_list)
    elif option == 'checkout':
        response = acceso_obj.do_checkout(checkin_id=checkin_id, \
            location=location, area= area, guards=guards, forzar=forzar, comments=comments)
    elif option == 'search_access_pass':
        response = acceso_obj.search_access_pass(qr_code=qr_code, location=location)
        #acceso_obj.HttpResponse({"data": response}, indent=4)
    elif option == 'do_out':
        response = acceso_obj.do_out(qr_code, location, area)
    elif option == 'do_access':
        response = acceso_obj.do_access(qr_code, location, area, data)
    elif option == 'notes_guard':
        response = acceso_obj.get_guard_notes(location, booth)
    elif option == 'vehiculo_tipo':
        print('tipo', tipo)
        print('marca', marca)
        if tipo and marca:
            response = acceso_obj.vehiculo_modelo(tipo, marca)
        elif tipo:
            response = acceso_obj.vehiculo_marca(tipo)
        else:
            response = acceso_obj.vehiculo_tipo()
    elif option == 'get_detail_user':
        response = acceso_obj.get_detail_user(curp_code)
    elif option == 'create_pase':
        response = acceso_obj.create_pase(data_pase)
    elif option == 'update_guards':
        response = acceso_obj.update_guards_checkin(support_guards, checkin_id, location, area)
    elif option == 'visita_a':
        response = acceso_obj.visita_a(location)
    elif option == 'visita_a_detail':
        response = acceso_obj.visita_a_detail(location, visita_a)
    else :
        response = {"msg": "Empty"}
    print('================ END RETURN =================')
    # print(simplejson.dumps(response, indent=3))
    acceso_obj.HttpResponse({"data":response})

