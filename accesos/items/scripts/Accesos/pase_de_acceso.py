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
    location = data.get("location",'')
    access_pass = data.get("access_pass",{})


        # answers[self.UBICACIONES_CAT_OBJ_ID][self.m['location']] = location
        # answers[self.pase_entrada_fields['tipo_visita']] = 'alta_nuevo_visitante'
        # answers[self.pase_entrada_fields['walkin_nombre']] = access_pass.get('nombre')
        # answers[self.pase_entrada_fields['walkin_email']] = access_pass.get('email')
        # answers[self.pase_entrada_fields['walkin_empresa']] = access_pass.get('empresa')
        # answers[self.pase_entrada_fields['walkin_fotografia']] = access_pass.get('foto')
        # answers[self.pase_entrada_fields['walkin_identificacion']] = access_pass.get('identificacion')
        # answers[self.pase_entrada_fields['walkin_telefono']] = access_pass.get('telefono')

    access_pass = {
        "nombre" : "Benito Juarez",
        "perfil_pase":"Walkin",
        "visita_a":"Venustiano Carranza",
        # "visita_a":"Pedro Parmo",
        "email" : "beno@juarez.com",
        "empresa" : "Linkaform",
        "foto" :[
         {
            "file_name": "benito.jpg",
            "file_url": "https://f001.backblazeb2.com/file/app-linkaform/public-client-10/121736/66c4d5b6d1095c4ce8b2c42a/66c5184c54ba9236c755940b.jpg"
         }
            ],
        "identificacion" : [
         {
            "file_name": "joeduck_id.jpg",
            "file_url": "https://f001.backblazeb2.com/file/app-linkaform/public-client-10/121736/66c4d5b6d1095c4ce8b2c42b/66c51852ed8e5696211adeea.jpg"
         }
            ],
        "telefono" : "8115778888"
        }



    if option == 'assets_access_pass':
        # used
        response = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
    elif option == 'create_access_pass' or option == 'crear_pase':
        response = acceso_obj.create_access_pass(location, access_pass)
    else :
        response = {"msg": "Empty"}
    print('================ END3 RETURN =================')
    print(simplejson.dumps(response, indent=3))
    acceso_obj.HttpResponse({"data":response})