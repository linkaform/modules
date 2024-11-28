# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *
import base64
import requests
import tempfile
import json
from pprint import pprint

from accesos_utils import Accesos

def get_visita_autorizada(img_url=''):
    # print(img_url)
    # Define the file path and API details
    image_path = img_url

    nufi = {
        'ocr_ine_frente': {'url':"https://nufi.azure-api.net/ocr/v4/frente",'method':'POST'},
        'calcular_rfc': {'url':"https://nufi.azure-api.net/api/v1/calcular_rfc",'method':'POST'},
        'nss': {'url':"https://nufi.azure-api.net/numero_seguridad_social/v2/consultar",'method':'POST'},
        'nss_status': {'url':"https://nufi.azure-api.net/numero_seguridad_social/v2/status",'method':'POST'},
    }

    api_key = ""

    response = requests.get(img_url)

    if response.status_code == 200:
        # Guardar la imagen de manera temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name
            # print(f"Imagen guardada temporalmente en: {temp_file_path}")
            
            # Codificar la imagen a base64
            with open(temp_file_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')

            # Obtener datos de la INE
            ine_payload = {
               "base64_credencial_frente": base64_image
            }

            ine_headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "NUFI-API-KEY": api_key
            }

            ine_response = requests.post(nufi['ocr_ine_frente']['url'], json=ine_payload, headers=ine_headers)

            # Output the response
            # print("Response Status Code:", ine_response.status_code)
            # print("Response Body:", ine_response.json())

            data_ine = ine_response.json()
            nombres = data_ine.get('data', {}).get('ocr', {}).get('nombre', '')
            apellido_paterno = data_ine.get('data', {}).get('ocr', {}).get('apellido_paterno', '')
            apellido_materno = data_ine.get('data', {}).get('ocr', {}).get('apellido_materno', '')
            fecha_nacimiento = data_ine.get('data', {}).get('ocr', {}).get('fecha_nacimiento', '')
            curp = data_ine.get('data', {}).get('ocr', {}).get('curp', '')
            #Direccion
            calle = data_ine.get('data', {}).get('ocr', {}).get('calle_numero', '')
            colonia = data_ine.get('data', {}).get('ocr', {}).get('colonia', '')
            codigo_postal = data_ine.get('data', {}).get('ocr', {}).get('codigo_postal', '')
            municipio = data_ine.get('data', {}).get('ocr', {}).get('municipio', '')
            estado = data_ine.get('data', {}).get('ocr', {}).get('estado', '')

            # Obtener RFC
            rfc_payload = {
                "nombres": nombres,
                "apellido_paterno": apellido_paterno,
                "apellido_materno": apellido_materno,
                "fecha_nacimiento": fecha_nacimiento
            }

            rfc_headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Ocp-Apim-Subscription-Key": api_key
            }

            rfc_response = requests.post(nufi['calcular_rfc']['url'], json=rfc_payload, headers=rfc_headers)
            
            data_rfc = rfc_response.json()
            rfc = data_rfc.get('data', {}).get('rfc', '')

            # print("Response Status Code:", rfc_response.status_code)
            # print("Response Body:", rfc_response.json())

            # Obtener UUID para consultar NSS
            uuid_nss_payload = {
                "curp": curp,
                "webhook": "https://webhook.site/590701b1-29b4-43c6-a5da-edfef59860fa"
            }
            uuid_nss_headers = {
                "NUFi-API-KEY": "",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "NUFI-API-KEY": api_key
            }

            uuid_nss_response = requests.post(nufi['nss']['url'], json=uuid_nss_payload, headers=uuid_nss_headers)

            print("Response Status Code:", uuid_nss_response.status_code)
            print("Response Body:", uuid_nss_response.json())

            data_uuid_nss = uuid_nss_response.json()
            nss_uuid = data_uuid_nss.get('data', {}).get('uuid', '')

            # Consultar NSS a partir de UUID
            consultar_nss_payload = {
                "uuid_nss": nss_uuid,
                "uuid_historial": "string"
            }
            consultar_nss_headers = {
                "NUFI-API-KEY": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            consultar_nss_response = requests.post(nufi['nss_status']['url'], json=consultar_nss_payload, headers=consultar_nss_headers)

            data_nss = consultar_nss_response.json()
            nss = data_nss.get('data', {}).get('nss', {}).get('info', {}).get('numero_seguridad_social', '')

            print("Response Status Code:", consultar_nss_response.status_code)
            print("Response Body:", consultar_nss_response.json())
    else:
        print(f"Error al descargar la imagen. Código de estado: {response.status_code}")
        return None

    visita_autorizada = {
        "nombre_completo": f"{nombres} {apellido_paterno} {apellido_materno}",
        "curp": curp,
        "direccion": f"Calle: {calle}\nColonia: {colonia}\nCodigo postal: {codigo_postal}\nMunicipio: {municipio}\nEstado: {estado}\n",
        "nss": nss,
        "rfc": rfc
    }

    return visita_autorizada

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    jwt = acceso_obj.lkf_api.get_jwt(user='seguridad@linkaform.com', api_key='58c62328de6b38d6d039122a9f0f7577f6f70ce2')
    settings.config['JWT_KEY'] = jwt
    acceso_obj = Accesos(settings, sys_argv=sys.argv, use_api=True)
    acceso_obj.console_run()
    
    argument = sys.argv[1]

    # Convertir el JSON en un diccionario de Python
    data = json.loads(argument)

    # Obtener el valor de 'record_id'
    record_id = data.get('record_id', '')

    # Imprimir el record_id
    # print(f'Record ID: {record_id}')

    #Obtengo el pase
    get_pase = acceso_obj.get_detail_access_pass(qr_code=record_id)
    img_url = get_pase.get('identificacion', [])[0].get('file_url')
    print('mi paseeeeeeeeeeeeeeeeeee', get_pase)

    #Obtengo NSS, RFC
    response = get_visita_autorizada(img_url=img_url)
    print(response)
    print('###################################')
    pase_info = {
        'email': get_pase.get('email', ''),
        'telefono': get_pase.get('telefono', ''),
        'fotografia': get_pase.get('foto', []),
        'identificacion': get_pase.get('identificacion', [])
    }
    respuesta_obj = acceso_obj.create_visita_autorizada(visita_autorizada_obj=response, pase_obj=pase_info)
    # acceso_obj.HttpResponse({"data":response})