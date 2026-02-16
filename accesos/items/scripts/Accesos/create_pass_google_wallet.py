# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *
import requests
import jwt
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import uuid

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def format_list_to_google_pass(self, list_data):
        if not list_data:
            return ''
        if len(list_data) == 1:
            return self.unlist(list_data)
        if len(list_data) == 2:
            return f"{list_data[0]} y {list_data[1]}"
        return ', '.join(list_data[:-1]) + ' y ' + list_data[-1]

    def create_class_google_wallet(self, data, qr_code):
        ISSUER_ID = '3388000000022924601'
        CLASS_ID = f'{ISSUER_ID}.ProdPassClass'

        google_wallet_creds = self.lkf_api.get_user_google_wallet(use_api_key=True, jwt_settings_key=False)
        QR_CODE_VALUE = qr_code
        OBJECT_ID = f'{ISSUER_ID}.pase-entrada-{QR_CODE_VALUE}-{uuid.uuid4()}'

        credentials_data = google_wallet_creds.get('data', {})
        private_key = credentials_data.get('private_key')
        client_email = credentials_data.get('client_email')

        credentials = service_account.Credentials.from_service_account_info(
            credentials_data,
            scopes=['https://www.googleapis.com/auth/wallet_object.issuer']
        )

        auth_req = Request()
        credentials.refresh(auth_req)
        access_token = credentials.token

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }

        class_url = f'https://walletobjects.googleapis.com/walletobjects/v1/genericClass/{CLASS_ID}'
        class_check = requests.get(class_url, headers={'Authorization': f'Bearer {access_token}'})
        
        if class_check.status_code != 200:
            class_body = {
                "id": CLASS_ID,
            }
            response = requests.post(
                'https://walletobjects.googleapis.com/walletobjects/v1/genericClass',
                headers=headers,
                json=class_body
            )
            print("Status code:", response.status_code)
            print("Response text:", response.text)

        response = self.create_pass_google_wallet(OBJECT_ID, CLASS_ID, QR_CODE_VALUE, data, headers, client_email, private_key)
        return response

    def create_pass_google_wallet(self, object_id, class_id, qr_code, data, headers, client_email, private_key):
        ubicaciones_list = data.get('ubicaciones', [])
        format_ubicacion = self.format_list_to_google_pass(ubicaciones_list)
        visita_a_list = data.get('visita_a', [])
        format_visita_a = self.format_list_to_google_pass(visita_a_list)
        empresa = data.get('empresa', 'Sin especificar')
        num_accesos = data.get('num_accesos', 1)
        fecha_desde = data.get('fecha_desde', '')
        fecha_hasta = data.get('fecha_hasta', '')
        geolocations = data.get('geolocations', [])
        if not fecha_hasta:
            fecha_hasta = fecha_desde

        object_body = {
            "id": object_id,
            "classId": class_id,
            "state": "ACTIVE",
            "genericType": "GENERIC_TYPE_UNSPECIFIED",
            "cardTitle": {
                "defaultValue": {
                    "language": "es-MX",
                    "value": empresa
                }
            },
            "subheader": {
                "defaultValue": {
                    "language": "es-MX",
                    "value": 'Pase de Entrada'
                }
            },
            "header": {
                "defaultValue": {
                    "language": "es-MX",
                    "value": f'Visita a: {format_visita_a}'
                }
            },
            'logo': {
                'sourceUri': {
                    'uri':
                        'https://f001.backblazeb2.com/file/app-linkaform/public-client-126/71202/60b81349bde5588acca320e1/698b8b36e216075bd8f4597a.png'
                },
                'contentDescription': {
                    'defaultValue': {
                        'language': 'en-US',
                        'value': 'Generic card logo'
                    }
                }
            },
            "hexBackgroundColor": "#FFFFFF",
            "groupingInfo": {
                "sortIndex": 1,
                "groupingId": "pase_de_entrada",
            },
            "textModulesData": [
                {
                    "id": "ubicacion",
                    "header": "UBICACION",
                    "body": format_ubicacion
                },
                {
                    "id": "fecha_entrada",
                    "header": "FECHA DESDE",
                    "body": fecha_desde
                },
                {
                    "id": "fecha_salida",
                    "header": "FECHA HASTA",
                    "body": fecha_hasta
                },
                {
                    "id": "accesos",
                    "header": "ACCESOS",
                    "body": num_accesos
                },
                {
                    "id": "vehiculos",
                    "header": "",
                    "body": ""
                },
                {
                    "id": "equipos",
                    "header": "",
                    "body": ""
                }
            ],
            "barcode": {
                "type": "QR_CODE",
                "value": qr_code,
                "alternateText": "Muestra tu QR para ingresar"
            },
        }

        if geolocations:
            object_body['linksModuleData'] = {
                "uris": [
                    {
                        "kind": "walletobjects#uri",
                        "uri": f"https://www.google.com/maps/dir/?api=1&destination={value['latitude']},{value['longitude']}",
                        "description": f"Cómo llegar a {key}",
                        "id": f"direcciones_{key}"
                    }
                    for key, value in geolocations.items()
                ]
            }

        requests.post(
            'https://walletobjects.googleapis.com/walletobjects/v1/genericObject',
            headers=headers,
            json=object_body
        )

        jwt_payload = {
            "iss": client_email,
            "aud": "google",
            "origins": [],
            "typ": "savetowallet",
            "payload": {
                "genericObjects": [
                    {"id": object_id}
                ]
            }
        }
        signed_jwt = jwt.encode(jwt_payload, private_key, algorithm='RS256')
        save_url = f'https://pay.google.com/gp/v/save/{signed_jwt}'
        print('Agrega tu pase con este link:', save_url)
        return save_url

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data = acceso_obj.data.get('data', {})
    qr_code = data.get('qr_code', '')
    access_pass = data.get('access_pass', {})

    if not qr_code:
        acceso_obj.LKFException({'title': 'Error', 'msg': 'No se proporciono el codigo QR'})

    data = acceso_obj.get_pass_custom(qr_code=qr_code)
    visita_a = [i.get('nombre') for i in data.get('visita_a', [])]
    data_to_google_pass = {
        "nombre": data.get("nombre"),
        "visita_a": visita_a,
        "empresa": data.get("empresa"),
        "ubicaciones": data.get("ubicacion"),
        "num_accesos": data.get("limite_de_acceso"),
        "fecha_desde": data.get("fecha_de_expedicion"),
        "fecha_hasta": data.get("fecha_de_caducidad"),
        "geolocations": data.get("ubicaciones_geolocation"),
    }
    google_wallet_pass_url = acceso_obj.create_class_google_wallet(data=data_to_google_pass, qr_code=qr_code)

    if not google_wallet_pass_url:
        acceso_obj.LKFException({'title': 'Error al crear el pase de Google Wallet', 'msg': 'No se pudo crear el pase de Google Wallet'})

    acceso_obj.HttpResponse({
        "data": {"google_wallet_url": google_wallet_pass_url},
        "status_code": 200,
        "json": {   
            "msg": "Pase de Google Wallet creado exitosamente."
        }
    })