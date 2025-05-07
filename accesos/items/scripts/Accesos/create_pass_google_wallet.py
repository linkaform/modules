# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *
import requests
import jwt
from google.oauth2 import service_account
from google.auth.transport.requests import Request

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def create_class_google_wallet(self, data={}, qr_code='6810f4026d0667ebf41d9a11'):
        google_wallet_creds = self.lkf_api.get_user_google_wallet(use_api_key=True, jwt_settings_key=False)
        ISSUER_ID = '3388000000022916692'
        CLASS_ID = f'{ISSUER_ID}.passClass-01'
        QR_CODE_VALUE = qr_code
        OBJECT_ID = f'{ISSUER_ID}.pase-entrada-{QR_CODE_VALUE}-01'

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

        class_url = f'https://walletobjects.googleapis.com/walletobjects/v1/eventTicketClass/{CLASS_ID}'
        class_check = requests.get(class_url, headers={'Authorization': f'Bearer {access_token}'})
        


        if class_check.status_code != 200:
            class_body = {
                "id": CLASS_ID,
                "issuerName": "Pase de Entrada",
                "hexBackgroundColor": "#FFFFFF",
                "logo": {
                    "sourceUri": {
                        "uri": "https://f001.backblazeb2.com/file/app-linkaform/public-client-126/68600/6076166dfd84fa7ea446b917/2025-04-28T11:11:42.png",
                    },
                    "contentDescription": {
                        "defaultValue": {
                            "language": "es-MX",
                            "value": "Logo de la empresa"
                        }
                    }
                },
                # "eventName": {
                #     "defaultValue": {
                #         "language": "es-MX",
                #         "value": f"Visita a: {visita_a}"
                #     }
                # },
                # "venue": {
                #     "name": {
                #         "defaultValue": {
                #             "language": "es-MX",
                #             "value": ubicacion
                #         }
                #     },
                #     "address": {
                #         "defaultValue": {
                #             "language": "es-MX",
                #             "value": address
                #         }
                #     }
                # },
                # "dateTime": {
                #     "start": "2025-05-05T19:00:00Z"
                # },
                "reviewStatus": "UNDER_REVIEW",
            }
            requests.post(
                'https://walletobjects.googleapis.com/walletobjects/v1/eventTicketClass',
                headers=headers,
                json=class_body
            )

        self.create_pass_google_wallet(OBJECT_ID, CLASS_ID, QR_CODE_VALUE, data, headers, client_email, private_key)

    def create_pass_google_wallet(self, object_id, class_id, qr_code, data, headers, client_email, private_key):
        nombre = data.get('nombre', 'Alejandro Fernandez')
        ubicacion = data.get('ubicacion', 'Planta Monterrey')
        address = data.get('address', 'Av. Revolución 123, Zapata, MX')
        visita_a = data.get('visita_a', 'Emiliano Zapata')

        object_body = {
            "id": object_id,
            "classId": class_id,
            "state": "ACTIVE",
            "barcode": {
                "type": "QR_CODE",
                "value": qr_code,
            },
            "ticketHolderName": nombre,
            "ticketNumber": "09-3924",
            "textModulesData": [
                {
                    "header": "Ubicación",
                    "body": ubicacion
                },
                {
                    "header": "Dirección",
                    "body": address
                },
                {
                    "header": "Visita a",
                    "body": visita_a
                }
            ],
            "eventDateTime": {
                "start": data.get("start_time", "2025-05-05T19:00:00Z")
            }
        }

        requests.post(
            'https://walletobjects.googleapis.com/walletobjects/v1/eventTicketObject',
            headers=headers,
            json=object_body
        )

        jwt_payload = {
            "iss": client_email,
            "aud": "google",
            "origins": [],
            "typ": "savetowallet",
            "payload": {
                "eventTicketObjects": [
                    {"id": object_id}
                ]
            }
        }

        signed_jwt = jwt.encode(jwt_payload, private_key, algorithm='RS256')
        save_url = f'https://pay.google.com/gp/v/save/{signed_jwt}'

        print('Agrega tu pase con este link:', save_url)


if __name__ == "__main__":
    print('Entra en create_pass_google_wallet')
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    acceso_obj.create_class_google_wallet()

    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'response_sms': response
    # }))