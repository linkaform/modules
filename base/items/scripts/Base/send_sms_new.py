# coding: utf-8
#####
# Script para enviar notificacion por mensaje de texto
#####
import requests, sys, simplejson, json

from base_utils import Base
from account_settings import *

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def send_sms(self, phone_number, message):
        API_URL = f"http://api.alprotel.com/v1/sms"
        #TODO Cambiar de lugar el token
        API_TOKEN = ''

        headers = {
            'Authorization': f'{API_TOKEN}',
            'Content-Type': 'application/json'
        }

        data = {
            'para': phone_number,
            'texto': message
        }

        response = requests.post(API_URL, json=data, headers=headers)

        if response.status_code == 200:
            print('SMS enviado correctamente')
            return response.json()
        else:
            print('Error al enviar SMS')
            return response.text

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    base_obj.console_run()

    msg = 'Test de SMS desde Back'
    tel = '528340000000' # => +52 834 000 0000

    response = base_obj.send_sms(tel, msg)

    base_obj.HttpResponse({"data":response})