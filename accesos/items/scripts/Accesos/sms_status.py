# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *
from twilio.rest import Client
from bson import ObjectId

from get_twilio_cred import get_twilio_cred

from accesos_utils import Accesos

class Accesos(Accesos):
    
    def send_cel_msj(self, data_cel_msj):
        print('entra a send_cel_msj')

        # #twilio_creds = get_twilio_cred()
        # twilio_creds = self.lkf_api.get_user_twilio_creds()
        # if twilio_creds.get('status_code') == 201:
        #     twilio_creds = twilio_creds['json']
        # else:
        #     self.LKFException({"msg":"Error al obtener credecniales de Twilio"})
        # print('entra a twilio_creds=',twilio_creds)
        
        # if not twilio_creds:
        #     return "No se pudieron obtener las credenciales de Twilio."

        # # "phone": 
        # # "api_key_sid":
        # # "api_key_secret":
        # # "twilio_auth_token":
        # # "twilio_sid": 




        # account_sid = twilio_creds['twilio_sid']
        # auth_token = twilio_creds['twilio_auth_token']
        # phone_twilio = twilio_creds['phone']
        # api_key_sid = twilio_creds['api_key_sid']
        # api_key_secret = twilio_creds['api_key_secret']

        # mensaje = data_cel_msj.get('mensaje', '')
        # phone_to = data_cel_msj.get('numero', '')
        
        # # mensaje = f"""
        # #     👋 ¡Hola! 🎉

        # #     Tu acceso para está listo.
        # #     Presenta este QR en la entrada: 

        # #     📅 Fecha de visita: 
        # #     📍 Ubicación: 

        # #     ¡Nos vemos pronto!  
        # # """
        mensaje ='Nuevo mensaje Paco....'
        msg = self.lkf_api.send_sms(phone_to, mensaje)
        print('msg', msg)

        # client = Client(api_key_sid, api_key_secret, account_sid)

        # try:
        #     response = client.messages.create(
        #         from_=phone_twilio,
        #         body=mensaje,
        #         to=phone_to,
        #     )
        #     return f"Mensaje enviado: {response.sid}"
        # except Exception as e:
        #     return f"Error al enviar el mensaje: {e}"

if __name__ == "__main__":
    print('ENTRA A SMS STATUS...')
    print('sys argv', sys.argv)
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data = acceso_obj.data.get('data',{})
    data_cel_msj = data.get('data_cel_msj', {})
    # phone_to = ['+528341227834', '+528115778605']
    # for p in phone_to:
        # res = acceso_obj.enviar_sms(p, "Hola Paco!!!333")
    phone_to = '+528341227834'
    mensaje = acceso_obj.send_cel_msj(data_cel_msj=data_cel_msj)
    print('msg', mensaje)
    # res = acceso_obj.enviar_sms(phone_to, mensaje)