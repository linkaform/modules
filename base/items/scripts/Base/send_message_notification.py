# coding: utf-8
#####
# Script para enviar notificacion por mensaje de texto
#####
import requests, sys, simplejson, json
from linkaform_api import settings
from account_settings import *
from twilio.base.exceptions import TwilioRestException

from base_utils import Base

class Base(Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def create_response(self, status, status_code, message="", data=[]):
        """
            Crea una respuesta estructurada.

            Parámetros:
                status (str): Indica que sucedio con la petición(error, success, etc.).
                status_code (int): Código de estado HTTP de la respuesta.
                message (str, opcional): Mensaje descriptivo de la respuesta.
                data (list, opcional): Datos devueltos en la respuesta.

            Retorna:
                dict: Diccionario con el estado, código, mensaje y datos.
        """
        response = {}
        response = {
            "status": status,
            "status_code": status_code,
            "message": message,
            "data": data
        }
        return response

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    base_obj.console_run()
    data_full_answers = json.loads(sys.argv[1])
    data_raw = json.loads(sys.argv[2])

    folio = data_full_answers.get('folio')
    message_version = data_raw.get('message_version')

    # User data
    user_info = base_obj.lkf_api.get_user_by_id(base_obj.user.get('user_id'))
    # phone_to = f"+52{user_info.get('phone')}"
    phone_to = ""
    mensaje = ""

    if message_version == 'cita_confirmada':
        mensaje = f"La Orden de Instalacion con Folio: {folio}, fue marcada como: Cita confirmada, "
        mensaje += "para mas detalles consulta la plataforma de Linkaform."
    elif message_version == 'finalizado':
        mensaje = f"La Orden de Instalacion con Folio: {folio}, fue marcada como: Finalizada, "
        mensaje += "para mas detalles consulta la plataforma de Linkaform. "

    if mensaje:
        #Enviar sms
        response = base_obj.lkf_api.send_sms(phone_to, mensaje, use_api_key=True)
        if hasattr(response, 'sid'):
            response = base_obj.create_response("success", 200, "El mensaje fue enviado con exito.")
        else:
            error = ['Revisa el script send_message_notification.py']
            response = base_obj.create_response("error", 500, "El mensaje tuvo un error...", error)
        
        base_obj.HttpResponse({"data": response})