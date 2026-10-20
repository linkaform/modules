# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

if __name__ == "__main__":
    script_obj = Accesos(settings, sys_argv=sys.argv)
    script_obj.console_run()
    
    #TODO: Revisar lo de los codigos de los paises
    para = f"52{script_obj.answers.get(script_obj.envio_correo_fields['phone_to'])}"
    texto = script_obj.answers.get(script_obj.envio_correo_fields['msj']).strip()[:100]
    response = script_obj.send_sms_masiv(para, texto)

    if response.get('statusCode') not in {200, 201, 202}:
        script_obj.LKFException({'title': 'Error en SMS', 'msg': response.get('response', '')})

    sys.stdout.write(simplejson.dumps({
        'status': response.get('statusCode', 404),
        'response_sms': response
    }))