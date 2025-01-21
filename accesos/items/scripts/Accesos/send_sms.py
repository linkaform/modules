# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):
    pass

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    #Obtener id para obtener el link del pdf
    qr_code = json.loads(sys.argv[1])
    qr_code = qr_code.get('_id', '').get('$oid', '')

    #Saber si sera un pre_sms o no
    pre_sms_raw = json.loads(sys.argv[2])
    pre_sms_value = pre_sms_raw.get('pre_sms', '')

    # Convertir a booleano de forma segura
    if isinstance(pre_sms_value, bool):
        pre_sms = pre_sms_value
    else:
        pre_sms = pre_sms_value.lower() == 'true'

    #Datos necesario para enviar el sms
    telefono_invitado = acceso_obj.answers.get('662c2937108836dec6d92582', '')
    nombre_invitado = acceso_obj.answers.get('662c2937108836dec6d92580', '')
    link_completar_pase = acceso_obj.answers.get('6732aa1189fc6b0ae27e3824', '')
    # nombre_visita_a = acceso_obj.answers.get('663d4ba61b14fab90559ebb0', '')[0].get('677ffe8c638c8536ff37effb', '').get('62c5ff407febce07043024dd', '')
    # ubicacion = acceso_obj.answers.get('666718cb1bdffb6d8fc908d6', '').get('663e5c57f5b8a7ce8211ed0b', '')
    nombre_visita_a = acceso_obj.answers.get('663d4ba61b14fab90559ebb0', '')[0].get('66a83ab5ca3453e21ea08d19', '').get('62c5ff407febce07043024dd', '')
    ubicacion = acceso_obj.answers.get('66a83a74de752e12018fbc3c', '').get('663e5c57f5b8a7ce8211ed0b', '')
    fecha_desde = acceso_obj.answers.get('662c304fad7432d296d92582', '')
    fecha_hasta = acceso_obj.answers.get('662c304fad7432d296d92583', '')

    data_cel_msj = {
        'numero': telefono_invitado,
        'nombre': nombre_invitado,
        'link': link_completar_pase,
        'visita_a': nombre_visita_a,
        'ubicacion': ubicacion,
        'fecha_desde': fecha_desde,
        'fecha_hasta': fecha_hasta,
        'qr_code': qr_code,
        'pre_sms': pre_sms
    }

    print(simplejson.dumps(data_cel_msj, indent=4))
    
    #Enviar sms
    response = acceso_obj.send_msj_pase(data_cel_msj=data_cel_msj, pre_sms=pre_sms)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'response_sms': response
    }))