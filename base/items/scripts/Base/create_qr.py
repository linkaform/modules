# coding: utf-8
import sys, simplejson
from linkaform_api import settings, generar_qr
from account_settings import *

from app import Accesos


def create_qr(field_id):
    record_id = acceso_obj.record_id
    lkf_qr = generar_qr.LKF_QR(settings)
    qr_generado = lkf_qr.procesa_qr( record_id, f"qr_{record_id}", acceso_obj.form_id, img_field_id=field_id )
    acceso_obj.answers[field_id] = qr_generado


if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    field_id = acceso_obj.data.get('field_id')
    if not field_id:
        acceso_obj.LKFException({"msg":"No se encontro el dato de field_id en los argumentos de script create_qr. Favor de agregar `fild_id` a los argumentos"})
    if not acceso_obj.record_id:
        acceso_obj.record_id = acceso_obj.object_id()
    create_qr(field_id)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans':  acceso_obj.answers,
        'metadata':{"id":acceso_obj.record_id}

    }))

