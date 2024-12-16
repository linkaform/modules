# coding: utf-8
import sys, simplejson
from linkaform_api import settings, generar_qr
from account_settings import *

from base_utils import Base


def create_qr(field_id):
    record_id = base_ojb.record_id
    lkf_qr = generar_qr.LKF_QR(settings)
    qr_generado = lkf_qr.procesa_qr( record_id, f"qr_{record_id}", base_ojb.form_id, img_field_id=field_id )
    base_ojb.answers[field_id] = qr_generado


if __name__ == "__main__":
    base_ojb = Base(settings, sys_argv=sys.argv)
    base_ojb.console_run()

    field_id = base_ojb.data.get('field_id')
    if not field_id:
        base_ojb.LKFException({"msg":"No se encontro el dato de field_id en los argumentos de script create_qr. Favor de agregar `fild_id` a los argumentos"})
    if not base_ojb.record_id:
        base_ojb.record_id = base_ojb.object_id()
    create_qr(field_id)
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans':  base_ojb.answers,
        'metadata':{"id":base_ojb.record_id}

    }))

