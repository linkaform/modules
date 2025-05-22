# -*- coding: utf-8 -*-
import sys, simplejson, re
from bson import ObjectId

from jit_utils import SIPRE, JIT

from account_settings import *

if __name__ == '__main__':
    JIT_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    JIT_obj.console_run()
    print('answerssssssssssssssssss', JIT_obj.answers)

    sipre_obj = SIPRE()
    sipre_obj.stock = sipre_obj.create_xfer_spire()
    # sipre_obj.stock = [
    #  {'almacen': '01', 'almacenNombre': 'ALM MONTERREY', 'producto': '750200301001', 'productoNombre': 'MTRS TUBO S/C A106B/API5L STD 1/4"', 'ventas': 61.0, 'inventario': 2.7, 'familiaProducto': 'TUBOS', 'lineaProducto': 'A.C.', 'fechaAltaProducto': '2016-06-30T00:00:00', 'renglones': 1, }, 
    #  {'almacen': '02', 'almacenNombre': 'ALM GUADALAJARA', 'producto': '750200301001', 'productoNombre': 'MTRS TUBO S/C A106B/API5L STD 1/4"', 'ventas': 17.4, 'inventario': 345.8, 'familiaProducto': 'TUBOS', 'lineaProducto': 'A.C.', 'fechaAltaProducto': '2016-06-30T00:00:00', 'renglones': 1} ]

    #HARDCODEADO DE MOMENTO EL FOLIO
    JIT_obj.answers[JIT_obj.f['folio_sipre']] = '2D32D131D1'

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': JIT_obj.answers,
    }))
    # res = class_obj.update_status_record(estatus)

