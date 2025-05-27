# -*- coding: utf-8 -*-
import sys, simplejson, re
from bson import ObjectId

from jit_utils import SIPRE, JIT

from account_settings import *

if __name__ == '__main__':
    JIT_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    sipre_obj = SIPRE()
    JIT_obj.console_run()
    print('answerssssssssssssssssss', JIT_obj.answers)

    products_group = JIT_obj.answers.get(JIT_obj.f['product_group'], [])
    warehouse_source = JIT_obj.answers.get(JIT_obj.WH.WAREHOUSE_LOCATION_OBJ_ID, {}).get(JIT_obj.f['wh_name'], '')
    warehouse_target = JIT_obj.answers.get(JIT_obj.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID, {}).get(JIT_obj.f['wh_name_dest'], '')
    
    # De momento solo se manejan 2 almacenes ##
    if warehouse_source == 'ALM MONTERREY':
        warehouse_source = '01'
    elif warehouse_source == 'ALM GUADALAJARA':
        warehouse_source = '02'

    if warehouse_target == 'ALM MONTERREY':
        warehouse_target = '01'
    elif warehouse_target == 'ALM GUADALAJARA':
        warehouse_target = '02'
    ###########################################

    sipre_list = []

    for idx, product in enumerate(products_group):
        quantity = product.get(JIT_obj.f['cantidad_salida'], 0)
        product_code = product.get(JIT_obj.STOCK_INVENTORY_OBJ_ID, {}).get(JIT_obj.f['product_code_salida'], '')
        
        product_code = '750200309149' # PRODUCTO DE PRUEBA, QUITAR LINEA CUANDO SE UTILICEN PRODUCTOS REALES

        if product_code and quantity > 0:
            sipre_response = sipre_obj.create_xfer_spire(warehouse_source, warehouse_target, product_code, quantity)
            JIT_obj.answers[JIT_obj.f['product_group']][idx][JIT_obj.f['renglon_sipre']] = sipre_response.get('renglon', '')
            sipre_folio = sipre_response.get('folio', 'No se obtuvo folio')
            sipre_list.append(sipre_folio)

    sipre_list_formatted = str(sipre_list[0]) if len(set(sipre_list)) == 1 else ", ".join(str(x) for x in sipre_list)
    sipre_obj.stock = sipre_list_formatted

    # sipre_obj.stock = [
    #  {'almacen': '01', 'almacenNombre': 'ALM MONTERREY', 'producto': '750200301001', 'productoNombre': 'MTRS TUBO S/C A106B/API5L STD 1/4"', 'ventas': 61.0, 'inventario': 2.7, 'familiaProducto': 'TUBOS', 'lineaProducto': 'A.C.', 'fechaAltaProducto': '2016-06-30T00:00:00', 'renglones': 1, }, 
    #  {'almacen': '02', 'almacenNombre': 'ALM GUADALAJARA', 'producto': '750200301001', 'productoNombre': 'MTRS TUBO S/C A106B/API5L STD 1/4"', 'ventas': 17.4, 'inventario': 345.8, 'familiaProducto': 'TUBOS', 'lineaProducto': 'A.C.', 'fechaAltaProducto': '2016-06-30T00:00:00', 'renglones': 1} ]

    #HARDCODEADO DE MOMENTO EL FOLIO
    print(sipre_obj.stock)

    JIT_obj.answers[JIT_obj.f['folio_sipre']] = sipre_obj.stock

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': JIT_obj.answers,
    }))
    # res = class_obj.update_status_record(estatus)

