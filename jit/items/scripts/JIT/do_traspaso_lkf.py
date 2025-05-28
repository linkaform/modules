
# -*- coding: utf-8 -*-
from datetime import datetime
import pytz
import sys, simplejson, re
from bson import ObjectId

from jit_utils import SIPRE, JIT

from account_settings import *

class JIT(JIT):

    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

    def format_locations(self, w_form, w_to):
        if w_to == 'mty':
            w_to = 'monterrey'
        elif w_to == 'gdl':
            w_to = 'guadalajara'
        else:
            w_to = ''

        for w in w_form:
            w_from = w.get('from', '').split(' ', 1)[1].lower()

        return w_from, w_to

    def format_products_traspaso(self, list_of_products):
        formated_list_of_products = []
        for product in list_of_products:
            qty = product.get('adjust', 0)
            if not qty:
                qty = product.get('handover', 0)
            obj = {
                'product_code': product.get('sku', ''),
                'sku': product.get('sku', ''),
                'lot_number': 'lote1',
                'cantidad': qty
            }
            formated_list_of_products.append(obj)
        return formated_list_of_products

    def create_salida_mult_prod_a_ubicacion(self, w_from, w_to, list_of_products):
        almacen_origen = {
            'warehouse_name': 'ALM ' + w_from.upper(),
            'location': 'Almacen ' + w_from.capitalize()
        }
        almacen_destino = {
            'warehouse_name': 'ALM ' + w_to.upper(),
            'location': 'Almacen ' + w_to.capitalize()
        }

        metadata = self.lkf_api.get_metadata(form_id=self.STOCK_ONE_MANY_ONE)
        metadata.update({
            "properties": {
                "device_properties":{
                    "System": "Script",
                    "Module": "Accesos",
                    "Process": "Creación de Salida Multiple Productos a una Ubicacion",
                    "Action": "do_report_traspaso",
                    "File": "jit/do_report_traspaso.py"
                }
            },
        })
        answers = {}

        tz = pytz.timezone('America/Mexico_City')
        today = datetime.now(tz).strftime('%Y-%m-%d')

        answers[self.f['fecha_salida_multiple']] = today
        answers[self.WH.WAREHOUSE_LOCATION_OBJ_ID] = {
            self.f['wh_name']: almacen_origen.get('warehouse_name', ''),
            self.f['wh_location']: almacen_origen.get('location', '')
        }
        answers[self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID] = {
            self.f['wh_name_dest']: almacen_destino.get('warehouse_name', ''),
            self.f['wh_location_dest']: almacen_destino.get('location', '')
        }

        products = []
        for product in list_of_products:
            obj = {
                self.STOCK_INVENTORY_OBJ_ID: {
                    self.f['product_code_salida']: product.get('product_code', ''),
                    self.f['sku_salida']: product.get('sku', ''),
                    self.f['lot_number_salida']: product.get('lot_number', '')
                },
                self.f['cantidad_salida']: product.get('cantidad', 0)
            }
            products.append(obj)
        answers[self.f['grupo_productos_salida_multiple']] = products

        answers[self.f['status_salida_multiple']] = 'to_do'

        metadata.update({'answers':answers})
        print(simplejson.dumps(metadata, indent=3))

        # print(stop)
        res = self.lkf_api.post_forms_answers(metadata)
        return res
    
    def get_folio_sipre(self, record_id):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.STOCK_ONE_MANY_ONE,
                "_id": ObjectId(record_id)
            }},
            {'$project': {
                '_id': 0,
                'folio_sipre': f'$answers.{self.f["folio_sipre"]}'
            }},
            {'$limit': 1}
        ]

        res = self.format_cr(self.cr.aggregate(query))
        formatted_res = self.unlist(res)
        formatted_res = formatted_res.get('folio_sipre', '')
        return formatted_res

if __name__ == '__main__':
    JIT_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    JIT_obj.console_run()
    data = JIT_obj.data.get('data',{})
    list_of_products = data.get('data', [])
    almacen_destino = data.get('to', '')

    print('ANSWERSSSSSSSSSSSSSSSSS', simplejson.dumps(data, indent=3))
    formated_list_of_products = JIT_obj.format_products_traspaso(list_of_products)
    w_from, w_to = JIT_obj.format_locations(list_of_products, almacen_destino)

    response = JIT_obj.create_salida_mult_prod_a_ubicacion(w_from, w_to, formated_list_of_products)
    status_code = 400
    sipre_folio = 'No obtenido'
    if response.get('status_code') in [200, 201, 202]:
        status_code = 200
        print('Se creo la salida multiple de productos a ubicacion exitosamente')
        record_id = response.get('json', {}).get('id', '')
        sipre_folio = JIT_obj.get_folio_sipre(record_id)
    else:
        print(response)

    sys.stdout.write(simplejson.dumps({
        'response': response,
        'status': status_code,
        'sipre_folio': sipre_folio
    }))