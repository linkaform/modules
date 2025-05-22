
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
        self.load(module='Product', module_class='Warehouse', import_as='WH', **self.kwargs)

        self.f.update({
            'fecha_salida_multiple': '000000000000000000000111',
            'wh_name': '6442e4831198daf81456f274',
            'wh_location': '65ac6fbc070b93e656bd7fbe',
            'wh_name_dest': '65bdc71b3e183f49761a33b9',
            'wh_location_dest': '65c12749cfed7d3a0e1a341b',
            'status_salida_multiple': '6442e4537775ce64ef72dd6a',
            'grupo_productos_salida_multiple': '6442e4537775ce64ef72dd69',
            'product_code_salida': '61ef32bcdf0ec2ba73dec33d',
            'sku_salida': '65dec64a3199f9a040829243',
            'lot_number_salida': '620a9ee0a449b98114f61d77',
            'cantidad_salida': '6442e4cc45983bf1778ec17d'
        })

    def format_products_traspaso(self, list_of_products):
        formated_list_of_products = []

        for product in list_of_products:
            obj = {
                'product_code': product.get('sku', ''),
                'sku': product.get('sku', ''),
                'lot_number': 'lote1',
                'cantidad': product.get('adjust', 0)
            }
            formated_list_of_products.append(obj)

        print('FORMATED_LIST_OF_PRODUCTS', simplejson.dumps(formated_list_of_products, indent=3))
        return formated_list_of_products

    def create_salida_mult_prod_a_ubicacion(self, list_of_products):
        #HARDCODE DE MOMENTO DE LAS UBICACIONES
        almacen_origen = {
            'warehouse_name': 'ALM GUADALAJARA',
            'location': 'Almacen Guadalajara'
        }
        almacen_destino = {
            'warehouse_name': 'ALM MONTERREY',
            'location': 'Almacen Monterrey'
        }
        ##########################################

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
            print(product)
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

if __name__ == '__main__':
    JIT_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    JIT_obj.console_run()
    data = JIT_obj.data.get('data',{})
    list_of_products = data.get('data', [])
    almacen_destino = data.get('to', '')

    print('ANSWERSSSSSSSSSSSSSSSSS', simplejson.dumps(data, indent=3))
    formated_list_of_products = JIT_obj.format_products_traspaso(list_of_products)

    response = JIT_obj.create_salida_mult_prod_a_ubicacion(formated_list_of_products)
    if response.get('status_code') in [200, 201, 202]:
        print('Se creo la salida multiple de productos a ubicacion exitosamente')
    else:
        print(response)

