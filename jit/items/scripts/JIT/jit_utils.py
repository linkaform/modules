# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.jit.app import JIT

today = date.today()
year_week = int(today.strftime('%Y%W'))



class JIT(JIT):

    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

    def borrar_historial(self):
        print('arranca borrar')
        forms = [
            self.DEMANDA_UTIMOS_12_MES,
            'Reglas de Reorden  ',
            'stock ajustes de inventario',
            'stock',
            'procurment'
        ]
        self.cr.metodo_para_borrar_de_mongo({'form_id':{'$in':forms}})

    def upsert_procurment(self, product_by_warehouse, **kwargs):
        print('product by warehouse',product_by_warehouse)
        response = {}
        for wh, create_records in product_by_warehouse.items():
            print(f'----------------{wh}--------------------')
            existing_records = self.get_procurments(warehouse=wh)
            update_records = []
            # existing_skus = [prod['sku'] for prod in existing_procurments]
            for product in create_records[:]:
                if self.Product.SKU_OBJ_ID in product:
                    product_code = product[self.Product.SKU_OBJ_ID].get(self.f['product_code'])
                    sku = product[self.Product.SKU_OBJ_ID].get(self.f['sku'])
                    for existing_record in existing_records:
                        if existing_record.get('product_code') == product_code and \
                            existing_record.get('sku') == sku:
                            update_records.append(product)
                            try:
                                create_records.remove(product)
                            except ValueError:
                                 print('allready removed')

            print('update_records', update_records)
            print('create_records', create_records)
            response = self.create_procurment(create_records, **kwargs)

        return response