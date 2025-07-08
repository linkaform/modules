# -*- coding: utf-8 -*-
import sys, simplejson, copy

from jit_utils import JIT
from lkf_addons.addons.base.app import CargaUniversal

from account_settings import *


class JIT(JIT):


    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)


    def upsert_reorder_point(self, option):
        if self.current_record:
            print('record, product base')
        records = self.get_product_average_demand_by_product()
        product_by_warehouse = {}
        config = self.get_config()
        print('config', config)
        ###
        warehouse = 'ALM GUADALAJARA'
        product_by_warehouse[warehouse] = []
        ###
        for rec in records:
            print('rec', rec)
            product_code = rec.get('product_code')
            demanda_12_meses = rec.get('demanda_12_meses',0)
            sku = rec.get('sku')
            if demanda_12_meses == 0 or not sku or not product_code:
                continue
            
            consumo_promedio_diario = float(rec.get('consumo_promedio_diario',0))
            location = rec.get('location')
            if not location:
                wh_config = self.WH.get_warehouse_config(warehouse, location_type='abastacimiento')
                print('warehouse', wh_config)
                location = wh_config.get('warehouse_location')
                if not location:
                    self.LKFException({"status_code":400, "msg":f"Se debe de configura una ubicacion de Abastecimiento para el almacen {warehouse}."})
            uom = rec.get('uom', config.get('uom'))
            ans = self.model_reorder_point(
                product_code, 
                sku,
                uom, 
                warehouse,
                location,
                consumo_promedio_diario,
                )
            product_by_warehouse[warehouse].append(ans)

        for wh, create_records in product_by_warehouse.items():
            update_records = []
            existing_products = self.get_reorder_rules(warehouse=wh)
            existing_skus = [prod['sku'] for prod in existing_products]
            #only_pr_skus = [prod['sku'] for prod in existing_products]
            for product in create_records[:]:
                if self.Product.SKU_OBJ_ID in product:
                    product_code = product[self.Product.SKU_OBJ_ID].get(self.f['product_code'])
                    sku = product[self.Product.SKU_OBJ_ID].get(self.f['sku'])
                    for existing_product in existing_products:
                        if existing_product.get('product_code') == product_code and \
                            existing_product.get('sku') == sku:
                            update_records.append(product)
                            try:
                               create_records.remove(product)
                            except ValueError:
                                print('allready removed')

            response = self.create_reorder_rule(create_records)
            # repose_edit = self.update_reorder_rule(update_records)

        return True

if __name__ == '__main__':
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    cu_obj = CargaUniversal(settings, sys_argv=sys.argv, use_api=True)
    jit_obj.console_run()
    option = jit_obj.data.get('option', 'xfer')
    option = 'buy'
    if option == 'xfer':
        response = jit_obj.upsert_reorder_point(option='xfer')
    elif option == 'buy':
        response = jit_obj.upsert_reorder_point(option='buy')
    res = cu_obj.update_status_record('reglas_reorden')
    print('==========res',res)
    # res = class_obj.update_status_record(estatus)

    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'replace_ans': jit_obj.answers,
    #     }))
