# -*- coding: utf-8 -*-
import sys, simplejson, copy, json

from jit_utils import JIT
from lkf_addons.addons.base.app import CargaUniversal

from account_settings import *


class JIT(JIT):


    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.config_fields.update({
            'warehouse': f"{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}"
        })

    def get_product_config(self, family, *args, **kwargs):
        if not self.GET_CONFIG:
            query = [
                {'$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": self.CONFIGURACIONES_JIT,
                    f"answers.{self.Product.PRODUCT_OBJ_ID}.{self.f['family']}": family,
                }},
                {'$project': {
                    "_id": 0,
                    "lead_time": f"$answers.{self.f['lead_time']}",
                    "demora": f"$answers.{self.f['demora']}",
                    "factor_seguridad_jit": f"$answers.{self.f['factor_seguridad_jit']}",
                    "factor_crecimiento_jit": f"$answers.{self.f['factor_crecimiento_jit']}",
                    "uom": f"$answers.{self.UOM_OBJ_ID}.{self.f['uom']}",
                    "warehouse": f"$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}",
                }}
            ]
            self.GET_CONFIG = self.format_cr(self.cr.aggregate(query))
            print('GET_CONFIG', self.GET_CONFIG)
        result = {}
        for res in self.GET_CONFIG:
            args = args or list(self.config_fields.keys())
            result = {arg:res[arg] for arg in args if res.get(arg)}
        return result if result else None

    def upsert_reorder_point_buy(self):
        if self.current_record:
            print('record, product base')
        records = self.get_product_average_demand_by_product(procurement_method='buy')
        product_by_warehouse = {}
        buy_family = self.answers.get(self.Product.PRODUCT_OBJ_ID, {}).get(self.f['family'], '')
        config = self.get_product_config(family=buy_family)
        print('config', config)
        ###
        warehouse = config.get('warehouse') # type: ignore
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
            uom = rec.get('uom', config.get('uom')) # type: ignore
            ans = self.model_reorder_point(
                product_code, 
                sku,
                uom, 
                warehouse,
                location,
                consumo_promedio_diario,
                method='buy'
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
                            existing_product.get('sku') == sku and \
                            existing_product.get('procurment_method') == 'buy':
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
    data_raw = json.loads(sys.argv[2])
    option = data_raw.get('option', 'transfer')

    if option == 'transfer':
        response = jit_obj.upsert_reorder_point()
    elif option == 'buy':
        response = jit_obj.upsert_reorder_point_buy()
    res = cu_obj.update_status_record('reglas_reorden')
    print('==========res',res)
    # res = class_obj.update_status_record(estatus)

    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'replace_ans': jit_obj.answers,
    #     }))
