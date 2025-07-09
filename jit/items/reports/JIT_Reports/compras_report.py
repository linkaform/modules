# -*- coding: utf-8 -*-
import sys, simplejson

from jit_report import Reports

from account_settings import *

class Reports(Reports):
    
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        
        self.f.update({
            'familia': '61ef32bcdf0ec2ba73dec343',
        })
        
    def get_catalog_product_field(self, id_field):
        query = {"form_id":self.PROCURMENT, 'deleted_at':{'$exists':False}}
        proc = self.get_procurments()
        products = [item['product_code'] for item in proc]
        # Obtener los ids distintos y filtrar los None
        mango_query = {
            "selector": {
                f'answers.{self.Product.f["product_code"]}': {'$in': products}
            },
            "limit": 10000,
            "skip": 0
        }
        res = self.lkf_api.search_catalog(self.Product.PRODUCT_ID, mango_query)
        res_format = self.format_catalog_product(res, id_field)
        return res_format
    
    def format_catalog_product(self, data_query, id_field):
        list_response = []
        for item in data_query:
            wharehouse = item.get(id_field,'')
            if wharehouse not in list_response and wharehouse !='':
                list_response.append(wharehouse)
        list_response.sort()
        return list_response
    
    def get_procurments_report(self, families: list):
        """
        Recupera los registros de procurments
        Args:
            None
        Returns:
            dict: Un diccionario con los registros de procurments agrupados por warehouse.
        """
        query = [
            {"$match": {
                "form_id": self.PROCURMENT,
                f"answers.{self.Product.SKU_OBJ_ID}.{self.f['familia']}": {"$in": families},
                f"answers.{self.f['procurment_method']}": "buy",
            }},
            {"$project": {
                "_id": 0,
                "warehouse": f"$answers.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.WH.f['warehouse_location']}",
                "sku": f"$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_sku']}",
                "family": f"$answers.{self.Product.SKU_OBJ_ID}.{self.f['familia']}",
                "line": f"$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_category']}",
                "desc": f"$answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_name']}",
                "peso": f"$answers.{self.UOM_OBJ_ID}.{self.f['uom']}",
                "stock": f"$answers.{self.f['stock_actual']}",
                "stock_min": f"$answers.{self.f['min_stock']}",
                "stock_max": f"$answers.{self.f['max_stock']}",
                "transit": f"$answers.{self.f['stock_en_transito']}",
                "purchase": f"$answers.{self.f['compra_sugerida']}",
                # "peso_compra": "",
            }}
        ]
        procurements = self.format_cr(self.cr.aggregate(query))
        format_data = {}
        for proc in procurements:
            wh = proc.get('warehouse')
            proc['family'] = self.unlist(proc.get('family', ''))
            proc['line'] = self.unlist(proc.get('line', ''))
            proc['desc'] = self.unlist(proc.get('desc', ''))
            proc['peso_compra'] = proc.get('peso', '')
            if wh:
                wh = wh.replace(' ', '_').lower()
                format_data.setdefault(wh, []).append(proc)
                proc.pop('warehouse', None)
            
        return format_data
        
    def get_details_procurment(self, sku: str):
        """
        Recupera los detalles del producto a partir del SKU.
        Args:
            sku (str): El SKU del producto.
        Returns:
            dict: Un diccionario con los detalles del producto.
        """
        mango_query = {
            "selector": {
                f"answers.{self.Product.f['product_sku']}": sku
            },
            "fields": ["_id", f"answers.{self.f['family']}", f"answers.{self.Product.f['product_name']}", f"answers.{self.Product.f['product_category']}"],
            "limit": 1,
        }
        res = self.lkf_api.search_catalog(self.Product.SKU_ID, mango_query)
        if res:
            res = self.unlist(res)
        return res
    
if __name__ == "__main__":
    class_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()

    response = {}
    data = class_obj.data
    data = data.get('data',[])
    option = data.get('option', '')
    families = data.get('product_families', [])

    if option == 'get_catalog':
        data = class_obj.get_catalog_product_field(id_field=class_obj.Product.f['product_type'])
        response = {
            "product_families": data
        }
    elif option == 'get_report':
        data = class_obj.get_procurments_report(families=families)
        response = {
            "data": data
        }
    else:
        response = {
            "data": 'No se selecciono ninguna opcion'
        }

    class_obj.HttpResponse(response)