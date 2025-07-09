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
    
    def get_product_families(self):
        """
            Recupera y retorna una lista de familias únicas de productos del catálogo Product Catalog.
        Retorna:
            list: Una lista de familias únicas de productos.
        """
        selector = {}
        fields = ["_id", f"answers.{self.f['familia']}"]
        
        mango_query = {
            "selector": selector,
            "fields": fields,
            "limit": 10000,
            "skip": 0
        }
        
        res = self.lkf_api.search_catalog(self.Product.PRODUCT_ID, mango_query)
        
        familias = set()
        for doc in res:
            familia = doc.get(self.f['familia'])
            if familia:
                familias.add(familia)
        familias = list(familias)
        
        return familias
        
    def get_procurments(self, families: list):
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
                "peso_unit": f"$answers.{self.UOM_OBJ_ID}.{self.f['uom']}",
                "inventario": f"$answers.{self.f['stock_actual']}",
                "transito": f"$answers.{self.f['stock_en_transito']}",
                "compra": f"$answers.{self.f['compra_sugerida']}",
                # "peso_compra": "",
            }}
        ]
        procurements = self.format_cr(self.cr.aggregate(query))
        format_data = {}
        for proc in procurements:
            sku = proc.get('sku')
            wh = proc.get('warehouse')
            if sku and wh:
                details = self.get_details_procurment(sku)
                proc.update({
                    'descripcion': details.get(self.Product.f['product_name'], ''),
                    'linea': details.get(self.Product.f['product_category'], ''),
                    'familia': details.get(self.f['family'], '')
                })
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
        data = class_obj.get_product_families()
        response = {
            "product_families": data
        }
    elif option == 'get_report':
        data = class_obj.get_procurments(families=families)
        response = {
            "data": data
        }
    else:
        response = {
            "data": 'No se selecciono ninguna opcion'
        }

    class_obj.HttpResponse(response)