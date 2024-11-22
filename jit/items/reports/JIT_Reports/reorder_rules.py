# -*- coding: utf-8 -*-
import sys, simplejson, math
import json
import math
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

from jit_report import Reports
from lkf_addons.addons.product.app import Product
from lkf_addons.addons.product.app import Warehouse
from lkf_addons.addons.jit.app import JIT
from lkf_addons.addons.stock.app import Stock
from itertools import zip_longest


class Reorder_Rules:

    def get_product_by_type(self, product_type, product_line=""):
        product_field = None
        # Base query with both product_type and product_line (if not empty)
        mango_query = {
            "selector": {
                "$and": [
                    {"answers." + prod_obj.f['product_type']: {"$eq": product_type}},
                    # Only add the product_line filter if it's not empty
                    {"answers." + prod_obj.f['product_category']: {"$eq": product_line}} if product_line else {}
                ]
            },
            "limit": 10000,
            "skip": 0
        }
    
        # Clean up any empty $and conditions if product_line was empty
        mango_query["selector"]["$and"] = [clause for clause in mango_query["selector"]["$and"] if clause]

        # Execute query and return records
        record = prod_obj._labels_list(prod_obj.lkf_api.search_catalog(prod_obj.PRODUCT_ID, mango_query), prod_obj.f)
        return record
    
    
    def get_procurments(self, warehouse=None, location=None, product_code=None, sku=None, status='programmed', group_by=False):
        match_query ={ 
                'form_id': procurment_obj.PROCURMENT,  
                'deleted_at' : {'$exists':False},
                f'answers.{procurment_obj.mf["procurment_status"]}': status,
            }
        # if product_code:
        #     match_query.update({
        #         f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": {'$in': product_code}
        #         })
            
        match_query.update({
                f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": '750200301045'})
        if sku:
            match_query.update({
                f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['sku']}":sku
                })
        if warehouse:
            match_query.update({
                f"answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f['warehouse']}":warehouse
                })
        if location:
            match_query.update({
                f"answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f['warehouse_location']}":location
                })
        query = [
            {'$match': match_query},
            {'$project':{
                    '_id':0,
                    'date':f'$answers.{procurment_obj.mf["procurment_date"]}',
                    'date_schedule':f'$answers.{procurment_obj.mf["procurment_schedule_date"]}',
                    'procurment_method':f'$answers.{procurment_obj.mf["procurment_method"]}',
                    'procurment_qty':f'$answers.{procurment_obj.mf["procurment_qty"]}',
                    'product_code':f'$answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f["product_code"]}',
                    'sku':f'$answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f["sku"]}',
                    'uom':f'$answers.{procurment_obj.UOM_OBJ_ID}.{procurment_obj.f["uom"]}',
                    'warehouse':f'$answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f["warehouse"]}',
                    'warehouse_location':f'$answers.{warehouse_obj.WAREHOUSE_LOCATION_OBJ_ID}.{warehouse_obj.f["warehouse_location"]}',
            }}]
        return procurment_obj.format_cr(procurment_obj.cr.aggregate(query))


    def get_report_filters(self, filters=[], product_code_aux=None):
        mango_query = {
            "selector": {
                "_id": {"$gt": None}
            },
            "limit": 10000,
            "skip": 0
        }

        if 'inventory' in filters:
            if product_code_aux:
                mango_query['selector'] = {f"answers.{prod_obj.f['product_type']}": product_code_aux}

            res = prod_obj.lkf_api.search_catalog(123105, mango_query)

            product_categories = []

            for item in res:
                category = item.get(prod_obj.f['product_category'])
                if category and category not in product_categories:
                    product_categories.append(category)

        return product_categories
    
    
    def format_catalog_product(self, data_query, id_field):
        list_response = []
        for item in data_query:
            wharehouse = item.get(id_field,'')
            if wharehouse not in list_response and wharehouse !='':
                list_response.append(wharehouse)

        list_response.sort()
        return list_response

        
    def get_catalog_product_field(id_field):
        match_query = { 
            'deleted_at':{"$exists":False},
        }

        mango_query = {"selector":
            {"answers":
                {"$and":[match_query]}
            },
            "limit":10000,
            "skip":0
        }
        res = script_obj.lkf_api.search_catalog( 123105, mango_query)
        res_format = reorder_obj.format_catalog_product(res, id_field)
        return res_format
    
    
    def get_max_stock(self, data):
        res = {x['warehouse'].lower().replace(' ','_'):x['stock_maximum'] for x in data}
        return res
    
    
    def generate_report_info(self):
        products = reorder_obj.get_product_by_type(product_type=product_family, product_line=product_line)
        product_dict = {x['product_code']:x for x in products}
        product_code = list(product_dict.keys())
                
        procurment = reorder_obj.get_procurments(product_code=product_code)
        
        product_stock = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_info)
        stock_cedis = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_cedis)
        stock_cedis_dict = {x['product_code']:x['actuals'] for x in stock_cedis}
        
        res_first = report_obj.reorder_rules_warehouse(product_code=product_code)
        
        stock_dict = {}
            
        for item in procurment:
            code = item['product_code']
            warehouse = item['warehouse'].lower().replace(' ','_')
            actuals = stock_cedis_dict.get(code, 0)
            product_name = product_dict[code]['product_name']
            product_category = product_dict[code]['product_category']
            product_type = product_dict[code]['product_type']
            

            stock_dict[code] = stock_dict.get(code,
                {
                    'sku': item['sku'],
                    'desc_producto': product_name,
                    'line': product_category,
                    'familia': product_type,
                    'stock_mty': 0,
                    'stock_gdl': 0,
                    'stock_max': 0,
                    'stock_merida': 0,
                    'actuals': actuals, #   proviene de stock_cedis_dict
                    'percentage_stock_max': 0,
                    'stock_final': actuals,
                }
            )
        
            stock_dict[code].update({f'stock_to_move_{warehouse}': round(item['procurment_qty'], 2)})
            stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - item['procurment_qty'],2)
                
        for x in product_stock:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ','_')
            if stock_dict.get(code):
                stock_dict[code][f'actuals_{warehouse}'] = x['actuals']
            
        for x in res_first:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ','_')
            if stock_dict.get(code):
                stock_dict[code]['rules'] = stock_dict[code].get('rules',[])
                stock_dict[code]['rules'].append(x)
                stock_dict[code][f'stock_max_{warehouse}'] = x['stock_maximum']
                if x[f'stock_maximum'] == 0:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = 0
                else:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = round((stock_dict[code].get(f'actuals_{warehouse}', 0) / x[f'stock_maximum']) * 100, 2)
        
        #buscar negativo
        for pcode, rec in stock_dict.items():
            stock_final = rec['stock_final']
            if stock_final < 0 :
                p_stock_max = [v for k,v in rec.items() if 'p_stock_max_' in k]
                alm_max_stock = reorder_obj.get_max_stock(rec['rules'])
                    
        stock_list = list(stock_dict.values())
        return stock_list
    
    
    def build_data_from_report(self, stock_list):
        data = {}
        
        # Extraer los nombres de los almacenes dinámicamente desde las claves que contienen 'stock_max_alm_'
        warehouse_keys = [key.split('stock_max_alm_')[-1] for key in stock_list[0].keys() if 'stock_max_alm_' in key]

        for product in stock_list:
            sku = product['sku']
            data[sku] = {}
            # print('PROD', data)
            # Iterar sobre los almacenes extraídos dinámicamente
            for warehouse_key in warehouse_keys:
                # Usamos las claves formateadas para acceder a la información de stock final y max
                warehouse_code = f'alm_{warehouse_key}'

                # Si el almacén está en los datos del producto, lo agregamos al diccionario
                if f'stock_max_{warehouse_code}' in product:
                    # Obtengo los valores del stock y los demás parámetros
                    stock_max = product.get(f'stock_max_{warehouse_code}', 0)
                    actuals = product.get(f'actuals_{warehouse_code}', 0)
                    p_stock_max = product.get(f'p_stock_max_{warehouse_code}', 0)
                    
                    # Si el almacén no existe en el diccionario, lo creamos
                    if warehouse_key not in data:
                        data[sku][warehouse_key] = {
                            'almancen': warehouse_key,
                            'stock_max': stock_max,
                            'porce': p_stock_max,
                            'transpaso': 0,  # Suponiendo que la clave 'transpaso' es 0 por defecto
                            'actuals': actuals
                        }
                    else:
                        # Si el almacén ya existe, solo actualizamos los valores de stock_max, porce, actuals
                        data[sku][warehouse_key]['stock_max'] = stock_max
                        data[sku][warehouse_key]['porce'] = p_stock_max
                        data[sku][warehouse_key]['actuals'] = actuals
            
        return data
    
    
    def get_stock_data(self, data, idx):
        stock_warehouse = data[idx]
        stock_max = stock_warehouse['stock_max']
        return stock_max
    
    
    # def warehouses_by_percentage_two(self, warehouses_by_percentage_dict, percentage_list):
    #     stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[1])
    #     stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
    #     stock_cedis -= stock_qty
    #     if stock_cedis <= 0:
    #         pass
    #     warehouses_by_percentage_dict[percentage_list[1]]['transpaso'] += stock_qty
    #     return warehouses_by_percentage_dict
        
    
    # def warehouses_by_percentage_three(self, warehouses_by_percentage_dict, percentage_list):
    #     stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[2])
    #     stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
    #     stock_cedis -= stock_qty
    #     if stock_cedis <= 0:
    #         pass
    #     warehouses_by_percentage_dict[percentage_list[1]]['transpaso'] += stock_qty
    #     return warehouses_by_percentage_dict
        
    
    def warehouses_by_percentage(self):
        stock_list = reorder_obj.generate_report_info()  # Obtén la información del stock
        actuals = sum(product['actuals'] for product in stock_list)  # Suma los 'actuals' de todos los productos
        data = reorder_obj.build_data_from_report(stock_list)  # Genera los datos del informe
        percentage_list = []  # Lista para almacenar los porcentajes
        warehouses_by_percentage_dict = {}  # Diccionario para almacenar los datos de los almacenes por porcentaje

        # Itera sobre los productos
        for key, value in data.items():
            # Itera sobre los almacenes de cada producto
            for warehouse_key, warehouse_data in value.items():
                if 'porce' in warehouse_data:
                    porce = warehouse_data['porce']
                    percentage_list.append(porce)  # Agrega el porcentaje a la lista
                    warehouses_by_percentage_dict[porce] = warehouse_data  # Almacena la información del almacén en el diccionario

        percentage_list.sort()  # Ordena la lista de porcentajes
        stock_cedis = actuals  # Inicializa la cantidad de stock en CEDIS

        # Ahora realiza las operaciones de acuerdo a los porcentajes
        for idx, f in enumerate(percentage_list):
            if (idx + 1) >= len(percentage_list):
                diff = round(f - 100)  # Calcula la diferencia al 100%
                stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[0])
                stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
                stock_cedis -= stock_qty
                if stock_cedis <= 0:
                    break
                warehouses_by_percentage_dict[percentage_list[0]]['transpaso'] += stock_qty

                # Realiza la misma operación para el siguiente almacén
                stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[1])
                stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
                stock_cedis -= stock_qty
                if stock_cedis <= 0:
                    break
                warehouses_by_percentage_dict[percentage_list[1]]['transpaso'] += stock_qty

                # Y lo mismo para el siguiente almacén
                stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[2])
                stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
                stock_cedis -= stock_qty
                if stock_cedis <= 0:
                    break
                warehouses_by_percentage_dict[percentage_list[1]]['transpaso'] += stock_qty
                continue

            stock_warehouse = warehouses_by_percentage_dict[f]
            diff = round(percentage_list[idx + 1] - percentage_list[idx])
            diff_percentage = diff / 100

            if idx == 1:
                for r in range(diff):
                    stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[0])
                    stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
                    stock_cedis -= stock_qty
                    if stock_cedis <= 0:
                        break
                    warehouses_by_percentage_dict[percentage_list[0]]['transpaso'] += stock_qty

                    stock_max = reorder_obj.get_stock_data(warehouses_by_percentage_dict, percentage_list[1])
                    stock_qty = reorder_obj.piezas_porcentaje(stock_max, .01)
                    stock_cedis -= stock_qty
                    if stock_cedis <= 0:
                        break
                    warehouses_by_percentage_dict[percentage_list[1]]['transpaso'] += stock_qty
            else:
                stock_max = stock_warehouse['stock_max']
                stock_qty = reorder_obj.piezas_porcentaje(stock_max, diff_percentage)
                stock_cedis -= stock_qty
                if stock_cedis <= 0:
                    warehouses_by_percentage_dict[percentage_list[0]]['transpaso'] += stock_cedis
                    break
                warehouses_by_percentage_dict[percentage_list[0]]['transpaso'] += stock_qty

        print('almacenes_por_porcentaje', warehouses_by_percentage_dict)
        return warehouses_by_percentage_dict
        
    def piezas_porcentaje(self, stock_max, porcentaje):
        piezas = porcentaje * stock_max / 100 *100
        return piezas
        
        
if __name__ == "__main__":
    reorder_obj = Reorder_Rules()
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    procurment_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    warehouse_obj = Warehouse(settings, sys_argv=sys.argv, use_api=True)

    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    data = report_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    product_family = data.get('product_family', 'TUBOS')
    product_line = data.get('product_line', '')
    warehouse_info = data.get('warehouse', '')
    warehouse_cedis = 'CEDIS GUADALAJARA'

    if option == 'get_report':
        reorder_obj.warehouses_by_percentage()
        # script_obj.HttpResponse({
        #     "stockInfo": reorder_obj.generate_report_info(),
        # })

    elif option == 'get_catalog':
        warehouse_types_catalog = warehouse_obj.get_all_stock_warehouse()
        product_type = reorder_obj.get_catalog_product_field(id_field='61ef32bcdf0ec2ba73dec343')
        
        script_obj.HttpResponse({
            "dataCatalogWarehouse": warehouse_types_catalog,
            "dataCatalogProductFamily": product_type,
        })

    elif option == 'get_product_line':
        filters = ['inventory',]
        product_code_aux = data.get("product_code")
        products_categorys = reorder_obj.get_report_filters(filters, product_code_aux=product_code_aux)

        script_obj.HttpResponse({
            "product_line": products_categorys,
        })