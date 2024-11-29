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


class Reports(Reports):

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
        print('RECORD', record)
        return record
    
    
    def get_procurments(self, warehouse=None, location=None, product_code=None, sku=None, status='programmed', group_by=False):
        #product_code = ["750200301045", "750200301170"]
        match_query ={ 
                'form_id': procurment_obj.PROCURMENT,  
                'deleted_at' : {'$exists':False},
                f'answers.{procurment_obj.mf["procurment_status"]}': status,
            }
        if product_code:
            match_query.update({
                f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": {'$in': product_code}
                })
            
        # match_query.update({
        #     f"answers.{prod_obj.SKU_OBJ_ID}.{prod_obj.f['product_code']}": "750200301045"})
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

        
    def get_catalog_product_field(self, id_field):
        query = {"form_id":123150, 'deleted_at':{'$exists':False}}

        # Obtener los ids distintos y filtrar los None
        db_ids = [item for item in self.cr.distinct("answers.66dfc4d9a306e1ac7f6cd02c.61ef32bcdf0ec2ba73dec343", query) if item is not None]
        
        match_query = { 
            'deleted_at':{"$exists":False},
        }

        mango_query = {
            "selector": {
                "answers": {
                    "$and": [match_query]
                }
            },
            "limit": 10000,
            "skip": 0
        }
        
        res = script_obj.lkf_api.search_catalog(123105, mango_query)
        res_format = reorder_obj.format_catalog_product(res, id_field)
        
        return res_format
    
    
    def get_max_stock(self, data):
        res = {x['warehouse'].lower().replace(' ','_'):x['stock_maximum'] for x in data}
        return res
    
    
    def generate_report_info(self):
        products = reorder_obj.get_product_by_type(product_type=product_family, product_line=product_line)
        product_dict = {x['product_code']: x for x in products}
        product_code = list(product_dict.keys())
                    
        procurment = reorder_obj.get_procurments(product_code=product_code)

        product_stock = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_info)
        stock_cedis = stock_obj.get_products_inventory(product_code=product_code, warehouse=warehouse_cedis)
        stock_cedis_dict = {x['product_code']: x['actuals'] for x in stock_cedis}
                    
        res_first = reorder_obj.reorder_rules_warehouse(product_code=product_code)
                    
        stock_dict = {}

        for item in procurment:
            code = item['product_code']
            warehouse = item['warehouse'].lower().replace(' ', '_')
            actuals = stock_cedis_dict.get(code, 0)
            product_name = product_dict[code]['product_name']
            product_category = product_dict[code]['product_category']
            product_type = product_dict[code]['product_type']

            # Inicializa el producto en el diccionario de stock si no existe
            stock_dict[code] = stock_dict.get(code, {
                'sku': item['sku'],
                'desc_producto': product_name,
                'line': product_category,
                'familia': product_type,
                'stock_mty': 0,
                'stock_gdl': 0,
                'stock_max': 0,
                'stock_merida': 0,
                'actuals': actuals,  # proviene de stock_cedis_dict
                'percentage_stock_max': 0,
                'stock_final': actuals,  # Inicializamos stock_final con los actuales
            })

            # Actualiza stock_to_move, asegurándose de no exceder el stock disponible
            available_stock = stock_dict[code].get(f'actuals_{warehouse}', 0)
            stock_to_move = min(item['procurment_qty'], available_stock)
            stock_dict[code].update({f'stock_to_move_{warehouse}': round(stock_to_move, 2)})

            # Verificación para evitar que stock_final sea negativo
            if stock_dict[code]['stock_final'] >= item['procurment_qty']:
                # Si hay suficiente stock para el traspaso
                stock_dict[code]['stock_final'] = round(stock_dict[code]['stock_final'] - item['procurment_qty'], 2)
            else:
                # Si no hay suficiente stock, se ajusta a 0
                stock_dict[code]['stock_final'] = 0

        # Ahora actualiza la información de los productos en el stock de los almacenes
        for x in product_stock:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ', '_')
            #print('WH', code, warehouse, x['actuals'])
            if stock_dict.get(code):
                if 'cedis' in warehouse:
                    warehouse = warehouse.replace('cedis', 'alm')
                elif warehouse == 'alm_guadalajara':
                    stock_dict[code]['actuals_alm_guadalajara'] = 0
                else:
                    stock_dict[code][f'actuals_{warehouse}'] = x['actuals']
                
        # Actualiza las reglas de stock máximo para cada producto
        for x in res_first:
            code = x['product_code']
            warehouse = x['warehouse'].lower().replace(' ', '_')
            if stock_dict.get(code):
                stock_dict[code][f'stock_max_{warehouse}'] = x['stock_maximum']
                if x['stock_maximum'] == 0:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = 0
                else:
                    stock_dict[code][f'p_stock_max_{warehouse}'] = round((stock_dict[code].get(f'actuals_{warehouse}', 0) / x['stock_maximum']) * 100, 2)

        stock_list = list(stock_dict.values())
        #print('STOCK_LIST', simplejson.dumps(stock_list, indent=4))
        return stock_list
    
    
    def build_data_from_report(self, stock_list):
        data = {}
        
        # Extraer los nombres de los almacenes dinámicamente desde las claves que contienen 'stock_max_alm_'
        warehouse_keys = [key.split('stock_max_alm_')[-1] for key in stock_list[0].keys() if 'stock_max_alm_' in key]

        for product in stock_list:
            #print('///', product)
            sku = product['sku']
            data[sku] = {'actuals': product.get("actuals", 0)}
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
                            'almacen': warehouse_key,
                            'stock_max': stock_max,
                            'porce': p_stock_max,
                            'traspaso': 0,  # Suponiendo que la clave 'traspaso' es 0 por defecto
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
    
    
    def piezas_porcentaje(self, stock_max, porcentaje):
        piezas = porcentaje * stock_max / 100 *100
        return piezas
    
        
    def warehouses_by_percentage(self):
        stock_list = reorder_obj.generate_report_info()  # Obtén la información del stock
        data = reorder_obj.build_data_from_report(stock_list)  # Genera los datos del informe

        traspaso_por_product = {}

        # Itera sobre los productos
        for key, value in data.items():
            warehouses_by_percentage_dict = {}  # Diccionario para almacenar los datos de los almacenes por porcentaje
            stock_cedis = value['actuals']  # El stock total disponible para mover

            # Itera sobre los almacenes de cada producto
            for warehouse_key, warehouse_data in value.items():
                if isinstance(warehouse_data, dict) and 'porce' in warehouse_data:
                    porce = warehouse_data['porce']
                    warehouses_by_percentage_dict[porce] = warehouse_data  # Almacena la información del almacén en el diccionario

            percentage_list = sorted(warehouses_by_percentage_dict.keys())  # Ordena los porcentajes

            # Realiza los cálculos de traspaso para cada almacén
            for idx, f in enumerate(percentage_list):
                stock_warehouse = warehouses_by_percentage_dict[f]
                porce = stock_warehouse['porce']
                actuals = stock_warehouse['actuals']
                warehouse = stock_warehouse['almacen']

                # Verificar si el porcentaje es cero para evitar la división por cero
                if porce == 0:
                    traspaso = 0
                else:
                    # Cálculo del traspaso
                    traspaso = (100 * actuals / porce) - actuals

                # Evitar traspasos negativos
                if traspaso < 0:
                    traspaso = 0

                warehouses_by_percentage_dict[f]['traspaso'] = traspaso

            # Aquí se guarda el traspaso por producto de forma única
            traspaso_por_product[key] = warehouses_by_percentage_dict
            
        return traspaso_por_product
            
    
    def warehouse_transfer_update(self):
        stock_list_response = reorder_obj.generate_report_info()  # Lista de productos
        warehouse_percentage_response = reorder_obj.warehouses_by_percentage()  # Respuesta con almacenes y traspasos
        
        # Iteramos sobre cada producto en warehouse_percentage_response
        for sku, warehouses in warehouse_percentage_response.items():
            # Buscamos el producto correspondiente en stock_list_response por SKU
            product = next((p for p in stock_list_response if p['sku'] == sku), None)
            
            if product:  # Si encontramos el producto correspondiente
                total_traspaso = 0  # Variable para acumular los traspasos del producto

                # Iteramos sobre los almacenes y traspasos del producto
                for percentage_key, percentage_value in warehouses.items():
                    if 'almacen' in percentage_value:
                        almacen = percentage_value['almacen']  # Nombre del almacén
                        traspaso = percentage_value['traspaso']  # Valor del traspaso
                        
                        # Creamos la clave correspondiente para el almacén en el producto
                        stock_key = f"stock_to_move_alm_{almacen}"
                        
                        if traspaso > product['actuals']:
                            traspaso = 0

                        # Verificamos si la clave del almacén existe en el producto y la actualizamos
                        if stock_key in product:
                            product[stock_key] = round(traspaso, 2)  # Actualizamos el valor redondeado a 2 decimales
                            total_traspaso += traspaso  # Acumulamos el traspaso
                        
                # Después de calcular los traspasos, actualizamos el stock_final
                if 'actuals' in product:  # Verificamos si 'actuals' existe en el producto
                    stock_final = round(product['actuals'] - total_traspaso, 2)
                    
                    # Si el stock_final es negativo, lo establecemos en 0
                    if stock_final < 0:
                        stock_final = round(product['actuals'])
                        
                    product['stock_final'] = stock_final  # Asignamos el valor final al producto
                    
                if 'actuals_alm_guadalajara' not in product:
                    product['actuals_alm_guadalajara'] = 0
                    
        #print(simplejson.dumps(stock_list_response, indent=4))            
        return stock_list_response
    
    
    def get_total_p_max_gdl(self):
        stock_list_response = reorder_obj.warehouse_transfer_update()
        for p in stock_list_response:
            actuals = p['actuals']
            before_stock_final = p['stock_final']
            mty_transfer = p.get('stock_to_move_alm_monterrey', 0)
            merida_transfer = p.get('stock_to_move_alm_merida', 0)
            stock_max_alm_guadalajara = p.get('stock_max_alm_guadalajara', 0)
            
            if p['actuals_alm_guadalajara'] == 0:
                if before_stock_final > stock_max_alm_guadalajara:
                    p['stock_to_move_alm_guadalajara'] = stock_max_alm_guadalajara
                
                    if p['stock_to_move_alm_guadalajara'] != 0:
                        total_p_max_gdl = 0
                        # total_p_max_gdl = round((stock_max_alm_guadalajara / before_stock_final)*100,2)
                        p['p_stock_max_alm_guadalajara'] = total_p_max_gdl
                                        
                    after_stock_final_one = round(actuals - mty_transfer - stock_max_alm_guadalajara - merida_transfer, 2)
                    p['stock_final'] = after_stock_final_one
            
            else:
                total_p_max_gdl = round((p['actuals_alm_guadalajara'] / before_stock_final) * 100, 2)
                p['p_stock_max_alm_guadalajara'] = total_p_max_gdl
                
                transfer_gdl = round((100 * p['actuals_alm_guadalajara'] / p['p_stock_max_alm_guadalajara']) - p['actuals_alm_guadalajara'], 2)
                p['stock_to_move_alm_guadalajara'] = transfer_gdl
            
                after_stock_final_two = round(actuals - mty_transfer - p['stock_to_move_alm_guadalajara'] - merida_transfer, 2)
                p['stock_final'] = after_stock_final_two
            
        #print(simplejson.dumps(stock_list_response, indent=4))
        return stock_list_response
        
        
if __name__ == "__main__":
    reorder_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    procurment_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    warehouse_obj = Warehouse(settings, sys_argv=sys.argv, use_api=True)

    data = reorder_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    #option = 'get_report'
    product_family = data.get('product_family', 'TUBOS')
    product_line = data.get('product_line', '')
    warehouse_info = data.get('warehouse', '')
    warehouse_cedis = 'CEDIS GUADALAJARA'

    if option == 'get_report':
        #reorder_obj.get_total_p_max_gdl()
        script_obj.HttpResponse({
            "stockInfo": reorder_obj.get_total_p_max_gdl(),
        })

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