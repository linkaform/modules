 # -*- coding: utf-8 -*-
import sys, simplejson, math
import json
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

print('inicia....')
from jit_report import Reports
from lkf_addons.addons.product.app import Product
from lkf_addons.addons.product.app import Warehouse
from lkf_addons.addons.jit.app import JIT
from lkf_addons.addons.stock.app import Stock
from itertools import zip_longest


def get_report_filters(filters=[], product_code_aux=None):
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

def get_procurments(warehouse=None, location=None, product_code=None, sku=None, status='programmed', group_by=False):
    match_query ={ 
            'form_id': procurment_obj.PROCURMENT,  
            'deleted_at' : {'$exists':False},
            f'answers.{procurment_obj.mf["procurment_status"]}': status,
        }
    if product_code:
        match_query.update({
            f"answers.{procurment_obj.SKU_OBJ_ID}.{procurment_obj.f['product_code']}": {'$in': product_code}
            })
    if sku:
        match_query.update({
            f"answers.{procurment_obj.SKU_OBJ_ID}.{procurment_obj.f['sku']}":sku
            })
    if warehouse:
        match_query.update({
            f"answers.{procurment_obj.WH.WAREHOUSE_LOCATION_OBJ_ID}.{procurment_obj.WH.f['warehouse']}":warehouse
            })
    if location:
        match_query.update({
            f"answers.{procurment_obj.WH.WAREHOUSE_LOCATION_OBJ_ID}.{procurment_obj.WH.f['warehouse_location']}":location
            })
    query = [
        {'$match': match_query},
        {'$project':{
                '_id':0,
                'date':f'$answers.{procurment_obj.mf["procurment_date"]}',
                'date_schedule':f'$answers.{procurment_obj.mf["procurment_schedule_date"]}',
                'procurment_method':f'$answers.{procurment_obj.mf["procurment_method"]}',
                'procurment_qty':f'$answers.{procurment_obj.mf["procurment_qty"]}',
                'product_code':f'$answers.{procurment_obj.SKU_OBJ_ID}.{procurment_obj.f["product_code"]}',
                'sku':f'$answers.{procurment_obj.SKU_OBJ_ID}.{procurment_obj.f["sku"]}',
                'uom':f'$answers.{procurment_obj.UOM_OBJ_ID}.{procurment_obj.f["uom"]}',
                'warehouse':f'$answers.{procurment_obj.WH.WAREHOUSE_LOCATION_OBJ_ID}.{procurment_obj.WH.f["warehouse"]}',
                'warehouse_location':f'$answers.{procurment_obj.WH.WAREHOUSE_LOCATION_OBJ_ID}.{procurment_obj.WH.f["warehouse_location"]}',
        }}]
    return procurment_obj.format_cr(procurment_obj.cr.aggregate(query))

def get_product(product_code):
    return get_product_field(product_code, pfield='*')

def get_product_field(product_code, pfield='product_family'):
    product_field = None
    mango_query = {
        "selector": {
            "answers": {
                '61ef32bcdf0ec2ba73dec33d': {"$eq": product_code},
                } ,
            },
        "limit": 1,
        "skip": 0
            }
    record = prod_obj.lkf_api.search_catalog(prod_obj.PRODUCT_ID, mango_query)
    if record and len(record) > 0:
        rec = record[0]
        if pfield == '*':
            return rec
        product_field = rec.get(prod_obj.f[pfield])
    return product_field

def format_catalog_product(data_query, id_field):
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
    res_format = format_catalog_product(res, id_field)
    return res_format


if __name__ == "__main__":
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    procurment_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    warehouse_obj = Warehouse(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()

    data = report_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    status = 'active'

    warehouse = data.get('warehouse', '')
    product_family = data.get('product_family', '')
    product_line = data.get('product_line', '')
    # familia = data.get('familia', '')

    if option == 'get_report':
        products = prod_obj.get_product_by_type(product_family)
        product_dict = {x['product_code']:x for x in products}
        #print('///////////product dict', product_dict)
        product_code = list(product_dict.keys())
        #print('///////PRODUCT CODES', product_code)
        procurment = get_procurments(product_code=product_code)
        #print('///////PROCURMENT', procurment)
        product_stock = stock_obj.ger_products_inventory(product_code=product_code, warehouse=warehouse)
        res_first = report_obj.reorder_rules_warehouse(product_code=product_code)
        
        stock_dict = {}
        product_code = []
        
        for item in procurment:
            if item['product_code'] not in product_code:
                product_code.append(item['product_code'])
                code = item['product_code']
                warehouse = item['warehouse']
                
                product_name = product_dict[code]['product_name']
                product_category = product_dict[code]['product_category']
                product_type = product_dict[code]['product_type']

                stock_dict[warehouse] = stock_dict.get(warehouse, {})
                stock_dict[warehouse][code] = stock_dict.get(code,
                    {
                        'sku': item['sku'],
                        'desc_producto': product_name,
                        'line': product_category,
                        'familia': product_type,
                        'stock_to_move': item['procurment_qty'],
                        'stock_mty': 0,
                        'p_stock_min_mty': 0,
                        'stock_gdl': 0,
                        'stock_max': 0,
                        'p_stock_min_gdl': 0,
                        'stock_merida': 0,
                        'p_stock_min_merida': 0,
                        'actuals': 0,
                        'percentage_stock_max': 0,
                        'stock_final': 0,
                    }
                )
        
        for x in product_stock:
            code = x['product_code']
            warehouse = x['warehouse']
            if stock_dict.get(warehouse):
                if stock_dict[warehouse].get(code):
                    stock_dict[warehouse][code]['actuals'] = x['actuals']

                    if warehouse == 'ALM MONTERREY':
                        stock_dict[warehouse][code]['stock_mty'] = x['actuals']
                    elif warehouse == 'ALM GUADALAJARA':
                        stock_dict[warehouse][code]['stock_gdl'] = x['actuals']
                    elif warehouse == 'ALM MERIDA':
                        stock_dict[warehouse][code]['stock_merida'] = x['actuals']            
        
        for x in res_first:
            code = x['product_code']
            warehouse = x['warehouse']
            if stock_dict.get(warehouse):
                if stock_dict[warehouse].get(code):
                    if warehouse == 'ALM MONTERREY':
                        stock_dict[warehouse][code]['stock_max_ALM MONTERREY'] = x['stock_maximum']
                    elif warehouse == 'ALM GUADALAJARA':
                        stock_dict[warehouse][code]['stock_max_ALM GUADALAJARA'] = x['stock_maximum']
                    elif warehouse == 'ALM MERIDA':
                        stock_dict[warehouse][code]['stock_max_ALM MERIDA'] = x['stock_maximum']
                    
                    # stock_dict[warehouse][code]['stock_max'] = x['stock_maximum']
                    # stock_dict[warehouse][code]['percent_stock_max'] = round((stock_dict[warehouse][code]['actuals'] / x['stock_maximum']) * 100, 2)
                    
                    if warehouse == 'ALM MONTERREY':
                        stock_dict[warehouse][code]['p_stock_max_ALM MONTERREY'] =  stock_dict[warehouse][code]['percent_stock_max'] = round((stock_dict[warehouse][code]['actuals'] / x['stock_maximum']) * 100, 2)
                    elif warehouse == 'ALM GUADALAJARA':
                        stock_dict[warehouse][code]['p_stock_max_ALM GUADALAJARA'] =  stock_dict[warehouse][code]['percent_stock_max'] = round((stock_dict[warehouse][code]['actuals'] / x['stock_maximum']) * 100, 2)
                    elif warehouse == 'ALM MERIDA':
                        stock_dict[warehouse][code]['p_stock_max_ALM MERIDA'] =  stock_dict[warehouse][code]['percent_stock_max'] = round((stock_dict[warehouse][code]['actuals'] / x['stock_maximum']) * 100, 2)
                    
        stock_list = []

        for almacen, productos in stock_dict.items():
            for sku, detalles in productos.items():
                # print(detalles.keys())
                stock_list.append({
                    'sku': detalles['sku'],
                    'desc_producto': detalles['desc_producto'],
                    'line': detalles['line'],
                    'familia': detalles['familia'],
                    # 'stock_cedis': detalles['stock_cedis'],
                    'stock_ALM MONTERREY': detalles['stock_mty'],
                    'p_stock_min_ALM MONTERREY': detalles.get('p_stock_max_ALM MONTERREY', 0),
                    'stock_ALM GUADALAJARA': detalles['stock_gdl'],
                    #'stock_max': detalles['stock_max'],
                    'p_stock_min_ALM GUADALAJARA': detalles.get('p_stock_max_ALM GUADALAJARA', 0),
                    'stock_ALM MERIDA': detalles['stock_merida'],
                    'p_stock_min_ALM MERIDA': detalles.get('p_stock_max_ALM MERIDA', 0),
                    #'actuals': detalles['actuals'],
                    #'percentage_stock_max': detalles['percentage_stock_max'],
                    'stock_final': detalles['stock_final'],  
                    'stock_to_move': detalles['stock_to_move'],
                })
        
        script_obj.HttpResponse({
            "stockInfo": stock_list,
        })


    elif option == 'get_catalog':
        warehouse_types_catalog = warehouse_obj.get_all_stock_warehouse()
        product_type = get_catalog_product_field(id_field='61ef32bcdf0ec2ba73dec343')
        
        script_obj.HttpResponse({
            "dataCatalogWarehouse": warehouse_types_catalog,
            "dataCatalogProductFamily": product_type,
        })

    elif option == 'get_product_line':
        filters = ['inventory',]
        product_code_aux = data.get("product_code")
        products_categorys = get_report_filters(filters, product_code_aux=product_code_aux)

        script_obj.HttpResponse({
            "product_line": products_categorys,
        })