# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings
from account_settings import *

print('inicia....')
from jit_report import Reports
from lkf_addons.addons.product.app import Product
from lkf_addons.addons.stock.app import Stock




if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    prod_obj = Product(settings, sys_argv=sys.argv, use_api=True)
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    #-FILTROS
    data = report_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    status = 'active'
    # res_first = get_elements(status)
    # print('info: ', res_first)
    # #-FILTROS
    # data = script_obj.data
    # data = data.get('data',[])
    # option = data.get('option','get_report')
    print('-------------')
    warehouse = data.get('warehouse', 'ALM MONTERREY')
    product_family = data.get('product_family', 'TUBOS')
    product_line = data.get('product_line', '')
    # familia = data.get('familia', '')
    if option == 'get_report':
        # products = prod_obj.get_product_by_type(product_family)
        # product_code = [x['product_code'] for x in products]
        # print('product_code', product_code)
        product_code = ['750200301168','750200301047','750200301003']
        product_stock = stock_obj.ger_products_inventory(product_code=product_code, warehouse=warehouse)
        print('product_stock', product_stock)
        res_first = report_obj.reorder_rules_warehouse(product_code=product_code)
        print('res_first', res_first)

    #     '''
    #     script_obj.HttpResponse({
    #         "firstElement":res_first,
    #     })
    #     '''
    # elif option == 'get_catalog':
    #     #res_catalog = get_catalog_wharehouse()
    #     print('Hola catalogo')
    #     '''
    #     script_obj.HttpResponse({
    #         "dataCatalog":res_catalog,
    #     })
    #     '''   