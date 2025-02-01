# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    #current record
    product_code, sku, lot_number, warehouse, location = stock_obj.get_product_lot_location()
    stock = stock_obj.get_product_stock(product_code, sku, lot_number, warehouse, location)
    if stock.get('actuals') == 0 or stock.get('actuals') < 0:
        record_catalog = stock_obj.get_record_catalog_del()
        stock_obj.del_catalog_record(record_catalog, stock_obj.form_id)
    #doing cleanup
    #for done records that for some reason are not deleted
    # record_catalog = get_record_catalog(form_id)
    # del_catalog_record(record_catalog)

