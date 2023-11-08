# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings
from account_settings import *

from lkf_addons.addons.stock_greenhouse.stock_reports import Reports

    


if __name__ == '__main__':
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    #getFilters
        
    option = report_obj.data.get('data').get("option",0)
    if option == 'getFilters':
        filters = ['products','warehouse']
        filter_data = report_obj.get_report_filters(filters)
    else:
        report_obj.json['firstElement'] = {'data':[]}
        report_obj.json['secondElement'] = {'data':report_obj.get_product_kardex()}


    sys.stdout.write(simplejson.dumps(report_obj.report_print()))

