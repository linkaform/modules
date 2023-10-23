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
    print('data=', report_obj.data.get('data'))
    options = data.get("option",0)
    if option == 'getFilters'
    report_obj.get_product_kardex()

