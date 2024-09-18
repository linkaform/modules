# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from stock_report import Reports

from account_settings import *

if __name__ == '__main__':
    report_obj = Reports(settings, sys_argv=sys.argv)
    report_obj.console_run()
    data, actuals = report_obj.get_product_kardex()
    print("data=", data)
    print("actuals", actuals)
    actuals = str(actuals)
    res = {'response':data, 'product_code':product_code, 'lot_number': lot_number, 'actuals':actuals}
    sys.stdout.write(simplejson.dumps(res))

