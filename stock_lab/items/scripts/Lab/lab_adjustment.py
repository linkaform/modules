# -*- coding: utf-8 -*-
import sys, simplejson

from lab_stock_utils import Stock

from account_settings import *


print('usando greeeeeenhouse....')

# lkf = self.lkf.LKFModules()

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.inventory_adjustment()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans':  stock_obj.answers
    }))
