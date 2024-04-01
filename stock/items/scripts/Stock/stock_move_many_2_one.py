# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    response = stock_obj.move_one_many_one()
    print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
