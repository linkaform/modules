# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    
    try:
        response = stock_obj.move_one_many_one()
        stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    
    except Exception as e:
        stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'error'
        stock_obj.answers[stock_obj.f['stock_move_comments']] =  e
        print(e)
        print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
