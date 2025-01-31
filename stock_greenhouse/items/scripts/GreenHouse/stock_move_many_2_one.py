# -*- coding: utf-8 -*-
import sys, simplejson

from lkf_addons.addons.stock_greenhouse.app import Stock

from account_settings import *


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    if not stock_obj.record_id:
        stock_obj.record_id = stock_obj.object_id() 
    stock_obj.move_multi_2_one_location(move_type='sale')
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        'metadata':{"id":stock_obj.record_id}
        }))
