# -*- coding: utf-8 -*-
import sys, simplejson
from linkaform_api import settings, network, utils

#from account_utils import get_plant_recipe, select_S4_recipe, get_record_greenhouse_inventory
from account_settings import *

from lkf_addons.addons.stock_greenhouse.stock_utils import Stock


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    status_code = stock_obj.do_scrap()
    stock_obj.answers[stock_obj.f['inv_scrap_status']] = 'done'
    update_ok = False
    print()
    if status_code.get('status_code') == 202:
        stock_obj.answers[stock_obj.f['inv_scrap_status']] = 'done'
        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans':  stock_obj.answers
        }))

    # try:
    #     update_ok = status_code.raw_result['updatedExisting']
    # except:
    #     stock_obj.LKFException( simplejson.dumps( "Error updating scrap" ) )

    # if update_ok:
    #     sys.stdout.write(simplejson.dumps({
    #         'status': 101,
    #         'replace_ans': stock_obj.answers
    #     }))

    # else:
    #     msg = "One or more of the moves were not executed correctly"
    #     msg_error_app = {
    #             "63f8e128694361f17f7b59d5": {
    #                 "msg": [msg],
    #                 "label": "Please check stock moves",
    #                 "error":[]

    #             }
    #         }
    #     stock_obj.LKFException( simplejson.dumps( msg_error_app ) )
 