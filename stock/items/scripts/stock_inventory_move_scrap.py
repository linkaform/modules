# -*- coding: utf-8 -*-
import sys, simplejson
from linkaform_api import settings, network, utils

#from account_utils import get_plant_recipe, select_S4_recipe, get_record_greenhouse_inventory
from account_settings import *

from lkf_addons.addons.stock.stock_utils import Stock


if __name__ == '__main__':
    print(sys.argv)
    stock_obj = Stock(settings, sys_argv=sys.argv)
    current_record = stock_obj.current_record
    status_code = stock_obj.do_scrap(current_record)

    record_id =  current_record['_id']['$oid']
    current_record.pop('_id')
    current_record = stock_obj.lkf_api.drop_fields_for_patch(current_record)
    current_record['answers'][stock_obj.f['inv_scrap_status']] = 'done'
    response = stock_obj.lkf_api.patch_record(current_record, record_id)
    status_code = response.get('status_code')
    if status_code != 202:
        msg = "One or more of the moves were not executed correctly"
        msg_error_app = {
                "63f8e128694361f17f7b59d5": {
                    "msg": [msg],
                    "label": "Please check stock moves",
                    "error":[]

                }
            }
        raise Exception( simplejson.dumps( msg_error_app ) )

