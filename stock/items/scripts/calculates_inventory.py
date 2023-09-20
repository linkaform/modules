# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime
from linkaform_api import base

from lkf_addons.addons.stock.stock_utils import Stock

from account_settings import *



if __name__ == '__main__':
    print('asi entra=', sys.argv)
    base = base.LKF_Base(settings, sys.argv)
    current_record = base.current_record
    USER_ID = base.current_record['user_id']
    print('current_record=', current_record)
    print('USER_ID=', USER_ID)
    settings.config.update(config)
    stock_obj = Stock(settings)
    folio =current_record.get('folio')
    kwargs = current_record.get('kwargs', current_record.get('properties',{}).get('kwargs', {}))

    if folio:
        #si ya existe el registro, no cambio el numero de lote
        kwargs['force_lote'] = True
    print('calcluates inv kwargs', kwargs)
    answers = stock_obj.get_product_info(current_record['answers'],current_record.get('folio'), kwargs=kwargs)
    current_record['answers'].update(answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': current_record['answers']
    }))
