# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime
from bson import ObjectId

from linkaform_api import settings, network, utils
from lkf_addons.addons.stock.stock_utils import Stock

from account_settings import *



if __name__ == '__main__':
    print('asi entra=', sys.argv)
    stock_obj = Stock(settings, sys_argv=sys.argv)
    current_record = stock_obj.current_record
    folio =current_record.get('folio')
    # print('current_record',current_record)

    # if folio:
    #     #si ya existe el registro, no cambio el numero de lote
    #     kwargs['force_lote'] = True
    answers = stock_obj.get_product_info(current_record['answers'],current_record.get('folio'))
    query = None
    print('en el calculate inventory......', answers.get(stock_obj.f['product_lot_actuals']))
    _id = current_record.get('connection_record_id',{}).get('$oid')
    print('_id', _id)
    if _id:
        query = {'_id':ObjectId(_id)}
    if not query and folio:
        query = {'folio':folio, 'form_id':current_record['form_id'] }
    if query:
        stock_obj.cr.update_one(query, {'$set': {'answers':answers}})
    current_record['answers'].update(answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': current_record['answers']
    }))
