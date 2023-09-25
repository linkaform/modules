# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings, network, utils
from lkf_addons.addons.stock.stock_utils import Stock

from account_settings import *
#from account_utils import get_plant_recipe, select_S4_recipe, set_lot_ready_week
#from stock_utils import *

# lkf = self.lkf.LKFModules()

# # GREENHOUSE_GRADING = lkf.get_form_id('green house')#100513
# GREENHOUSE_GRADING = 99536
# GREENHOUSE_SCRAPPING = 99536
# CATALOG_STOCK_ID = 98230
# CATALOG_STOCK_OBJ_ID='6442cbafb1b1234eb68ec178'

# fdict = {
#     'cat_stock_folio':'62c44f96dae331e750428732',
#     'grading_group':'644bf7ccfa9830903f087867',
#     'grading_move_type':'64d5550ec4909ab3c20c5806',
#     'grading_ready_yearweek':'64edf8aeffeaaa1febca2a06',
#     'grading_flats':'644bf9a04b1761305b080013',
#     'grading_date':'000000000000000000000111',
#     'inv_group':'644bf504f595b744814a4990',
#     'inv_group_readyweek':'644bf6c2d281661b082b6348',
#     'inv_group_flats':'644bf6c2d281661b082b6349',
#     }



# def get_grading(folio, answers):
#     global fdict
#     match_query ={ 
#      'form_id': GREENHOUSE_GRADING,  
#      'deleted_at' : {'$exists':False},
#      f'answers.{CATALOG_STOCK_OBJ_ID}.{fdict["cat_stock_folio"]}': folio
#      } 
#     query = [
#         {'$match': match_query},
#         {'$unwind':f'$answers.{fdict["grading_group"]}'},
#         {'$match':{f'answers.{fdict["grading_group"]}.{fdict["grading_move_type"]}':'grading'}},
#         {'$project':{
#             '_id':0,
#             'date':f'$answers.{fdict["grading_date"]}',
#             'ready_yearweek':f'$answers.{fdict["grading_group"]}.{fdict["inv_group_readyweek"]}',
#             'flats':f'$answers.{fdict["grading_group"]}.{fdict["grading_flats"]}',
#         }},
#         {'$sort':{'date':1}},
#         {'$group':{
#             '_id':{
#                 'date':'$date',
#                 'ready_yearweek':'$ready_yearweek'
#             },
#             'flats':{'$first':'$flats'}
#         }},
#         {'$project':{
#         '_id':0,
#         fdict['inv_group_readyweek']:'$_id.ready_yearweek',
#         fdict['inv_group_flats']:'$flats',
#         }}

#     ]
#     #print('query=',simplejson.dumps(query, indent=5))
#     res = cr.aggregate(query)
#     return [r for r in res]





if __name__ == '__main__':
    # print('asi entra=', sys.argv)
    stock_obj = Stock(settings, sys_argv=sys.argv)
    current_record = stock_obj.current_record
    folio =current_record.get('folio')
    # print('current_record',current_record)
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
