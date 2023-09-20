# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings, network, utils

from account_settings import *
from account_utils import get_plant_recipe, select_S4_recipe, set_lot_ready_week
from stock_utils import *

# lkf = self.lkf.LKFModules()

# GREENHOUSE_GRADING = lkf.get_form_id('green house')#100513
GREENHOUSE_GRADING = 99536
GREENHOUSE_SCRAPPING = 99536
CATALOG_STOCK_ID = 98230
CATALOG_STOCK_OBJ_ID='6442cbafb1b1234eb68ec178'

fdict = {
    'cat_stock_folio':'62c44f96dae331e750428732',
    'grading_group':'644bf7ccfa9830903f087867',
    'grading_move_type':'64d5550ec4909ab3c20c5806',
    'grading_ready_yearweek':'64edf8aeffeaaa1febca2a06',
    'grading_flats':'644bf9a04b1761305b080013',
    'grading_date':'000000000000000000000111',
    'inv_group':'644bf504f595b744814a4990',
    'inv_group_readyweek':'644bf6c2d281661b082b6348',
    'inv_group_flats':'644bf6c2d281661b082b6349',
    }

def get_product_info(answers, folio=None,  **kwargs):
    warehouse = answers['6442e4831198daf81456f273']['6442e4831198daf81456f274']
    plant_code = answers.get('61ef32bcdf0ec2ba73dec33c', {}).get('61ef32bcdf0ec2ba73dec33d', '')
    yearWeek = str(answers['620a9ee0a449b98114f61d75'])
    print('product_code', plant_code)
    print('yearweek', yearWeek)
    print('warehouse', warehouse)
    print('kwargs1', kwargs)
    year = yearWeek[:4]
    week = yearWeek[4:]
    recipes = get_plant_recipe( [plant_code,], stage=[4, 'Ln72'] )
    print('recipes=', recipes)
    recipe = select_S4_recipe(recipes[plant_code], week)
    grow_weeks = recipe.get('S4_growth_weeks')
    print('asi va el lot numer...',answers.get('620a9ee0a449b98114f61d77'))
    if kwargs.get('kwargs',{}).get("force_lote") and answers.get('620a9ee0a449b98114f61d77'):
        print('usa el q esta....')
        ready_date = answers['620a9ee0a449b98114f61d77']
    else:
        print('calcual el plant week....')
        if not folio:
            plant_date = datetime.strptime('%04d-%02d-1'%(int(year), int(week)), '%Y-%W-%w')
            ready_date = plant_date + timedelta(weeks=grow_weeks)
            ready_date = int(ready_date.strftime('%Y%W'))
        else:
            ready_date = answers['620a9ee0a449b98114f61d77']

    print('lot_number', ready_date)
    print('kwargs', kwargs.get('kwargs',{}))
    product_stock = get_product_stock(cr, plant_code, warehouse=warehouse, lot_number=ready_date, kwargs=kwargs.get('kwargs',{}))
    scrapped = product_stock['scrapped']
    overage = recipes[plant_code][0].get('S4_overage_rate')
    actual_flats_on_hand = product_stock['actuals']
    print('actual_flats_on_hand=',product_stock)
    proyected_flats_on_hand = math.floor(( 1 - overage) * actual_flats_on_hand)
    lot_size = current_record['answers'].get('6271dc35e84e2577579eafeb',0)
    print('lot_size', lot_size)
    if lot_size == 0:
        perc_scrapped = 0
    else:
        perc_scrapped = round(scrapped / lot_size, 2)

    real_flats_proyected = lot_size - scrapped

    if real_flats_proyected < proyected_flats_on_hand:
        proyected_flats_on_hand = real_flats_proyected

    answers['6271dc35e84e2577579eafeb'] = product_stock['production']
    answers['620ad6247a217dbcb888d000'] = product_stock['move_in']
    answers['620ad6247a217dbcb888d16d'] = product_stock['scrapped']
    answers['620ad6247a217dbcb888d17e'] = product_stock['move_out']
    answers['6442e2fbc0dd855fe856f1da'] = product_stock['sales']
    answers['6442e2fbc0dd855fe856fddd'] = product_stock['cuarentin']
    answers['6441d33a153b3521f5b2afc9'] = product_stock['actuals']
    answers['aaaaa0000000000000000000'] = product_stock['adjustments']

    answers['6442e25f13879061894b4bb3'] = perc_scrapped
    answers['6442e25f13879061894b4bb2'] = proyected_flats_on_hand
    answers['6442e25f13879061894b4bb3'] = perc_scrapped       
    answers['620a9ee0a449b98114f61d77'] = ready_date
    if answers['6441d33a153b3521f5b2afc9'] <= 0:
        answers['620ad6247a217dbcb888d175'] = 'done'
    else:
        answers['620ad6247a217dbcb888d175'] = 'active'
    answers.update({fdict['inv_group']:get_grading(folio, answers)})
    return answers


def get_grading(folio, answers):
    global fdict
    match_query ={ 
     'form_id': GREENHOUSE_GRADING,  
     'deleted_at' : {'$exists':False},
     f'answers.{CATALOG_STOCK_OBJ_ID}.{fdict["cat_stock_folio"]}': folio
     } 
    query = [
        {'$match': match_query},
        {'$unwind':f'$answers.{fdict["grading_group"]}'},
        {'$match':{f'answers.{fdict["grading_group"]}.{fdict["grading_move_type"]}':'grading'}},
        {'$project':{
            '_id':0,
            'date':f'$answers.{fdict["grading_date"]}',
            'ready_yearweek':f'$answers.{fdict["grading_group"]}.{fdict["inv_group_readyweek"]}',
            'flats':f'$answers.{fdict["grading_group"]}.{fdict["grading_flats"]}',
        }},
        {'$sort':{'date':1}},
        {'$group':{
            '_id':{
                'date':'$date',
                'ready_yearweek':'$ready_yearweek'
            },
            'flats':{'$first':'$flats'}
        }},
        {'$project':{
        '_id':0,
        fdict['inv_group_readyweek']:'$_id.ready_yearweek',
        fdict['inv_group_flats']:'$flats',
        }}

    ]
    #print('query=',simplejson.dumps(query, indent=5))
    res = cr.aggregate(query)
    return [r for r in res]





if __name__ == '__main__':
    print('asi entra=', sys.argv)
    current_record = simplejson.loads( sys.argv[1] )
    data = simplejson.loads( sys.argv[2] )
    config['USER_JWT_KEY'] = data["jwt"].split(' ')[1]
    USER_ID = current_record['user_id']
    # ------------------------------------------------------------------------------
    lkf_api = utils.Cache(settings)
    jwt_parent = lkf_api.get_jwt(api_key=config['API_KEY'])
    config['JWT_KEY'] = jwt_parent
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    cr = net.get_collections()
    # calculates_inventory_greenhouse(current_record)
    folio =current_record.get('folio')
    print('current_record',current_record)
    kwargs = current_record.get('kwargs', current_record.get('properties',{}).get('kwargs', {}))
    if folio:
        #si ya existe el registro, no cambio el numero de lote
        kwargs['force_lote'] = True
    print('calcluates inv kwargs', kwargs)
    answers = get_product_info(current_record['answers'],current_record.get('folio'), kwargs=kwargs)
    current_record['answers'].update(answers)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': current_record['answers']
    }))
