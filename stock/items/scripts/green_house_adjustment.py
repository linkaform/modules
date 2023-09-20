# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings, network, utils

from account_settings import *
from account_utils import get_plant_recipe, select_S4_recipe, set_lot_ready_week
from stock_utils import *

# lkf = self.lkf.LKFModules()


if __name__ == '__main__':
    print(sys.argv)
    current_record = simplejson.loads( sys.argv[1] )
    data = simplejson.loads( sys.argv[2] )
    config['USER_JWT_KEY'] = data["jwt"].split(' ')[1]
    config['JWT_KEY'] = data["jwt"].split(' ')[1]
    USER_ID = current_record['user_id']
    settings.config.update(config)
    net = network.Network(settings)
    cr = net.get_collections()
    # ------------------------------------------------------------------------------
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    cr = net.get_collections()
    current_folio = current_record['folio']
    plants = current_record['answers']['644bf7ccfa9830903f087867']
    warehouse = current_record['answers']['6442e4831198daf81456f273']['6442e4831198daf81456f274']
    print('warehouse', warehouse)
    patch_records = []
    metadata = lkf_api.get_metadata(98225)
    kwargs = {"force_lote":True, "inc_folio":current_folio }
    properties = {
            "device_properties":{
                "system": "Script",
                "process": "Inventroy Adjustment", 
                "accion": 'Inventroy Adjustment', 
                "folio carga": current_record.get('folio'), 
                "archive": "green_house_adjustment.py",
            },
                "kwargs": kwargs 
        }
    metadata.update({
        'properties': properties,
        'kwargs': kwargs,
        'answers': {}
        },
    )
    print('kwargs', kwargs)
    search_codes = []
    for plant in plants:
        product_code = plant['6205f7690e752ce25bb30102']['61ef32bcdf0ec2ba73dec33d']
        search_codes.append(product_code)


    recipes = get_plant_recipe( search_codes, stage=[4, 'Ln72'] )
    growth_weeks = 0

    not_found = []
    for idx, plant in enumerate(plants):
        print('----------------------------------')
        print('plant', plant)
        status = plant['ad00000000000000000ad999']
        print('status', status)

        if status == 'done':
            print('skipping done plant code')
            continue
        lot_number = plant['620a9ee0a449b98114f61d77']
        yearWeek = plant.get('620a9ee0a449b98114f61d75')
        product_code = plant['6205f7690e752ce25bb30102']['61ef32bcdf0ec2ba73dec33d']
        if recipes.get(product_code) and len(recipes[product_code]):
            growth_weeks = recipes[product_code][0]['S4_growth_weeks']
            soli_type = recipes[product_code][0].get('soil_type','RIVERBLEND')
            start_size = recipes[product_code][0].get('recipes','Ln72')
        else:
            print('codes not found', product_code)
            not_found.append(product_code)
            plant['ad00000000000000000ad999'] = 'not_found'
            continue

        ready_date = lot_number
        year = str(ready_date)[:4]
        week = str(ready_date)[4:]
        if not growth_weeks:
            growth_weeks = int(week) -1

        plant_ready_date = datetime.strptime('%04d-%02d-1'%(int(year), int(week)), '%Y-%W-%w')
        if not yearWeek:
            yearWeek = plant_ready_date - timedelta(weeks=growth_weeks)
            yearWeek = int(yearWeek.strftime('%Y%W'))
        else:
            year = str(yearWeek)[:4]
            week = str(yearWeek)[4:]
            plant_date = datetime.strptime('%04d-%02d-1'%(int(year), int(week)), '%Y-%W-%w')
            growth_weeks_diff = plant_ready_date - plant_date
            growth_weeks = int(growth_weeks_diff.days/7)

        exist = product_stock_exists(cr, product_code=product_code, warehouse=warehouse, lot_number=lot_number)
        if exist:
            print('product_code',product_code)
            print('plant_code',exist['folio'])
            print('id',exist['_id'])
            print('lot_number',lot_number)
            exist.update({'properties':properties, 'kwargs':kwargs, "set_id":idx})
            patch_records.append(exist)

            
        else:
            print('proque no lo encunra', product_code)
            print('proque no lo warehouse', warehouse)
            print('proque no lo lot_number', lot_number)
            answers = {
                "61ef32bcdf0ec2ba73dec33c":{
                    "61ef32bcdf0ec2ba73dec33d":product_code,
                    "6205f73281bb36a6f1573358":start_size,
                    "6209705080c17c97320e3383":soli_type,
                    },
                "620a9ee0a449b98114f61d75":yearWeek,
                "620a9ee0a449b98114f61d77":lot_number,
                "645576e878f3060d1f7fc61b":growth_weeks,
                "620ad6247a217dbcb888d175":"active",
                "6442e4831198daf81456f273":{"6442e4831198daf81456f274":warehouse},
                    }
            metadata['answers'] = answers
            response_sistema = lkf_api.post_forms_answers(metadata)
            print('response_sistema=',response_sistema)
            status_code = response_sistema.get('status_code',404)
            print('status_code===',status_code)
            if status_code == 201:
                plant['ad00000000000000000ad999'] = 'done'
            else:
                error = response_sistema.get('json',{}).get('error', 'Unkown error')
                plant['ad00000000000000000ad999'] = 'error'
                plant['ad00000000000000000ad400'] = f'Status Code: {status_code}, Error: {error}'

    if patch_records:
        answers_to_update = {
            '620ad6247a217dbcb888d175': 'active',
            }
        print('folios', [x['folio'] for x in patch_records] )
        response_patch_list = lkf_api.patch_record_list( patch_records )
        #response_multi_patch = lkf_api.patch_multi_record(answers_to_update, 98225, folios=list(folios.keys()), threading=True)
        print('response_patch_list', response_patch_list)
        for idx, result in response_patch_list:
            print('this idx=', idx)
            print('this result', result)
            set_id = patch_records[idx]['set_id']
            status_code = result.get('status_code')
            if status_code == 202:
                plants[set_id]['ad00000000000000000ad999'] = 'done'
                plants[set_id]['ad00000000000000000ad400'] = ""
            else:
                error = result.get('json',{}).get('error', 'Unkown error')
                plant['ad00000000000000000ad999'] = 'error'
                plant['ad00000000000000000ad400'] = f'Status Code: {status_code}, Error: {error}'




    record_id = current_record['_id']['$oid']
    current_record['answers']['6442e4537775ce64ef72dd6a'] = 'done'
    if not_found:
        current_record['answers']['64d05792c373f9b62f539d00'] = f'Codes not found: {not_found}'
    print('end', current_record['answers'])
    lkf_api.patch_record(current_record, record_id)
