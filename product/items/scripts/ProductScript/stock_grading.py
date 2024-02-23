# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta

from linkaform_api import settings, network, utils
from account_utils import get_inventory_flow
from magnolia_settings import *

INVENTORY_FORM_ID = 98225

def get_sublots(gradings):
    totals = {'3_weeks':0, '2_weeks':0, '1_weeks':0, 'on_time':0, 'weeks_1':0, 'weeks_2':0, 'weeks_3':0, 'scrap':0, 'cuarentin':0}
    for cutter in gradings:
        totals['3_weeks']  += cutter.get('644bf9a04b1761305b080013',0)
        totals['2_weeks']  += cutter.get('644bf9a04b1761305b080012',0)
        totals['1_weeks']  += cutter.get('644bf9a04b1761305b080011',0)
        totals['on_time'] += cutter.get('644bf9a04b1761305b080000',0)
        totals['weeks_1']  += cutter.get('644bf9a04b1761305b080001',0)
        totals['weeks_2']  += cutter.get('644bf9a04b1761305b080002',0)
        totals['weeks_3']  += cutter.get('644bf9a04b1761305b080003',0)
        totals['scrap']   += cutter.get('644bf9a04b1761305b080099',0)
        totals['cuarentin']   += cutter.get('644bf9a04b1761305b080098',0)
    return totals

def set_lot_ready_week(lot_ready_week, gradin_totals):
    lot_ready_week = str(lot_ready_week)
    week = int(lot_ready_week[-2:])
    year = int(lot_ready_week[:4])
    ready_week = datetime.strptime('%04d-%02d-1' % (year, week), '%Y-%W-%w')
    # ready_week = this_date.strftime('%Y%W')
    print('ready_week', ready_week)
    sets = []
    for week, qty in gradin_totals.items():
        week_type = week.split('_')
        print('week_type', week_type)
        print('qty', qty)
        if len(week_type)  == 2:
            if week_type[0] == 'on':
                delta_weeks = 0 
            else:
                try:
                    delta_weeks = int(week_type[0]) * -1
                except:
                    delta_weeks = int(week_type[1]) 
            print('delta_weeks', delta_weeks)
            new_week = ready_week + timedelta(weeks=delta_weeks)
            new_week = new_week.strftime('%Y%W')
            print('new_week', new_week)
            tset ={
             '644bf6c2d281661b082b6348': int(new_week) ,
             '644bf6c2d281661b082b6349': qty
            }
            print('tset', tset)
            sets.append(tset)
        elif week == 'cuarentin':
            tset ={
             '644bf6c2d281661b082b6348': int(year* 100) ,
             '644bf6c2d281661b082b6349': qty
            }
            print('tset', tset)
            sets.append(tset)
        else:
            print('is scrap, do scrap record>>>>>>>>>>>>>>>>>', qty)
    return sets



def get_gradings(current_record):
    current_answers = current_record['answers']
    gradings = current_answers.get('644bf7ccfa9830903f087867',[])
    plant_info = current_answers.get('6442cbafb1b1234eb68ec178',{})
    folio_inventory = plant_info.get('62c44f96dae331e750428732')
    ready_date = plant_info.get('620a9ee0a449b98114f61d77')
    plant_code = plant_info.get('61ef32bcdf0ec2ba73dec33d')
    record_inventory_flow = get_inventory_flow(folio_inventory, form_id=INVENTORY_FORM_ID)
    print('record_inventory_flow=', record_inventory_flow)
    grading_totals = get_sublots(gradings)
    print('grading_totals=', grading_totals)
    totals = sum(x for x in grading_totals.values())
    
    acctual_containers = record_inventory_flow['answers']['6441d33a153b3521f5b2afc9']
    lot_ready_week = record_inventory_flow['answers']['620a9ee0a449b98114f61d77']
    print('lot_ready_week=', lot_ready_week)

    # if acctual_containers != totals:
    if totals != totals:
        #trying to move more containeres that there are...
        msg = "Diferencia en la cantidad total de flats de lote y del grading "
        msg += "La cantidad de total de flats reportadas es de: {} y la cantdidad flats del lote es de: {}. ".format(totals, acctual_containers)
        msg += "Hay una diferencia de : {} favor de corregir".format(acctual_containers - totals)
        msg_error_app = {
                "6441d33a153b3521f5b2afc9": {
                    "msg": [msg],
                    "label": "Please check your Actual Containers on Hand",
                    "error":[]
  
                }
            }
        raise Exception( simplejson.dumps( msg_error_app ) )
    record_inventory_flow['answers']['644bf504f595b744814a4990'] = set_lot_ready_week(lot_ready_week, grading_totals)
    res_update_inventory = lkf_api.patch_record( record_inventory_flow, jwt_settings_key='USER_JWT_KEY' )
    return res_update_inventory

if __name__ == '__main__':
    print(sys.argv)
    current_record = simplejson.loads( sys.argv[1] )
    jwt_complete = simplejson.loads( sys.argv[2] )
    config['USER_JWT_KEY'] = jwt_complete['jwt'].split(' ')[1]
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    cr = net.get_collections()

    response = get_gradings(current_record)
    print('status code', response.get('status_code'))
    if response.get('status_code') > 299 or not response.get('status_code'):
        print('response=',response)
        msg_error_app = response.get('json', 'Error al actualiza inventario , favor de contactar al administrador')
        raise Exception( simplejson.dumps(msg_error_app) )
    else:
        current_record['answers']['644c1cb6dc502afa06c4423e'] =  'done'
        # sys.stdout.write(simplejson.dumps({
        #     'status': 206,
        #     'metadata':{'editable':False},
        #     'replace_ans': current_record['answers'],
        #     }))        
        sys.stdout.write(simplejson.dumps({
            'status': 206,
            'metadata':{'editable':False},
            'merge': {
                'answers': {'644c1cb6dc502afa06c4423e': 'done'}},
            }))
