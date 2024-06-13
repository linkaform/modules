#-*- coding: utf-8 -*-
import simplejson, sys
from linkaform_api import settings
from bson import ObjectId
import time
from datetime import datetime, timedelta, date

from stock_report import Reports
from account_settings import *

all_codes = {}
columsTable = [
  { "title":"Plant Code", "field":'plant_code',"hozAlign":"left","width":150},
  { "title":"Folio", "field":'folio',"hozAlign":"left","width":150},
  { "title":"Stage", "field":'stage',"hozAlign":"left","width":150},
  { "title":"Quantity (eaches)", "field":'eaches',"hozAlign":"right","width":150},
  { "title":"Forcast (eaches)", "field":'eaches',"hozAlign":"right","width":150},
  { "title":"Group Time", "field":'group_time',"hozAlign":"right","width":150},
  { "title":"Plant Group", "field":'plant_group',"hozAlign":"right","width":150},
  { "title":"Location", "field":'location',"hozAlign":"right","width":150},
  { "title":"Cycles", "field":'cycles',"hozAlign":"right","width":150},
  { "title":"Next Cut Week", "field":'next_cut_week',"hozAlign":"right","width":200},
  { "title":"Last Cut Day", "field":'cut_day',"hozAlign":"right","width":200},
  { "title":"Cycle age", "field":'cycle_age',"hozAlign":"right","width":200},
  { "title":"Days to next cut day", "field":'days_next_cut_day',"hozAlign":"right","width":250},
  { "title":"Group age (days)", "field":'group_age',"hozAlign":"right","width":250},
  { "title":"Contamination", "field":'contamin_code',"hozAlign":"left","width":150},
  { "title":"Multiplication Factor", "field":'mult_rate',"hozAlign":"right","width":250},
]

def set_recipes():
    recipes = {}
    for stage, pcodes in all_codes.items():
        if stage == 'active':
            stage_num = 2
        elif stage == 'pull':
            stage_num = 3
        recipes[stage] =  report_obj.get_plant_recipe(pcodes, stage=[stage_num])
    return recipes

def calc_cycles(cycle):
    try:
        cycle_seq = cycle[0].lower()
    except:
        cycle_seq = 'a'
    try:
        cycle_num = int(cycle[1])
    except:
        cycle_num = 1
    cycle_wight = (string.ascii_lowercase.find(cycle_seq) +1) + (cycle_num * 24)
    return cycle_wight

def calc_info(records, recipes):
    tnow = time.time()
    tnow = datetime.fromtimestamp(tnow)
    data_list = []
    for lot in records:
        print('recipes',recipes)
        print('-------------')
        plant_code = lot.get('product_code')
        stage = lot.get('from')
        recipe = recipes[stage].get(plant_code)
        lot['mult_rate'] = lot.get('mult_rate')
        if not recipe:
            continue
        mult_rate = 0
        if stage == 'active':
            growth_weeks = recipe['S2_growth_weeks']
            if not lot['mult_rate']:
                mult_rate = recipe['S2_mult_rate']
                lot['mult_rate'] = mult_rate
            overage = recipe['S2_overage']
        elif stage == 'pull':
            growth_weeks = recipe['S3_growth_weeks']
            if not lot['mult_rate']:
                mult_rate = recipe['S3_mult_rate']
                lot['mult_rate'] = mult_rate
            overage = recipe['S3_overage']
        forcast = lot.get('eaches',0) * mult_rate * (1-overage)
        cut_day = '{}-{}'.format(lot['cut_year'], lot['cut_day'])
        cut_day = datetime.strptime(cut_day, '%Y-%j')
        cycle_age = tnow - cut_day
        next_cut_day = cut_day + timedelta(weeks=growth_weeks)
        days_next_cut_day = next_cut_day - tnow
        cycles = calc_cycles(lot.get('cycle'))
        group_time = growth_weeks * 7
        group_age = group_time * cycles
        lot['group_time'] = group_time
        lot['group_age'] = group_age
        lot['days_next_cut_day'] = days_next_cut_day.days
        lot['cycles'] = cycles
        lot['cycle_age'] = cycle_age.days
        lot['forcast'] = forcast
        print('========================')   
    return records

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()

    #-----Filter
    plants = {}
    data = report_obj.data
    test = data.get("test", False)
    plant_code = data.get('plant_code','')
    stage = data.get('stage')
    
    res , all_codes = report_obj.query_get_stock(plant_code, stage)
    recipe = {2:{},3:{}}
    if stage:
        recipe[stage] = report_obj.get_plant_recipe(all_codes, stage=stage)
    else:
        recipe[2] = report_obj.get_plant_recipe(all_codes, stage=2)
        recipe[3] = report_obj.get_plant_recipe(all_codes, stage=3)


    print('recipes',recipe)
    print('=================')
    #response = calc_info(res, recipes)
    '''
    response = calc_info(response, recipes)
    sys.stdout.write(simplejson.dumps(
        {
        "firstElement":{
            'tabledata':response,
            'colsData':columsTable,
            }
        }
        )
    )
    '''