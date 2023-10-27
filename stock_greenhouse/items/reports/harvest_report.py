#-*- coding: utf-8 -*-

import simplejson, sys
from copy import deepcopy
from linkaform_api import settings, network, utils
from bson import ObjectId
import time
from datetime import datetime, timedelta, date

#from account_utils import get_plant_recipe, get_plant_names


from linkaform_api import settings
from account_settings import *

from lkf_addons.addons.stock_greenhouse.stock_reports import Reports





table_data = []
plants = {}
WEEKS = []


def query_get_stock(plant_code, stage):
    stage ='pull'
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":81503,
        "answers.620ad6247a217dbcb888d175": stage
        }
    if plant_code:
        match_query.update({"answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33d":plant_code,})
    query= [{'$match': match_query },
        {'$project':
            {'_id': 1,
                'plant_code': '$answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33d',
                'cut_year': '$answers.620a9ee0a449b98114f61d75',
                'cut_week': '$answers.622bb9946d0d6fef17fe0842',
                'eaches': '$answers.620ad6247a217dbcb888d172'}},
        {'$group':
            {'_id':
                { 'plant_code': '$plant_code',
                  'cut_year': '$cut_year',
                  'cut_week': '$cut_week'
                  },
              'total': {'$sum': '$eaches'}}},
        {'$project':
            {'_id': 0,
            'plant_code': '$_id.plant_code',
            'cut_year': '$_id.cut_year',
            'cut_week': '$_id.cut_week',
            'total': '$total',
            'from': 'Stage 3'
            }
        },
        {'$sort': {'_id.plant_code': 1, '_id.cut_year': 1, '_id.cut_week':1}}
        ]
    res = report_obj.cr.aggregate(query)
    result = [r for r in res]
    all_codes = []
    for r in result:
        if r.get('plant_code') and r.get('plant_code') not in all_codes:
            all_codes.append(r['plant_code'])
    return result, all_codes

def query_greenhouse_stock(plant_code=None, all_codes=[]):
    stage ='pull'
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":report_obj.FORM_INVENTORY_ID,
        f"answers.{report_obj.f['status']}": 'active'
        }
    if plant_code:
        match_query.update({f"answers.{report_obj.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{report_obj.f['product_code']}":plant_code,})
    query= [{'$match': match_query },
        {'$project':
            {'_id': 1,
                'plant_code': f"$answers.{report_obj.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{report_obj.f['product_code']}",
                'plant_name': {"$arrayElemAt" : [f"$answers.{report_obj.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{report_obj.f['product_name']}",0]},
                'product_lot': f"$answers.{report_obj.f['product_lot']}",
                'eaches': f"$answers.{report_obj.f['product_lot_actuals']}"}},
        {'$group':
            {'_id':
                { 'plant_code': '$plant_code',
                  'plant_name': '$plant_name',
                  'product_lot': '$product_lot',
                  },
              'total': {'$sum': '$eaches'}}},
        {'$project':
            {'_id': 0,
            'plant_code': '$_id.plant_code',
            'plant_name': '$_id.plant_name',
            'product_lot': '$_id.product_lot',
            'total_planting': '$total'
            }
        },
        {'$sort': {'_id.plant_code': 1, '_id.cut_year': 1, '_id.cut_week':1}}
        ]
    res = report_obj.cr.aggregate(query)
    # result = [r for r in res]
    result=[]
    for r in res:
        if r.get('plant_code') and r.get('plant_code') not in all_codes:
            all_codes.append(r['plant_code'])
        product_lot = str(r['product_lot'])
        ready_year = product_lot[:4]
        ready_week = product_lot[4:]
        d = f'{ready_year}-W{ready_week}'
        harvest_date = datetime.strptime(d + '-1', "%Y-W%W-%w")
        r['havest_week'] = int(harvest_date.strftime('%W'))
        r['havest_month'] = int(harvest_date.strftime('%m'))
        r['havest_year'] = int(harvest_date.strftime('%Y'))
        r['from'] = 'GreenHouse'
        r['total_harvest'] = r['total_planting'] * 72
        result.append(r)
    return result, all_codes


def arrange_info(data, stage, recipes3={}, recipes4={}):
    col = 'forcast'
    global plants, WEEKS
    today = date.today()
    today_week = int(today.strftime('%Y%W'))
    res = []
    for x in data:
        pcode = x.get('plant_code')
        print('x')
        try:
            if x.get('from') == 'Stage 3':
                multi_rate = 1
                grow_weeks = 0
                stage_multi_rate = 'S4_overage_rate'
                stage_grow_weeks = 'S4_growth_weeks'
                multi_rate = 1 - recipes3.get(pcode,{})[0].get(stage_multi_rate,0)
                grow_weeks = recipes3.get(pcode,{})[0].get(stage_grow_weeks,1)
                week = int(x.get('cut_week')) + grow_weeks
                x['plant_name'] = recipes3.get(pcode,{})[0].get('plant_name')
                year = x.get('cut_year')
                cut_week = datetime.strptime(str(year), "%Y") + timedelta(weeks=int(week))
                cut_week = int(cut_week.strftime('%Y%W'))
                if col == 'forcast':
                    if cut_week < today_week:
                        cut_week = today_week
                else:
                    cut_week = int(x.get('year_week'))
                x['total_planting'] = x['total'] * multi_rate
                #Stage 4 Green House growth_time and multiplaction
                multi_rate = 1 - recipes4.get(pcode,{})[0].get(stage_multi_rate,0)
                grow_weeks = recipes4.get(pcode,{})[0].get(stage_grow_weeks,1)
                t_grow_weeks = int(str(cut_week)[4:]) + grow_weeks
                harvest_date = datetime.strptime(str(cut_week), "%Y%W") + timedelta(weeks=t_grow_weeks)
                harvest_week= int(harvest_date.strftime('%Y%W'))
                x['havest_week'] = int(harvest_date.strftime('%W'))
                x['havest_month'] = int(harvest_date.strftime('%m'))
                x['havest_year'] = int(harvest_date.strftime('%Y'))
                x['total_harvest'] = int(x['total_planting'] * multi_rate)
            else:
                overage = 0
                if recipes4[x['plant_code']] and len(recipes4[x['plant_code']]) > 0:
                    this_recipe = recipes4[x['plant_code']][0]
                    overage = this_recipe.get('S4_overage', this_recipe.get('S4_overage_rate',0))
                print('overage', overage)
                x['total_harvest'] = int(x['total_harvest'] * (1-overage) )
        except:
            x['plant_name'] ='Recipe not found'
            x['total_planting'] = 0
            x['havest_week'] = 0
            x['havest_month'] = 0
            x['havest_year'] = 0
            x['total_harvest'] = 0
        res.append(x)
    return res

def get_report(plant_code, stage=3):
    global plants, WEEKS
    res, all_codes = query_get_stock(plant_code, stage)
    greenhouse_stock, all_codes = query_greenhouse_stock(plant_code, all_codes)
    recipesS3 = report_obj.get_plant_recipe(all_codes, stage=[4,])
    recipesS4 = report_obj.get_plant_recipe(all_codes, stage=[4,"Ln72"])
    res += greenhouse_stock
    res = arrange_info(res, stage, recipesS3, recipesS4)
    return res


if __name__ == '__main__':
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    #getFilters
    data = report_obj.data.get("data", {})
    plant_code = data.get("plant_code")
    response = get_report(plant_code)
    sys.stdout.write(simplejson.dumps(
        {"firstElement":{
            'tabledata':response
            }
        }
        )
    )
