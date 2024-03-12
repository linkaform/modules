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

from stock_report import Reports

table_data = []
plants = {}
WEEKS = []


def arrange_info(data, stage, recipes3={}, recipes4={}):
    col = 'forcast'
    global plants, WEEKS
    today = date.today()
    today_week = int(today.strftime('%Y%W'))
    res = []
    for x in data:
        pcode = x.get('product_code')
        x['plant_name'] = x.get('product_name')
        x['plant_code'] = x.get('product_code')
        try:
        #if True:
            if x.get('from') == 'Stage 3':
                print('x=',x)
                multi_rate = 1
                grow_weeks = 0
                stage_multi_rate = 'S3_mult_rate'
                stage_grow_weeks = 'S3_growth_weeks'
                multi_rate = recipes3.get(pcode,{}).get(stage_multi_rate,0)
                grow_weeks = recipes3.get(pcode,{}).get(stage_grow_weeks,1)
                cut_yearWeek = x.get('cut_yearWeek')
                cut_week = datetime.strptime(str(cut_yearWeek), "%Y%W") + timedelta(weeks=grow_weeks)
                cut_week = int(cut_week.strftime('%Y%W'))
                if col == 'forcast':
                    if cut_week < today_week:
                        cut_week = today_week
                else:
                    cut_week = int(cut_yearWeek)
                stage_multi_rate = 'S4_overage'
                stage_grow_weeks = 'S4_growth_weeks'
                x['total_planting'] = x['total'] * multi_rate
                #Stage 4 Green House growth_time and multiplaction
                selected_recipe = report_obj.select_S4_recipe(recipes4.get(pcode,{}), str(cut_week)[4:])
                multi_rate = 1 - selected_recipe.get(stage_multi_rate,0)
                grow_weeks = selected_recipe.get(stage_grow_weeks,1)
                t_grow_weeks = int(str(cut_week)[4:]) + grow_weeks
                harvest_date = datetime.strptime(str(cut_week), "%Y%W") + timedelta(weeks=t_grow_weeks)
                harvest_week= int(harvest_date.strftime('%Y%W'))
                print('cut_week',cut_week)
                print('t_grow_weeks',t_grow_weeks)
                print('harvest_date',harvest_date)
                x['havest_week'] = int(harvest_date.strftime('%W'))
                x['havest_month'] = int(harvest_date.strftime('%m'))
                x['havest_year'] = int(harvest_date.strftime('%Y'))
                x['total_harvest'] = int(x['total_planting'] * multi_rate)
            else:
                print('x=',x)
                overage = 0
                multi_rate = 1 - selected_recipe.get(stage_multi_rate,0)
                if recipes4[x['product_code']] and len(recipes4[x['product_code']]) > 0:
                    this_recipe = recipes4[x['product_code']][0]
                    overage = this_recipe.get('S4_overage', this_recipe.get('S4_overage_rate',0))
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

def get_report(report_obj, product_code, stage='S3'):
    global plants, WEEKS
    res, all_codes = report_obj.query_get_stock(product_code, stage)
    greenhouse_stock, all_codes = report_obj.query_greenhouse_stock(product_code, all_codes)
    recipesS3 = report_obj.get_product_recipe(all_codes, stage="S3")
    print('report_obj.get_product_recipe',report_obj.get_product_recipe)
    print('recipesS3',recipesS3)
    recipesS4 = report_obj.get_product_recipe(all_codes, stage=[4, "Ln72"])
    print('report_obj.get_product_recipe',report_obj.get_product_recipe)
    print('recipesS4',recipesS4)
    res += greenhouse_stock
    res = arrange_info(res, stage, recipesS3, recipesS4)
    return res

if __name__ == '__main__':
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    #getFilters
    data = report_obj.data.get("data", {})
    product_code = data.get("product_code")
    response = get_report(report_obj, product_code)
    sys.stdout.write(simplejson.dumps(
        {"firstElement":{
            'tabledata':response
            }
        }
        )
    )
