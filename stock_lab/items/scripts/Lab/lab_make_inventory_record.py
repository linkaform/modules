# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta


from linkaform_api import settings, network, utils

from account_settings import *

from lab_stock_utils import Stock

class Stock(Stock):

    def set_product_catalog(self):
        plant =  self.answers.get(self.PRODUCT_RECIPE_OBJ_ID, {})
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ] = plant
        # soil_type = self.unlist(plant.get(self.f['reicpe_soil_type'],""))
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_soil_type']] = soil_type
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_container']] = self.unlist(
        #     self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_container'],"")
        #     )
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ][self.f['product_name']] = self.unlist(
        #     self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['product_name'],"")
        #     )
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ][self.f['prod_qty_per_container']] = \
        #     [self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['prod_qty_per_container'],""),  ]
        # self.answers[self.PRODUCT_RECIPE_OBJ_ID ][self.f['recipe_type']] = \
        #     [ self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['recipe_type'],""), ]
        # res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_soil_type']] = \
        #     self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_soil_type'],"")

        created_week = self.answers[self.f['production_cut_week']]
        print('create', created_week)
        cut_year = self.answers[self.f['plant_cut_year']]
        cut_day = self.answers[self.f['production_cut_day']]
        year = str( self.answers.get(self.f['plant_cut_year'], '') ).zfill(2)
        week = str( self.answers.get(self.f['production_cut_week'], '') ).zfill(2)
        plant_group = self.answers[self.f['plant_group']]

        self.answers[self.f['plant_cut_yearweek']] = f"{cut_year}{week}"
        
        plant_date = datetime.strptime('%04d-%02d-1' % (int(year), int(week)), '%Y-%W-%w')
        if plant.get(self.f['reicpe_growth_weeks']):
            grow_weeks = plant.get(self.f['reicpe_growth_weeks'])
            ready_date = plant_date + timedelta(weeks=grow_weeks)
            self.answers[self.f['product_growth_week']] = grow_weeks
            self.answers[self.f['plant_next_cutweek']] = int(ready_date.strftime('%Y%W'))
        return True


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.set_product_catalog()
    if stock_obj.folio:
        res = stock_obj.make_inventory_flow()
        status_code = res[0].get('status_code')
        update_ok = res[0].get('updatedExisting')
        #TODO, hace este pedazo atomico, ya que si se crea uno y se actualiza otro es distinto
        if status_code == 201 or update_ok:
            #todo poner sttatus en cada linea
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': False,
                'metadata':{'editable':False},
                'merge':{
                    'primary': False,
                    'replace': False,
                    'answers': {stock_obj.f['move_status']: 'done'}
                },
            }))
        else:
            msg_error_app = 'No error found'
            try:
                for r in res:
                    print('r',r)
                    if r.get('status_code') not in (200,201,202):
                        msg_error_app = r.get('json', 'Unkown error on creation, contact support')
                    else:
                        for folio, response in r.items():
                            print('response',response)
                            if response.get('status_code') not in (200,201,202):
                                msg_error_app = response.get('json', 'Unkown error on creation, contact support')
            except Exception as e:
                msg_error_app =  'Unkown error on creation, contact support', e
                raise Exception( simplejson.dumps( msg_error_app ) )
        
        sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': False,
                'metadata':{'editable':False},
                'merge':{
                    'primary': False,
                    'replace': False,
                    'answers': {stock_obj.f['move_status']: 'done'}
                },
        }))

