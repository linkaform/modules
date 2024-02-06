# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime
from bson import ObjectId

from lab_stock_utils import Stock

from account_settings import *


class Stock(Stock):

    def set_product_catalog(self):
        return True

    def get_product_info(self, **kwargs):
        try:
            warehouse = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
            warehouse_location = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
            plant_code = self.answers.get(self.f['product_recipe'], {}).get(self.f['product_code'], '')
        except Exception as e:
            print('**********************************************')
            self.LKFException('Warehosue and product code are requierd')
        yearWeek = str(self.answers[self.f['product_lot_created_week']])
        year = yearWeek[:4]
        week = yearWeek[4:]
        stage = self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_stage'])
        reicpe_start_size = self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_start_size'])
        recipe_type = self.unlist(self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['recipe_type']))
        recipes = self.get_plant_recipe( [plant_code,], stage=stage, recipe_type=recipe_type )
        if stage == 'S4':
            recipe = self.select_S4_recipe(recipes[plant_code], week)
        else:
            recipe = recipes[plant_code]
        grow_weeks = recipe.get(f'{stage}_growth_weeks')
        ready_date = self.answers.get(self.f['product_lot'])
        # if kwargs.get('kwargs',{}).get("force_lote") and answers.get(self.f['product_lot']):
        #     ready_date = answers.get(self.f['product_lot'])
        #     print('FOORCE LOTEEEEE')
        # else:
        #     if not folio and not ready_date:
        #         plant_date = datetime.strptime('%04d-%02d-1'%(int(year), int(week)), '%Y-%W-%w')
        #         ready_date = plant_date + timedelta(weeks=grow_weeks)
        #         ready_date = int(ready_date.strftime('%Y%W'))
        #     else:
        #         ready_date = answers[self.f['product_lot']]
        product_stock = self.get_product_stock(plant_code, warehouse=warehouse, location=warehouse_location, lot_number=ready_date, kwargs=kwargs.get('kwargs',{}) )
        scrapped = product_stock['scrapped']
        overage = recipe.get(f'{stage}_overage', recipe.get(f'{stage}_overage_rage', 0 ))
        actual_flats_on_hand = product_stock['actuals']
        proyected_flats_on_hand = math.floor(( 1 - overage) * actual_flats_on_hand)
        lot_size = self.answers.get(self.f['product_lot_produced'],0)
        if lot_size == 0:
            perc_scrapped = 0
        else:
            perc_scrapped = round(scrapped / lot_size, 2)

        real_flats_proyected = lot_size - scrapped

        if real_flats_proyected < proyected_flats_on_hand:
            proyected_flats_on_hand = real_flats_proyected

        print('acutal contoiners', self.answers.get('plant_per_container',0))

        self.answers[self.f['product_lot_produced']] = product_stock['production']
        self.answers[self.f['product_lot_move_in']] = product_stock['move_in']
        self.answers[self.f['product_lot_scrapped']] = product_stock['scrapped']
        self.answers[self.f['product_lot_move_out']] = product_stock['move_out']
        self.answers[self.f['product_lot_sales']] = product_stock['sales']
        self.answers[self.f['product_lot_cuarentin']] = product_stock['cuarentin']
        self.answers[self.f['product_lot_actuals']] = product_stock['actuals'] 
        self.answers[self.f['actual_eaches_on_hand']] = product_stock['actuals'] * self.answers.get('plant_per_container',0)
        self.answers[self.f['product_lot_adjustments']] = product_stock['adjustments']

        self.answers[self.f['product_lot_per_scrap']] = perc_scrapped
        self.answers[self.f['product_lot_proyected_qty']] = proyected_flats_on_hand
        self.answers[self.f['product_lot_per_scrap']] = perc_scrapped       
        self.answers[self.f['product_lot']] = ready_date
        if self.answers[self.f['product_lot_actuals']] <= 0:
            self.answers[self.f['inventory_status']] = 'done'
        else:
            self.answers[self.f['inventory_status']] = 'active'
        self.answers.update({self.f['inv_group']:self.get_grading()})
        return self.answers

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    current_record = stock_obj.current_record
    folio = stock_obj.folio
    # print('current_record',current_record)

    # if folio:
    #     #si ya existe el registro, no cambio el numero de lote
    #     kwargs['force_lote'] = True
    # print('answers = ', stock_obj.answers)
    answers = stock_obj.get_product_info()
    query = None
    _id = current_record.get('connection_record_id',{}).get('$oid')
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
