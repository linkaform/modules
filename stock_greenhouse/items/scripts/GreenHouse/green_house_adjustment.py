# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings, network, utils

from account_settings import *
#from account_utils import get_plant_recipe, select_S4_recipe, set_lot_ready_week
#from stock_utils import *
from lkf_addons.addons.stock_greenhouse.app import Stock

print('usando greeeeeenhouse....')

# lkf = self.lkf.LKFModules()




class Stock(Stock):

    def inventory_adjustment(self, folio, record):
        answers = record['answers']
        plants = answers.get(self.f['grading_group'])
        warehouse = answers[self.CATALOG_WAREHOUSE_OBJ_ID][self.f['warehouse']]
        adjust_date = answers[self.f['grading_date']]
        comments = record['answers'].get(self.f['inv_adjust_comments'],'') 
        patch_records = []
        metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
        kwargs = {"force_lote":True, "inc_folio":folio }
        properties = {
                "device_properties":{
                    "system": "Script",
                    "process": "Inventroy Adjustment", 
                    "accion": 'Inventroy Adjustment', 
                    "folio carga": folio, 
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
        search_codes = []
        # for plant in plants:
        #     product_code = plant[self.CATALOG_PRODUCT_OBJ_ID][self.f['product_code']]
        #     search_codes.append(product_code)


        # growth_weeks = 0
        latest_versions = versions = self.get_record_last_version(record)
        answers_version = latest_versions.get('answers',{})
        last_verions_products = {}
        if answers_version:
            version_products = answers_version.get(self.f['grading_group'])
            for ver_product in version_products:
                ver_lot_number = ver_product[self.f['product_lot']]
                ver_product_code = ver_product[self.CATALOG_PRODUCT_OBJ_ID][self.f['product_code']]
                last_verions_products[f'{ver_product_code}_{warehouse}_{ver_lot_number}'] = {
                    'product_code':ver_product_code,
                    'lot_number':ver_lot_number,
                    'warehouse':warehouse
                }
        for plant in plants:
            product_code =  plant[self.CATALOG_PRODUCT_OBJ_ID][self.f['product_code']]
            search_codes.append(product_code)
        recipes = self.get_plant_recipe( search_codes, stage=[4, 'Ln72'] )
        growth_weeks = 0

        not_found = []
        for idx, plant in enumerate(plants):
            status = plant[self.f['inv_adjust_grp_status']]
            lot_number = plant[self.f['product_lot']]
            adjust_qty = self.not_none(plant.get(self.f['inv_adjust_grp_qty'],0))
            adjust_in = self.not_none(plant.get(self.f['inv_adjust_grp_in'], 0))
            adjust_out = self.not_none(plant.get(self.f['inv_adjust_grp_out'], 0))
            product_code = plant[self.CATALOG_PRODUCT_OBJ_ID][self.f['product_code']]
            verify = 0
            print('adjust_qty=',adjust_qty)
            print('adjust_in=',adjust_in)
            print('adjust_out=',adjust_out)
            if adjust_qty or adjust_qty ==0:
                print('nonnneee')
                verify +=1
                adjust_in = 0
                adjust_out = 0
            elif adjust_in:
                if adjust_qty == 0  and last_verions_products:
                    pass
                else:
                    verify +=1
            elif adjust_out:
                if adjust_qty == 0  and last_verions_products:
                    pass
                else:
                    verify +=1
            if verify > 1:
                msg = f"You can have only ONE input on product {product_code} lot number {lot_number}."
                msg +=  "Either the Actual Qty, the Adjusted In or the Adjusted Out."
                plant[self.f['inv_adjust_grp_status']] = 'error'
                plant[self.f['inv_adjust_grp_comments']] = msg
                continue
            if verify ==  0:
                msg = f"You must input an adjusted Qty on product {product_code}, lot number {lot_number}."
                plant[self.f['inv_adjust_grp_status']] = 'error'
                plant[self.f['inv_adjust_grp_comments']] = msg
                continue
            print('22adjust_in=',adjust_in)
            print('22adjust_out=',adjust_out)

            if last_verions_products.get(f'{product_code}_{warehouse}_{lot_number}'):
                last_verions_products.pop(f'{product_code}_{warehouse}_{lot_number}')
            exist = self.product_stock_exists(product_code=product_code, warehouse=warehouse, lot_number=lot_number)
            if exist:
                print('2///////////////////////////////////////////')
                product_stock = self.get_product_stock(product_code, warehouse=warehouse, lot_number=lot_number, date_to=adjust_date, **{'nin_folio':folio})
                actuals = product_stock.get('actuals',0)
                print('product_stock', product_stock)
                print('actuals', actuals)
                print('adjust_qty', adjust_qty)
                if adjust_qty == 0 and adjust_in == 0 and adjust_out ==0:
                    print('111aadjust_qty', adjust_qty)
                    cache_adjustment = adjust_qty - actuals
                elif adjust_qty or adjust_qty == 0:
                    print('22222', adjust_qty)
                    if actuals < adjust_qty:
                        adjust_in = adjust_qty - actuals 
                        print('<<<<<<<<<<<<<<', adjust_in)
                        cache_adjustment = adjust_in
                    elif actuals > adjust_qty:
                        adjust_out = adjust_qty - actuals
                        print('33333adjust_in=',adjust_in)
                        print('>>>>>>>>>>>>>>', adjust_out)
                        cache_adjustment = adjust_out  
                    else:
                        cache_adjustment = adjust_qty - actuals
                        print('>>>>>>>>>>>>>><<<<<<<<<<<<<<<', actuals)
                        adjust_in  = 0
                        adjust_out = 0
                elif adjust_in:
                    print('333333', adjust_in)
                    cache_adjustment = adjust_in
                    adjust_out = 0
                    adjust_qty = 0
                elif adjust_out:
                    print('444444', adjust_out)
                    cache_adjustment = adjust_out * -1
                    adjust_in = 0
                    adjust_qty = 0
                print('cache_adjustment = ', cache_adjustment)
                if cache_adjustment > 0:
                    print('ccccccc')
                    adjust_in = abs(cache_adjustment)
                    adjust_out = 0 
                else:
                    print('aaaaaa')
                    adjust_out = abs(cache_adjustment) * -1
                    adjust_in = 0 
                print('--------------------------------')
                print('adjust_in', adjust_in)
                print('adjust_out', adjust_out)
                print('cache_adjustment', cache_adjustment)
                print('cache_adjustment _id', f'{product_code}_{lot_number}_{warehouse}')
                self.cache_set({
                        '_id': f'{product_code}_{lot_number}_{warehouse}',
                        'adjustments': cache_adjustment,
                        'product_lot': lot_number,
                        'product_code':product_code,
                        'warehouse': warehouse,
                        'record_id':self.record_id
                        })
                plant[self.f['inv_adjust_grp_qty']] = adjust_qty
                plant[self.f['inv_adjust_grp_in']] = adjust_in
                plant[self.f['inv_adjust_grp_out']] = abs(adjust_out)
                response = self.update_calc_fields(product_code, warehouse, lot_number, folio=exist['folio'], **{'nin_folio':folio} )
                if not response:
                    comments += f'Error updating product {product_code} lot {lot_number}. '
                    plant[self.f['inv_adjust_grp_status']] = 'error'
                else:
                    plant[self.f['inv_adjust_grp_status']] = 'done'
                    plant[self.f['inv_adjust_grp_comments']] = ""

            else:
                if recipes.get(product_code) and len(recipes[product_code]):
                    growth_weeks = recipes[product_code][0]['S4_growth_weeks']
                    soli_type = recipes[product_code][0].get('soil_type','RIVERBLEND')
                    start_size = recipes[product_code][0].get('recipes','Ln72')

                    yearWeek = plant.get(self.f['product_lot_created_week'])
                    if not yearWeek:
                        ready_date = lot_number
                        year = str(ready_date)[:4]
                        week = str(ready_date)[4:]
                        plant_ready_date = datetime.strptime('%04d-%02d-1'%(int(year), int(week)), '%Y-%W-%w')
                        yearWeek = plant_ready_date - timedelta(weeks=growth_weeks)
                        yearWeek = int(yearWeek.strftime('%Y%W'))

                    else:
                        not_found.append(product_code)
                        plant[self.f['inv_adjust_grp_status']] = 'not_found'
                        continue                
                    answers = {
                        self.CATALOG_PRODUCT_RECIPE_OBJ_ID:{
                            self.f['product_code']:product_code,
                            self.f['reicpe_start_size']:start_size,
                            self.f['reicpe_soil_type']:soli_type,
                            },
                        self.f['product_lot_created_week']:yearWeek,
                        self.f['product_lot']:lot_number,
                        self.f['product_growth_week']:growth_weeks,
                        self.f['status']:"active",
                        self.CATALOG_WAREHOUSE_OBJ_ID:{self.f['warehouse']:warehouse}
                            }
                    metadata['answers'] = answers
                    self.cache_set({
                            '_id': f'{product_code}_{lot_number}_{warehouse}',
                            'adjustments': adjust_qty + adjust_in - adjust_out,
                            'product_lot': lot_number,
                            'product_code':product_code,
                            'warehouse': warehouse,
                            'record_id':self.record_id
                            })
                    response_sistema = self.lkf_api.post_forms_answers(metadata)
                    # self.update_calc_fields(product_code, warehouse, lot_number)
                    try:
                        new_inv = self.get_record_by_id(response_sistema.get('id'))
                    except:
                        print('no encontro...')
                    status_code = response_sistema.get('status_code',404)
                    if status_code == 201:
                        plant[self.f['inv_adjust_grp_status']] = 'done'
                        plant[self.f['inv_adjust_grp_comments']] = "New Creation "
                    else:
                        error = response_sistema.get('json',{}).get('error', 'Unkown error')
                        plant[self.f['inv_adjust_grp_status']] = 'error'
                        plant[self.f['inv_adjust_grp_comments']] = f'Status Code: {status_code}, Error: {error}'
                else:
                        plant[self.f['inv_adjust_grp_status']] = 'error'
                        plant[self.f['inv_adjust_grp_comments']] = f'Recipe not found'

        if last_verions_products:
            for key, value in last_verions_products.items():
                self.update_calc_fields(value['product_code'], value['warehouse'], value['lot_number'])

        record_id = record['_id']['$oid']
        record['answers'][self.f['inv_adjust_status']] = 'done'
        if not_found:
            comments += f'Codes not found: {not_found}.'

        record['answers'][self.f['inv_adjust_comments']] = comments
        self.lkf_api.patch_record(record, record_id)



if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    if not stock_obj.record_id:
        stock_obj.record_id = stock_obj.object_id() 
    folio = stock_obj.current_record.get('folio')
    stock_obj.inventory_adjustment(folio, stock_obj.current_record)
