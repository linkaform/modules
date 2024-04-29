# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

print('antesrrrr33344...')
from lkf_addons.addons.stock_greenhouse.stock_reports import Reports

#Se agrega path para que obtenga el archivo de Stock de este modulo
sys.path.append('/srv/scripts/addons/modules/stock_lab/items/scripts/Lab')
from lab_stock_utils import Stock

today = date.today()
year_week = int(today.strftime('%Y%W'))


class Reports(Reports, Stock):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        #base.LKF_Base.__init__(self, settings, sys_argv=sys_argv, use_api=use_api)
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.GREENHOUSE_INVENTORY_ID = self.lkm.form_id('green_house_inventroy','id')
        self.plants_by_week = {}

    def get_requierd_plan(self, yearWeek_from, yearWeek_to):
        self.columsTable_title
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id":self.PRODUCTION_PLAN,
            # f"answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}":"LAGAM",
            f"answers.{self.f['stage_4_plan_require_yearweek']}": {"$gte": int(yearWeek_from),"$lte": int(yearWeek_to) }
            }
        query = [
            {"$match": match_query},
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
                    'week': f"$answers.{self.f['stage_4_plan_require_yearweek']}",
                    'eaches': f"$answers.{self.f['stage_4_plan_requierments']}"
                    }
            },
            {'$group':
                {'_id':
                    {
                      'product_code': '$product_code',
                      'week': '$week'
                      },
                  'total': {'$sum': '$eaches'}}},
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'week':'$_id.week',
                'total': '$total'
                }
            },
            {'$sort': {'product_code': 1, 'week':1}}
        ]
        result = self.cr.aggregate(query)
        rweeks = int(yearWeek_to) - int(yearWeek_from) + 1
        req_weeks = {}
        for x in range(rweeks):
            req_weeks['required_{}'.format(int(yearWeek_from) + x)] = 0
            this_col = deepcopy(self.col_req)
            this_col['field'] = 'required_{}'.format(int(yearWeek_from) + x)
            this_col['title'] = 'Week {}'.format(int(yearWeek_from) + x)
            self.columsTable_title.append(this_col)
        default_schema = {'available':0, 'previwes':0, 'required':0, 'plant_name':""}
        default_schema.update(req_weeks)
        for r in result:
            pcode = r.get('product_code')
            week = r.get('week')
            required = r.get('total',0)
            self.plants[pcode] = self.plants.get(pcode, deepcopy(default_schema))
            self.plants[pcode]['required_{}'.format(week)] += required
            self.plants[pcode]['required'] += required
        return True

    def get_product_kardex(self):
        data = self.data.get('data')
        product_code = data.get('product_code', [])
        lot_number = data.get('lot_number',[])
        date_options = data.get('date_options', "custom")
        if date_options == "custom":
            date_from = data.get('date_from')
            date_to = data.get('date_to')
        else:
            date_from, date_to = self.get_period_dates(date_options)
            #strips time from date
            if date_from:
                date_from = str(date_from)[:10]
            if date_to:
                date_to = str(date_to)[:10]
        date_since = None
        if date_from:
            date_since = self.date_operation(date_from, '-', 1, 'day', date_format='%Y-%m-%d')
        warehouse = data.get('warehouse',[])
        if type(warehouse) == str and warehouse != '':
            warehouse = [warehouse,]
        move_type = None
        if not product_code:
            self.LKFException('prduct code is missing...')
        stock = self.get_product_stock(product_code, lot_number=lot_number, date_from=date_from, date_to=date_to)
        if not warehouse or warehouse == '':
            warehouse = self.get_warehouse('Stock')
        result = []

        product_code = self.validate_value(product_code)
        lot_number = self.validate_value(lot_number)
        warehouse = self.validate_value(warehouse)
        date_since = self.validate_value(date_since)
        for idx, wh in enumerate(warehouse):
            # if wh != 'Lab B':
            #     continue
            print(f'============ Warehouse: {wh} ==========================')

            if not date_from and not date_since:
                 initial_stock = {'actuals': 0}
            else:
                initial_stock = self.get_product_stock(product_code, warehouse=wh, lot_number=lot_number,  date_to=date_since)
            # print('initial_stock........',initial_stock)
            # print('acrrranca........')
            moves = self.detail_stock_moves(wh, product_code=product_code, lot_number=lot_number, date_from=date_from, date_to=date_to)
            moves = self.detail_adjustment_moves(wh, product_code=product_code, lot_number=lot_number, \
                date_from=date_from, date_to=date_to, **{'result':moves})
            moves = self.detail_production_moves(wh, product_code=product_code, lot_number=lot_number, \
                date_from=date_from, date_to=date_to, **{'result':moves})
            moves = self.detail_many_one_one(wh, product_code=product_code, lot_number=lot_number, \
                date_from=date_from, date_to=date_to, **{'result':moves})
            moves = self.detail_scrap_moves(wh, product_code=product_code, lot_number=lot_number, \
                date_from=date_from, date_to=date_to, **{'result':moves})
            # moves = self.detail_many_one_one(wh, product_code=product_code, lot_number=lot_number, \
            #     date_from=date_from, date_to=date_to, **{'result':moves})
            #todo scrap out many one many
            if moves:
                warehouse_data = { "id":idx, "warehouse":wh, "qty_out_table":"Initial", 
                    "balance_table":initial_stock.get('actuals'),
                    "serviceHistory": self.set_kardex_order(initial_stock.get('actuals'), moves)
                    }
                result.append(warehouse_data)
            #scrap = self.detail_stock_move(wh)
            #todo_gradinscraping
            # print('moves=', moves)
        return result, stock.get('actuals',)

    def query_get_stock_by_cutWeek(self, yearWeek_from, yearWeek_to, status='active'):
        global plants, col_stok, columsTable_stock
        stage ='S3'
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id":self.FORM_INVENTORY_ID,
            f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['reicpe_stage']}":stage,
            f"answers.{self.f['inventory_status']}": status,
            f"answers.{self.f['new_cutweek']}": {"$lte": int(yearWeek_to) }
            }
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'plant_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'plant_name':{ "$arrayElemAt": [ f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_name']}",0]},
                    #'plant_name': f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_name']}",
                    'cutweek':{"$toInt":f"$answers.{self.f['new_cutweek']}"},
                    'eaches': f"$answers.{self.f['actual_eaches_on_hand']}",
                    }
            },
            # {'$project':
            #     {'_id': 1,
            #         'plant_code': '$plant_code',
            #         'plant_name':'$plant_name',
            #         'cutweek':{"$sum" :['$planted_yearweek','$growth_week'] },
            #         'eaches': '$eaches'}},
            # {"$match": {'cutweek': {"$lte": int(yearWeek_to) }}},
            {'$group':
                {'_id':
                    {
                    'plant_code': '$plant_code',
                    'plant_name': '$plant_name',
                    'cutweek': '$cutweek',
                      },
                  'total': {'$sum': '$eaches'}}},
            {'$project':
                {'_id': 0,
                'plant_code': '$_id.plant_code',
                'plant_name': '$_id.plant_name',
                'cutweek': '$_id.cutweek',
                'total': '$total'
                }
            },
            {'$sort': {'plant_code': 1, 'cutweek':1}}
            ]
        # print('query=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}

        rweeks = int(yearWeek_to) - int(yearWeek_from) + 1
        req_weeks = {}
        for x in range(rweeks):
            req_weeks['wstock_{}'.format(int(yearWeek_from) + x)] = 0
            this_col = deepcopy(self.col_req)
            this_col['field'] = 'wstock_{}'.format(int(yearWeek_from) + x)
            this_col['title'] = 'Stock Week {}'.format(int(yearWeek_from) + x)
            self.columsTable_stock.append(this_col)
        default_schema = {'available':0, 'previwes':0, 'required':0, 'plant_name':""}
        default_schema.update(req_weeks)
        for r in res:
            pcode = r.get('plant_code')
            week = r.get('cutweek')
            self.plants[pcode] = self.plants.get(pcode, deepcopy(default_schema))
            self.plants[pcode]['plant_name'] = r.get('plant_name')
            if week < int(yearWeek_from):
                self.plants[pcode]['previwes'] += r['total']
            else:
                self.plants[pcode]['available'] += r['total']
                #if week > int(yearWeek_from):
                self.plants[pcode]['wstock_{}'.format(week)] = r['total']
        return True

    def query_get_stock(self, product_code=None, stage=None, lot_number=None, warehouse=None, location=None, status='active' ):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.FORM_INVENTORY_ID,
            f"answers.{self.f['inventory_status']}": status
            }
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if stage:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['reicpe_stage']}":stage})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'cut_yearWeek': f"$answers.{self.f['plant_cut_year']}",
                    'cut_week': f"$answers.{self.f['production_cut_week']}",
                    'eaches': f"$answers.{self.f['actual_eaches_on_hand']}"
                    }
            },
            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      'cut_yearWeek': '$cut_yearWeek',
                      # 'cut_week': '$cut_week'
                      },
                  'total': {'$sum': '$eaches'}}},
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'cut_yearWeek': '$_id.cut_yearWeek',
                'total': '$total',
                'from': 'Stage 3'
                }
            },
            {'$sort': {'_id.product_code': 1, '_id.cut_yearWeek': 1, '_id.cut_week':1}}
            ]
        print('query=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = [r for r in res]
        all_codes = []
        for r in result:
            if r.get('product_code') and r.get('product_code') not in all_codes:
                all_codes.append(r['product_code'])
        return result, all_codes

    def query_greenhouse_stock(self, product_code=None, all_codes=[], status='active'):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id":self.GREENHOUSE_INVENTORY_ID,
            f"answers.{self.f['inventory_status']}": status
            }
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code,})
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'plant_name': {"$arrayElemAt" : [f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_name']}",0]},
                    'product_lot': f"$answers.{self.f['product_lot']}",
                    'eaches': f"$answers.{self.f['product_lot_actuals']}"}},
            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      'plant_name': '$plant_name',
                      'product_lot': '$product_lot',
                      },
                  'total': {'$sum': '$eaches'}}},
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'plant_name': '$_id.plant_name',
                'product_lot': '$_id.product_lot',
                'total_planting': '$total'
                }
            },
            {'$sort': {'_id.product_code': 1, '_id.cut_year': 1, '_id.cut_week':1}}
            ]
        res = self.cr.aggregate(query)
        # result = [r for r in res]
        result=[]
        for r in res:
            if r.get('product_code') and r.get('product_code') not in all_codes:
                all_codes.append(r['product_code'])
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

    def get_S3_requiers(self, plant_code, year_week, year_week_to):
        match_query = {
            "form_id":self.PRODUCTION_PLAN,
            f"answers.{self.f['prod_plan_S3_requier_yearweek']}": {"$gte":year_week , "$lte": year_week_to },
            "deleted_at":{"$exists":False},
            }
        if plant_code:
            if type(plant_code) == list:
                plant_code = plant_code[0]
            match_query.update({f"answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}":plant_code})
        query = [
            {"$match": match_query },
            # {"$unwind": "$answers.6226ac11de14768c8527bb3e"},
            {"$project":{
                "_id":1,
                f"plant_code":f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
                #"year_week_S4": "$answers.626c2792f7f680727fdba0f5",
                "year_week_S3": f"$answers.{self.f['prod_plan_S3_requier_yearweek']}",
                # "year_week_S2":"$answers.6226ac11de14768c8527bb3e.6227b17400e5c73fc923b4bd",
                #"requierd_S4": "$answers.6206b6186c0b3b00535d60d8",
                "requierd_S3": f"$answers.{self.f['prod_plan_require_S3']}",
                # "requierd_S2":"$answers.6226ac11de14768c8527bb3e.6226acece895584f7d0b830c",
            }},
            {"$group":{
                "_id":{
                    "plant_code":"$plant_code",
                    "year_week_S3":"$year_week_S3",
                    },
                "requierd_S3":{"$sum":"$requierd_S3"}
            }},
            {"$project":{
                "_id":0,
                "plant_code":"$_id.plant_code",
                "year_week_S3":{"$toInt":"$_id.year_week_S3"},
                "requierd_S3":"$requierd_S3",
            }},
            {"$sort": {"year_week_S3": 1}}

        ]
        result = self.cr.aggregate(query)
        res = []
        result = self.group_by_week(result, "year_week_S3", plant_code)
        print('resut333333333', result)
        return result

    def group_by_week(self, plant_list, key, plant_code):
        res = []
        for row in plant_list:
            res.append(row)
            plant_code = row.get('plant_code')
            if not self.plants_by_week.get(row.get(key)):
                self.plants_by_week[row.get(key)] = {}
            if not self.plants_by_week[row.get(key)].get(plant_code):
                self.plants_by_week[row.get(key)][plant_code] = {}
            self.plants_by_week[row.get(key)][plant_code].update(row)
        return res