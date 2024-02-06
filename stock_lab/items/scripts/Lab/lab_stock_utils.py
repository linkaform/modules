# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.stock_greenhouse.stock_utils import Stock
from lkf_addons.addons.employee.employee_utils import Employee
from lkf_addons.addons.product.product_utils import Product

today = date.today()
year_week = int(today.strftime('%Y%W'))



class Stock(Stock, Employee, Product):

    # _inherit = 'employee'

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        self.MEDIA = self.lkm.catalog_id('media')
        self.MEDIA_ID = self.MEDIA.get('id')
        self.MEDIA_OBJ_ID = self.MEDIA.get('obj_id')
        self.MEDIA_LOT = self.lkm.catalog_id('media_lot')
        self.MEDIA_LOT_ID = self.MEDIA_LOT.get('id')
        self.MEDIA_LOT_OBJ_ID = self.MEDIA_LOT.get('obj_id')
        self.WAREHOUSE_LOCATION = self.lkm.catalog_id('warehouse_locations')
        self.WAREHOUSE_LOCATION_ID = self.WAREHOUSE_LOCATION.get('id')
        self.WAREHOUSE_LOCATION_OBJ_ID = self.WAREHOUSE_LOCATION.get('obj_id')

        self.FORM_INVENTORY_ID = self.lkm.form_id('lab_inventory','id')
        self.MOVE_NEW_PRODUCTION_ID = self.lkm.form_id('lab_move_new_production','id')
        self.PRODUCTION_FORM_ID = self.lkm.form_id('lab_production','id')
        self.STOCK_MOVE_FORM_ID = self.lkm.form_id('lab_inventory_move','id')
        self.SCRAP_FORM_ID = self.lkm.form_id('lab_scrapping','id')
        self.ADJUIST_FORM_ID = self.lkm.form_id('lab_inventory_adjustment','id')

        self.container_per_rack = {
                'Baby Jar': 38,
                'baby_jar': 38,
                'Magenta Box': 24,
                'magenta_box': 24,
                'Box': 24,
                'box': 24,
                'Clam Shell': 8,
                'clam_shell': 8,
                'setis': 1,
                'Setis': 1,
            }
        self.f.update({
            'actual_eaches_on_hand':'620ad6247a217dbcb888d172',
            'media_name':'61ef43c226fd42cc223c98f7',
            'media_lot':'62e948f79928ba006783dc5c',
            'move_status':'62e9d296cf8d5b373b24e028',
            'plant_contamin_code':'6441d33a153b3521f5b2afcb',
            'plant_cycle':'620ad6247a217dbcb888d168',
            'plant_group':'620ad6247a217dbcb888d167',
            'plant_stage':'621007e60718d93b752312c4',
            'plant_cut_year':'620a9ee0a449b98114f61d75',
            'plant_cut_yearweek':'620a9ee0a449b98114f61d75',
            'plant_per_container':'620ad6247a217dbcb888d170',
            'plant_multiplication_rate':'645576e878f3060d1f7fc61b',
            'plant_next_cutweek':'6442e25f13879061894b4bb1',
            'plant_conteiner_type':'620ad6247a217dbcb888d16f',
            'production_cut_week':'622bb9946d0d6fef17fe0842',
            'production_cut_day':'620a9ee0a449b98114f61d76',
            'production_contamin_code':'ff0000000000000000000001',
            'production_estimated_hrs':'ab0000000000000000000111',
            'production_folio':'62fc62dfb26856412d2fe4ca',
            'production_group_estimated_hrs':'ab000000000000000000a111',
            'production_group':'61f1fab3ce39f01fe8a7ca8c',
            'production_eaches_req':'642dbe5638a8255f77dcdad6',
            'production_total_containers':'63f6de096468162a9a3c2ef4',
            'production_total_eaches':'63f6e1733b076aaf80ff4adb',
            'production_left_overs':'61f1fd95ef44501511f7f161',
            'production_containers_in':'61f1fcf8c66d2990c8fc7cc2',
            'production_multiplication_rate':'61f1fcf8c66d2990c8fc7cc8',
            'production_order_status':'62fbbf2587546d976e05dc7b',
            'production_working_group':'dd0000000000000000000001',
            'production_working_cycle':'cc0000000000000000000001',
            'set_production_date_out':'61f1fcf8c66d2990c8fcccc6',
            'new_location_group':'63193fc51b3cefa880fefcc7',
            'new_location_racks':'c24000000000000000000001',
            'new_location_containers':'6319404d1b3cefa880fefcc8',
            'warehouse_location':'65ac6fbc070b93e656bd7fbe',

            })

    def add_racks_and_containers(self, container_type, racks, containers):
        container_on_racks = racks * self.container_per_rack[container_type]
        print('container_on_racks', container_on_racks)
        print('containers', containers)
        qty = containers + container_on_racks
        return qty

    #### Se heredaron funciones para hacer lote tipo string

    def detail_stock_moves(self, warehouse=None, move_type=[], product_code=None, lot_number=None,  location=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MOVE_FORM_ID,
            }
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if warehouse:
            if type(warehouse) == list:
                warehouse_from = warehouse[0]
                warehouse_to = warehouse[1].lower().replace(' ', '_')
            else:
                warehouse_from = warehouse
                warehouse_to = warehouse.lower().replace(' ', '_')

            print('move_type', move_type)
            if move_type =='out' or 'out' in move_type:
                match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse_from})      
            if move_type =='in' or 'in' in move_type:
                match_query.update({f"answers.{self.f['move_new_location']}":warehouse_to})
            if not move_type:
                match_query.update(
                    {"$or":
                        [{f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse_from},
                        {f"answers.{self.f['move_new_location']}":warehouse_to}
                        ]
                    })      
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":location})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'folio': "$folio",
                    'created_at': "$created_at",
                    'date': f"$answers.{self.f['grading_date']}",
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'lot_number': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}",
                    'warehouse_from': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}",
                    'warehouse_to':f"$answers.{self.f['move_new_location']}",
                    'move_type':{ "$cond":[ 
                        {"$eq":[ f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}" , warehouse]}, 
                        "Out", 
                        "In" ]},
                    'qty':f"$answers.{self.f['inv_move_qty']}"
                    }
            },
            {'$project':
                {'_id': 1,
                    'folio': "$folio",
                    'created_at': "$created_at",
                    'date': "$date",
                    'product_code': "$product_code",
                    'lot_number': "$lot_number",
                    'warehouse_from': "$warehouse_from",
                    'warehouse_to': "$warehouse_to",
                    'move_type': "$move_type",
                    'qty_in' :{ "$cond":[ 
                        {"$eq":[ "$move_type" , "In"]}, 
                        "$qty", 
                        0 ]},
                    'qty_out' :{ "$cond":[ 
                        {"$eq":[ "$move_type" , "Out"]}, 
                       "$qty", 
                        0 ]},
                    }
            },
            {'$sort': {'date': 1}}
            ]
        res = self.cr.aggregate(query)
        if kwargs.get('result'):
            result = kwargs['result']
        else:
            result = {}
        for r in res:
            r['created_at'] = str(r['created_at'])
            epoch = self.date_2_epoch(r.get('date'))
            r['warehouse_to'] = r['warehouse_to'].replace('_', ' ').title()
            result[epoch] = result.get(epoch,[])
            result[epoch].append(r)
        return result

    def detail_adjustment_moves(self, warehouse=None, product_code=None, lot_number=None,  location=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ADJUIST_FORM_ID,
            f"answers.{self.f['inv_adjust_status']}":{"$ne":"cancel"}
            }
        print('adjustment', warehouse)
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        match_query_stage2 = {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"}
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if inc_folio:
            match_query_stage2 = {"$or": [
                {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"},
                {"folio":inc_folio}
                ]}
        if product_code:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID }.{self.f['product_code']}":product_code})
        if lot_number:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.f['product_lot']}":lot_number})
        if location:
            match_query_stage2.update({f"answers.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_lot_location']}":location})
        query= [{'$match': match_query },
            {'$unwind': '$answers.{}'.format(self.f['grading_group'])},
            ]
        if match_query_stage2:
            query += [{'$match': match_query_stage2 }]
        query += [
            {'$project':
                {'_id': 1,
                    'folio': "$folio",
                    'created_at': "$created_at",
                    'date': f"$answers.{self.f['grading_date']}",
                    'product_code': f"$answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_code']}",
                    'warehouse': f"$answers.{self.CATALOG_WAREHOUSE_OBJ_ID}.{self.f['warehouse']}",
                    'lot_number': f"$answers.{self.f['grading_group']}.{self.f['product_lot']}",
                    'adjust_in':{ "$ifNull":[ 
                        f"$answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_in']}",
                        0]},
                    'adjust_out': { "$ifNull":[
                        f"$answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_out']}",
                        0]}
                    }
            },
            {'$project':
                {'_id': 1,
                'folio':'$folio',
                'created_at': "$created_at",
                'date':'$date',
                'product_code':'$product_code',
                'lot_number':'$lot_number',
                'move_type': { "$cond":[ 
                        {"$gt":[ f"$adjust_in" , 0]}, 
                        "In", 
                        "Out" ]},
                'warehouse_from':{ "$cond":[ 
                        {"$gt":[ f"$adjust_in" , 0]}, 
                        "Adujstment", 
                        "$warehouse" ]},
                'warehouse_to':{ "$cond":[ 
                        {"$gt":[ f"$adjust_in" , 0]}, 
                        "$warehouse", 
                        "Adujstment" ]},
                'qty_in': "$adjust_in",
                'qty_out': "$adjust_out",
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        move_type = kwargs.get('move_type')
        print('move_type', move_type)
        if kwargs.get('result'):
            result = kwargs['result']
        else:
            result = {}
        for r in res:
            if move_type == 'in':
                if r['warehouse_to'] != 'Adujstment':
                    continue
            if move_type == 'out':
                if r['warehouse_from'] != 'Adujstment':
                    continue
            r['created_at'] = str(r['created_at'])
            epoch = self.date_2_epoch(r.get('date'))
            result[epoch] = result.get(epoch,[])
            result[epoch].append(r)
        return result

    def detail_production_moves(self, warehouse=None, product_code=None, lot_number=None,  location=None, date_from=None, date_to=None, status='posted', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PRODUCTION_FORM_ID,
            }
        match_query_stage2 = {}
        if date_from or date_to:
            match_query_stage2.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=f"{self.f['production_group']}.{self.f['set_production_date']}"))
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query.update({f"answers.{self.f['production_lote']}":lot_number})  
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})    
        if location:
            match_query_stage2.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot_location']}":location})    
        if status:
            match_query_stage2.update({f"answers.{self.f['production_group']}.{self.f['production_status']}":status})
        query= [{'$match': match_query },
            {'$unwind': f"$answers.{self.f['production_group']}"},
            ]
        if match_query_stage2:
            query += [{'$match': match_query_stage2 }]
        query += [
            {'$project':
                {'_id': 1,
                    'folio':'$folio',
                    'date': f"$answers.{self.f['production_group']}.{self.f['set_production_date']}",
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'lot_number':f"$answers.{self.f['production_lote']}",
                    'total': f"$answers.{self.f['production_group']}.{self.f['set_total_produced']}",
                    }
            },
            {'$group':
                {'_id':
                    { 
                        'id': '$_id',
                        'product_code': '$product_code',
                        'date': '$date',
                        'lot_number': '$lot_number',
                        'folio': '$folio',
                      },
                  'total': {'$sum': '$total'},
                  }
            },
            {'$project':
                {'_id': '$_id.id',
                'product_code': '$_id.product_code',
                'date': '$_id.date',
                'lot_number': '$_id.lot_number',
                'folio': '$_id.folio',
                'qty_in': '$total',
                'move_type':'In',
                'warehouse_from':"Production",
                'warehouse_to':warehouse,
                }
            },
            {'$sort': {'date': 1}}
            ]
        res = self.cr.aggregate(query)
        if kwargs.get('result'):
            result = kwargs['result']
        else:
            result = {}
        for r in res:
            epoch = self.date_2_epoch(r.get('date'))
            result[epoch] = result.get(epoch,[])
            result[epoch].append(r)
        return result

    def detail_scrap_moves(self, warehouse=None, product_code=None, lot_number=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": {"$in":[self.SCRAP_FORM_ID, self.GRADING_FORM_ID]}
            }
        print('warehouse', warehouse)
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})    
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})    
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot_location']}":location})    
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})    
        if status:
            match_query.update({f"answers.{self.f['inv_scrap_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [
            {'$match': match_query },
            {'$project':
                {'_id': 1,
                    'date': f"$answers.{self.f['grading_date']}",
                    'created_at': "$created_at",
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'lot_number': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}",
                    'warehouse_from': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}",
                    'scrap': f"$answers.{self.f['inv_scrap_qty']}",
                    'cuarentin': f"$answers.{self.f['inv_cuarentin_qty']}",
                    'warehouse_to':'Scrap',
                    }
            },
            {'$sort': {'date': 1}}
            ]
        res = self.cr.aggregate(query)
        move_type = kwargs.get('move_type')
        if kwargs.get('result'):
            result = kwargs['result']
        else:
            result = {}
        for r in res:
            r['created_at'] = str(r['created_at'])
            epoch = self.date_2_epoch(r.get('date'))
            result[epoch] = result.get(epoch,[])
            cuarentine = r.get('cuarentin',0)
            scrap = r.get('scrap',0)
            print('r=',r)
            print('scrap=',scrap)
            if cuarentine:
                r.update({
                    'warehouse_to': "Cuarentin",
                    'qty_out':cuarentine,
                    })
                result[epoch].append(r)
            if scrap:
                if move_type == 'in':
                    if r['warehouse_to'] != 'Scrap':
                        continue
                if move_type == 'out':
                    if r['warehouse_from'] != 'Scrap':
                        continue
                print('qty out', scrap)
                r.update({
                    'warehouse_to': "Scrap",
                    'qty_out':scrap,
                    })
                result[epoch].append(r)
        return result

    #### Temino heredacion para hace lote tipo string

    # def calculations(self):
    #     folio = self.folio
    #     print('******** Folio =', folio)
    #     # if not current_record.get('folio'):
    #     if True:

    #         current_sets = self.answers.get(self.f['production_group'], [])
    #         new_sets = []
    #         year = str( self.answers.get(self.f['production_year'], '') ).zfill(2)
    #         week = str( self.answers.get('61f1da41b112fe4e7fe85830', '') ).zfill(2)
    #         year_week = year + week
    #         total_eaches = 0
    #         total_flats = 0
    #         total_hrs = 0
    #         dict_growing = {}
    #         #Ready YearWeek
    #         plant_date = datetime.strptime('%04d-%02d-1' % (int(year), int(week)), '%Y-%W-%w')
    #         for s in current_sets:
    #             if not s.get('62e4bc58d9814e169a3f6beb'):
    #                 continue
    #             c = s.get('61ef32bcdf0ec2ba73dec33c')
    #             plant = s['61ef32bcdf0ec2ba73dec33c']
    #             required_eaches = s.get('62e4bc58d9814e169a3f6beb')
    #             total_eaches += required_eaches

    #             # Obteniendo la Receta
    #             plant_code = s.get('61ef32bcdf0ec2ba73dec33c', {}).get('61ef32bcdf0ec2ba73dec33d', '')
    #             # plant = Plant(plant_code, stage='4')
    #             # ss = plant.get_week_recipe('Ln72',start_size='S4', plant_date=plant_date )
    #             # print('recipes', ss)
    #             recipes = self.get_plant_recipe([plant_code,], stage=[4, 'Ln72',])
    #             if not recipes.get(plant_code):
    #                 error_msg = {
    #                         "61ef32bcdf0ec2ba73dec33d": {"msg": [ "No recipe with Start Size S4 found for : {}".format(plant_code)], "label": "Plant code", "error": []}
    #                     }
    #                 raise Exception(simplejson.dumps(error_msg))
    #             recipe = self.select_S4_recipe(recipes[plant_code], week)
    #             # print('=== recipe',simplejson.dumps(recipe, indent=4))

    #             #per container
    #             if folio and plant.get('6205f73281bb36a6f157335b'):
    #                 per_container = plant.get('6205f73281bb36a6f157335b')[0]
    #             else:
    #                 per_container = recipe.get('per_container')

    #             qty_per_container = float( per_container )
    #             subtotal_flats = math.ceil(required_eaches / qty_per_container)
    #             total_flats += subtotal_flats
    #             s['63f6db6474ef4ca424ff48e3'] = subtotal_flats

    #             #Growing Media
    #             if folio and plant.get('6209705080c17c97320e3383'):
    #                 growing_media = plant.get('6209705080c17c97320e3383')
    #             else:
    #                 growing_media = recipe.get('soil_type')
    #             dict_growing[growing_media] = dict_growing.get(growing_media,0)
    #             dict_growing[growing_media] += subtotal_flats

    #             #cut_productivity
    #             if folio and plant.get('6209705080c17c97320e337f'):
    #                 cut_productivity = plant.get('6209705080c17c97320e337f')[0]
    #             else:
    #                 cut_productivity = recipe.get('cut_productivity',10)

    #             #Estimated Hours
    #             est_hours = round(subtotal_flats / cut_productivity, 2)
    #             s['63ffa3ce39960473f4591ea6'] = est_hours
    #             total_hrs += est_hours

    #             #plant Start Week
    #             if folio and plant.get('6209705080c17c97320e3380'):
    #                 start_week = plant.get('6209705080c17c97320e3380')
    #                 print('ya trae los start weeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeek', start_week)
    #             else:
    #                 start_week = recipe.get('start_week')

    #             if folio and plant.get('6205f73281bb36a6f1573357'):
    #                 grow_weeks = plant.get('6205f73281bb36a6f1573357')[0]
    #                 print('ya trae los gorw weeks', grow_weeks)
    #             else:
    #                 grow_weeks = recipe.get('S4_growth_weeks')

    #             ready_date = plant_date + timedelta(weeks=grow_weeks)

    #             #plant name
    #             if folio and plant.get('61ef32bcdf0ec2ba73dec33e'):
    #                 plant_name = plant.get('61ef32bcdf0ec2ba73dec33e')[0]
    #             else:
    #                 plant_name = recipe.get('plant_name')

    #             #Start Size
    #             if folio and plant.get('6205f73281bb36a6f1573358'):
    #                 start_size = plant.get('6205f73281bb36a6f1573358')
    #             else:
    #                 start_size = recipe.get('start_size')




    #             s['63f8f4cad090912501be306a'] = int(ready_date.strftime('%Y%W'))
    #             # print('=== plant',simplejson.dumps(plant, indent=4))
    #             s['61ef32bcdf0ec2ba73dec33c'].update({
    #                 '6205f73281bb36a6f1573358': start_size,
    #                 '6209705080c17c97320e3383': growing_media,
    #                 '6209705080c17c97320e3380': start_week,
    #                 #plant.get('6209705080c17c97320e3380',recipe.get('start_week')),
    #                 '61ef32bcdf0ec2ba73dec33e': [plant_name],
    #                 '6205f73281bb36a6f157335b': [per_container],
    #                 '6205f73281bb36a6f1573357': [grow_weeks],
    #                 '6209705080c17c97320e337f': [cut_productivity],
    #             })
    #             s['640114b2cc0899ba18000006'] = s.get('640114b2cc0899ba18000006','lab')
    #             new_sets.append(s)
    #         if new_sets:
    #             self.answers['62e4babc46ff76c5a6bee76c'] = new_sets


    #     self.answers['63f6e1733b076aaf80ff4adb'] = total_eaches
    #     self.answers['63f6de096468162a9a3c2ef4'] = total_flats
    #     self.answers['63f6de096468162a9a3c2ef5'] = round(total_hrs, 2)
    #     self.answers['62e4bd2ed9814e169a3f6bef'] = self.answers.get('62e4bd2ed9814e169a3f6bef','planning')

    #     list_soils_requirements = []
    #     for growing in dict_growing:
    #         list_soils_requirements.append({
    #             '63f6de5f6468162a9a3c2f09': growing,
    #             '63f6de5f6468162a9a3c2f0a': dict_growing[growing]
    #         })
    #     if list_soils_requirements:
    #         self.answers['63f6de096468162a9a3c2ef3'] = list_soils_requirements

    #     sys.stdout.write(simplejson.dumps({
    #         'status': 101,
    #         'replace_ans': self.answers,
    #     }))

    def calculates_production_warehouse(self):
        production_recipe = self.answers.get(self.f['product_recipe'], {})
        prod_status = self.answers.get(self.f['production_left_overs'],'')
        print('self.TEAM_OBJ_ID', self.TEAM_OBJ_ID)
        print('self.TEAM_OBJ_ID', self.answers.get(self.TEAM_OBJ_ID))
        team = self.answers.get(self.TEAM_OBJ_ID).get(self.f['team_name'])
        qty_per_container = production_recipe.get(self.f['reicpe_per_container'], [])
        if qty_per_container:
            if type(qty_per_container) == list and qty_per_container[0]:
                qty_per_container = int( qty_per_container[0] )
            else:
                qty_per_container = int(qty_per_container)
        else:
            qty_per_container = 0
        from_stage = production_recipe.get(self.f['reicpe_stage'])
        to_stage = production_recipe.get(self.f['reicpe_start_size'])
        is_S2_to_S3 = True if from_stage == 'S2' and to_stage == 'S3' else False

        #inv_qty_per_container = 0
        total_left_overs = 0
        move_inventory = []
        per_container_selected = int(self.answers.get(self.f['production_per_container_in'],0 ))
        worked_containers = []
        total_picked_containers = 0

        total_container_out = 0
        total_container_out_progress = 0
        total_container_used = 0
        mult_out = 0
        mult_in = 0
        weighted_mult_rate = 0
        for production in self.answers.get(self.f['production_group'], []):
            """
            ##################################################
            # Calculate Total Hours
            ##################################################
            """
            production_status = production.get(self.f['production_status'],'progress')
            containers_in = float(production.get(self.f['production_containers_in'],0))
            containers_out = float(production.get(self.f['set_total_produced'],0))
            total_container_out += containers_out
            print('production_status', production_status)
            if production_status == 'progress':# or production_status == 'posted':
                date_in = production.get(self.f['set_production_date'])
                time_in = production.get(self.f['time_in'])
                date_out = production.get(self.f['set_production_date_out'], date_in)
                time_out = production.get(self.f['time_out'])
                print('dateout', date_out)
                print('time_out', time_out)
                cutter = production.get(self.EMPLOYEE_OBJ_ID,{}).get(self.f['worker_name'])
                d_time_in =  datetime.strptime('{} {}'.format(date_in,time_in), '%Y-%m-%d %H:%M:%S')
                d_time_out = datetime.strptime('{} {}'.format(date_out,time_out), '%Y-%m-%d %H:%M:%S')
                secs = (d_time_out - d_time_in).total_seconds()
                if secs < 0:
                    msg = "The time in and time out for the production set of the cutter: {}, is wrong.".format(cutter)
                    msg += " It was capture that see started at {} and finished at {}, having a difference of {} seconds.".format(time_in,
                        time_out, secs)
                    msg_error_app = {
                             self.f['time_in']: {"msg": [msg], "label": "Please time in. ", "error": []},
                             self.f['time_out']: {"msg": [msg], "label": "Please time out. ", "error": []},
                         }
                    raise Exception( simplejson.dumps( msg_error_app ) )
                total_hours = secs / 60.0**2
                luch_brake = production.get(self.f['set_lunch_brake'])
                if luch_brake == 'sí':
                    total_hours -= 0.5
                production[self.f['set_total_hours']] = round(total_hours, 2) # Total Hours

                """
                ##################################################
                # Calculate Multiplication Rate
                ##################################################
                """
                total_container_used += containers_in
                mult_out +=  (containers_out * qty_per_container)
                mult_in +=  float( containers_in * per_container_selected )
                multiplication_rate = (containers_out * qty_per_container) / float( containers_in * per_container_selected )
                production[self.f['production_multiplication_rate']] = round(multiplication_rate, 2) # Multiplication Rate

                """
                ##################################################
                # Calculate Plants per Hour
                # Cuantas plantas se hacen por hora
                ##################################################
                """
                total_plants = qty_per_container * containers_out
                if total_hours <= 0:
                    t_in = d_time_in.strftime('%H:%M')
                    t_out = d_time_out.strftime('%H:%M')
                    msg = "Double check your time in {} and time out {} input, of {}.".format(t_in, t_out, cutter)
                    msg_error_app = {
                             self.f['time_in']: {"msg": [msg], "label": "Please check your Time In. ", "error": []},
                             self.f['time_out']: {"msg": [msg], "label": "Please check your Time Out. ", "error": []}
                         }
                    self.LKFException( simplejson.dumps( msg_error_app ) )
                plants_per_hour = total_plants / float(total_hours)

                production[self.f['set_products_per_hours']] = round(plants_per_hour, 2) # Plants per Hour

                """
                ##################################################
                # Genera registro para almacenar nueva produccion
                # Cambia status de produccion a posted
                ##################################################
                """
                total_container_out_progress += containers_out
                production[self.f['production_status']] = 'posted'

                # remainding_cont = 0
                weighted_mult_rate = round(mult_out/mult_in, 2)
        
        self.answers[self.f['total_produced']] = total_container_out
        
        if prod_status == 'finished':
            self.answers[self.f['production_order_status']] = 'done'
            #remainding_cont = total_picked_containers - total_container_used - total_left_overs
            # print('the math picked - used - left = 0 / {} - {} - {} = {}'.format(total_picked_containers,
            # total_container_used, total_left_overs ,remainding_cont ))

            # if remainding_cont != 0:
            #     msg = "There are {} container  remainding. Picked Containers {} - Containers IN {} - Left Overs {} != 0".format(remainding_cont,
            #         total_picked_containers, total_container_used, total_left_overs)
            #     msg_error_app = {
            #              "62e98cb229e764936db75244": {"msg": [msg], "label": "Please check your Left Overs. ", "error": []}
            #          }
            #     raise Exception( simplejson.dumps( msg_error_app ) )

        print('total', total_container_out_progress)
        if total_container_out_progress > 0:
            new_prod_line = self.get_production_move(total_container_out_progress, weighted_mult_rate, d_time_out)
            print('new_prod_line=', simplejson.dumps(new_prod_line, indent=4))
            print('frowth weekds', new_prod_line[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_growth_weeks']])
            response_create = self.create_production_move(new_prod_line)
            status_code = response_create.get('status_code')
            if status_code >= 400:
                msg_error_app = response_create.get('json', 'Error de Script favor de reportrar con Admin')
                self.LKFException( simplejson.dumps(msg_error_app) )

        """
        ##################################################
        # Si hay sobrantes Crea el registro
        ##################################################
        """
        # print('prod_status=',prod_status)
        # if prod_status == 'finished':
        #     #adjust_inventory_flow(worked_containers)
        #     #create_move_line(current_record, move_inventory)
        #     self.answers[self.f['production_status']] = 'done'
        #     sys.stdout.write(simplejson.dumps({
        #         'status': 101,
        #         'replace_ans': self.answers,
        #     }))
        # else:
        #     self.answers[self.f['production_status']] = 'in_progress'
        #     sys.stdout.write(simplejson.dumps({
        #         'status': 101,
        #         'replace_ans': self.answers
        #     }))

    def create_inventory_flow(self, answers):
        #data es un json con answers y folio. Que puede ser el current record
        # answers_to_new_record['620ad6247a217dbcb888d176'] = 'todo' # Post Status
        print('answers',answers)
        product_code = answers[self.PRODUCT_RECIPE_OBJ_ID][self.f['product_code']]
        lot_number = answers[self.f['product_lot']]
        warehouse = answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
        location = answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
        print('product_code',product_code)
        print('lot_number',lot_number)
        print('warehouse',warehouse)
        print('location',location)
        product_exist = self.product_stock_exists(product_code, warehouse, location=location, lot_number=lot_number)
        print('product_exist',product_exist)
        if product_exist:
            print('update production and acutals')
            res = self.update_calc_fields(product_code, warehouse, lot_number, location=location)
            return res
        else:
            metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
            metadata.update({
                'properties': {
                    "device_properties":{
                        "system": "Script",
                        "process": answers.get('process', 'Inventory Move - New Production'),
                        "action": answers.get('action', 'Create record Inventory Flow'),
                        "from_folio":self.folio,
                        "script":"make_inventory_flow.py",
                        "module":"stock_lab",
                        "function": "create_inventory_flow",
                    }
                },
                'answers': answers
                })
        return {'new_record':metadata}

    def create_proudction_lot_number(self, prod_date=None):
        if not prod_date:
            year = today.strftime('%Y')
            day_num = today.strftime('%j')
        else:
            year = prod_date.strftime('%Y')
            day_num = prod_date.strftime('%j')            
        group = self.answers.get(self.f['production_working_group'])
        cycle = self.answers.get(self.f['production_working_cycle'])
        lot_number = f"{year}{day_num}-{group}{cycle}"
        return lot_number

    def create_production_move(self, new_production):
        print('self.MOVE_NEW_PRODUCTION_ID')
        metadata = self.lkf_api.get_metadata(form_id=self.MOVE_NEW_PRODUCTION_ID, user_id=self.record_user_id )
        metadata.update({
            'properties': {
                "device_properties":{
                    "System": "Script",
                    "Module": "Stock Lab",
                    "Process": "Production Left Overs",
                    "Action": 'Return Inventroy',
                    "From Folio": self.folio,
                    "File": "lab_stock_utils.py"
                }
            },
        })
        print('aqui va a asginmar el folio', self.folio)
        # metadata['folio'] = self.create_poruction_lot_number()
        metadata.update({'answers':new_production})
        response_create = self.lkf_api.post_forms_answers(metadata, jwt_settings_key='USER_JWT_KEY')
        return response_create

    def from_lab_to_inventory(self):
        return self.answers

    def make_inventory_flow(self):
        ####
        #### Si la nueva ubicacion de almacen esta dentro de un grupo la saca y llama a la funcion nuevamente
        ####
        new_records_data = []
        result = []
        # fields_to_new_record = self.get_record_form_fields(self.FORM_INVENTORY_ID)
        if self.answers.get(self.f['new_location_group']):
            move_group = self.answers.pop(self.f['new_location_group'])
            container_type = self.answers.get(self.f['plant_conteiner_type'])
            container_on_racks = sum([int(x[self.f['new_location_racks']]) for x in move_group]) * self.container_per_rack[container_type]
            move_qty = sum([int(x[self.f['new_location_containers']]) for x in move_group]) + container_on_racks
            record_qty =  int(self.answers.get(self.f['actuals'],0))
            if record_qty != move_qty:
                msg_error_app = {
                    self.f['actuals']:
                        {"msg": ["There are {} Containers on the record, but you are trying to alocate {}".format(record_qty, move_qty)],
                        "label": "Containers on Hand", "error": []}
                }
                raise Exception( simplejson.dumps( msg_error_app ) )
            for location in move_group:
                if self.form_id == self.MOVE_NEW_PRODUCTION_ID:
                    production = True
                else:
                    production = False
                answers = deepcopy(self.answers)
                answers.update(self.set_inventroy_format(answers, location, production=production ))
                product_code = self.answers[self.PRODUCT_RECIPE_OBJ_ID][self.f['product_code']]
                product_lot  = self.answers[self.f['product_lot']]
                warehouse = location[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
                location_id = location[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
                answers.update(self.set_up_containers_math(answers, record_qty, location, production=production ))
                production_qty  = answers.get(self.f['production'],0)
                
                self.cache_set({
                        '_id': f'{product_code}_{product_lot}_{warehouse}_{location_id}',
                        'production':production_qty,
                        'product_lot':product_lot,
                        'product_code':product_code,
                        'warehouse': warehouse
                        })
                res = self.create_inventory_flow(answers)
                if res.get('new_record'):
                    new_records_data.append(res['new_record'])
                else:
                    result.append(res)

        else:
            print('************ empieza *******************')
            res = self.create_inventory_flow(answers)
            if res.get('new_record'):
                new_records_data = res['new_record']
            else:
                result.append(res)
            
            print('1new_records_data',new_records_data)
    
        if new_records_data:
            # for x in new_records_data:
            #     print('x=',simplejson.dumps(x, indent=4))

            res_create = self.lkf_api.post_forms_answers_list(new_records_data)
            return res_create
        return result

    def get_plant_prodctivity(self, answers):
        group = answers.get(self.f['production_group'], [])
        total_hrs = 0
        total_containers = 0
        total_eaches = 0
        print('group', group)

        print('group', self.f['production_group'])
        for gset in group:
            plant = gset.get(self.f['product_recipe'], {})

            eaches = gset.get(self.f['production_eaches_req'], 0)
            print('eaches', eaches)
            plt_container =  plant.get(self.f['reicpe_per_container'],0)
            print('per_container', plt_container)
            if eaches:
                containers =  round(eaches/plt_container,0)
                gset[self.f['production_requier_containers']] = containers
                total_containers += containers
                total_eaches += eaches
            print('production_requier_containers', gset[self.f['production_requier_containers']])
            plant_per_hr = plant.get(self.f['reicpe_productiviy'],[])
            if plant_per_hr and len(plant_per_hr) > 0:
                plant_per_hr = plant_per_hr[0]
            else:
                continue
            requier_cont = gset.get(self.f['production_requier_containers'],0)
            plants_needed =  int(plt_container) * int(requier_cont)
            set_hrs = round(float(plants_needed) / float(plant_per_hr), 1)
            total_hrs += set_hrs
            gset[self.f['production_group_estimated_hrs']] = round(set_hrs,2)

        answers[self.f['production_group']] =  group
        answers[self.f['production_estimated_hrs']] = round(total_hrs,2)
        print('total eaches', total_eaches)
        answers[self.f['production_total_eaches']] = total_eaches
        answers[self.f['production_total_containers']] = total_containers
        return answers

    def get_production_move(self, new_containers, weighted_mult_rate, production_date):
        res = {}

        res[self.PRODUCT_RECIPE_OBJ_ID ] = deepcopy(self.answers.get(self.PRODUCT_RECIPE_OBJ_ID, {}))
        soil_type = self.unlist(self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_soil_type'],""))
        res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_soil_type']] = soil_type
        res[self.TEAM_OBJ_ID] = self.answers.get( self.TEAM_OBJ_ID,{})
        res[self.MEDIA_LOT_OBJ_ID] = self.answers.get(self.MEDIA_LOT_OBJ_ID,{})

        res[self.f['set_production_date']] = str(production_date)
        # prod_date = self.date_from_str(production_date)
        print('year', production_date.strftime('%Y'))
        res[self.f['plant_cut_year']] = int(production_date.strftime('%Y'))
        res[self.f['production_cut_week']] = int(production_date.strftime('%W'))
        res[self.f['production_cut_day']] = int(production_date.strftime('%j'))
        res[self.f['plant_group']] = self.answers.get(self.f['production_working_group'])
        res[self.f['plant_cycle']] = self.answers.get(self.f['production_working_cycle'])
        res[self.f['product_lot']] = self.create_proudction_lot_number(production_date)
        res[self.f['plant_contamin_code']] = self.answers.get(self.f['plant_contamin_code'])
        production_recipe = self.answers.get(self.f['product_recipe'], {})
        res[self.f['plant_stage']] = int(production_recipe.get(self.f['reicpe_start_size'])[1])
        res[self.f['plant_conteiner_type']] = self.unlist(production_recipe.get(self.f['reicpe_container'])).lower().replace(' ', '_')
        per_container = int(self.unlist(production_recipe.get(self.f['prod_qty_per_container'], [])))
        res[self.f['plant_per_container']] = per_container

        res[self.f['actuals']] = new_containers
        res[self.f['actual_eaches_on_hand']] = new_containers * per_container
        res[self.f['production_folio']] = self.folio
        res[self.f['production_multiplication_rate']] = weighted_mult_rate
        res[self.f['inventory_status']] = 'active' if res[self.f['plant_stage']] in (1,2,"1","2") else 'pull'
        res[self.f['move_status']] = 'to_do'
        print('res',res)
        return res

    #### Se heredaron funciones para hacer lote tipo string

    def get_invtory_record_by_product(self, form_id, plant_code, lot_number, warehouse, location ):
        #use to be get_record_greenhouse_inventory
        query_warehouse_inventory = {
            'form_id': form_id,
            'deleted_at': {'$exists': False},
            f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}": plant_code,
            f"answers.{self.f['product_lot']}": lot_number,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}": warehouse,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}": location,
        }
        record = self.cr.find_one(query_warehouse_inventory, {'folio': 1, 'answers': 1, 'form_id': 1, 'user_id': 1})
        return record
    #### Temino heredacion para hace lote tipo string

    def product_stock_exists(self, product_code, warehouse, location=None, lot_number=None, status=None):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.FORM_INVENTORY_ID,
            f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}": product_code,
            }
        match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})
        if lot_number:
            match_query.update({f"answers.{self.f['product_lot']}":lot_number})
        if status:
            match_query.update({f"answers.{self.f['inventory_status']}":status})
        exist = self.cr.find_one(match_query)
        return exist

    def set_inventroy_format(self, answers, location, production=False ):
        return answers

    def set_up_containers_math(self, answers, record_qty , new_location, production=False):
        per_container = int(answers[self.f['plant_per_container']])
        container_type = answers[self.f['plant_conteiner_type']]
        racks = new_location.get(self.f['new_location_racks'],0)
        containers = new_location.get(self.f['new_location_containers'],0)
        move_qty = self.add_racks_and_containers(container_type, racks, containers)
        answers.update(new_location)
        answers[self.f['actuals']] = move_qty
        answers[self.f['actual_eaches_on_hand']] = move_qty * per_container
        if production:
            answers[self.f['production']] = move_qty # qty produced
            # answers[self.f['move_out']] = record_qty - move_qty # Relocated
        return answers

    #### Se heredaron funciones para hacer lote tipo string
    def stock_adjustments(self, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ADJUIST_FORM_ID,
            f"answers.{self.f['inv_adjust_status']}":{"$ne":"cancel"}
            }
        # inc_folio = kwargs.get("kwargs",{}).get("inc_folio")
        # exclude_folio = kwargs.get("kwargs",{}).get("exclude_folio")
        inc_folio = None
        exclude_folio = None
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))

        match_query_stage2 = {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"}
        if inc_folio:
            match_query_stage2 = {"$or": [
                {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"},
                get_folios_match(inc_folio = inc_folio)
                ]}
        if product_code:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.f['product_lot']}":lot_number})
        if location:
            match_query_stage2.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot_location']}":location})
        query= [{'$match': match_query }]
        if exclude_folio:
            query += [{'$match': get_folios_match(exclude_folio=exclude_folio) }]
        query += [
            {'$unwind': '$answers.{}'.format(self.f['grading_group'])},
            ]
        if match_query_stage2:
            query += [{'$match': match_query_stage2 }]

        query += [
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_code']}",
                    'date': f"$answers.{self.f['grading_date']}",
                    'adjust': f"$answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_qty']}",
                    }
            },
            {'$sort': {'date': 1}},
            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'date': {'$last': '$date'},
                  'adjust': {'$last': '$adjust'},
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'date': '$date',
                'total': '$adjust'
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, {'total':0,'date':''})        
            result[pcode]['date'] = r.get('date',0)
            result[pcode]['total'] = r.get('total',0)
        return result  

    def stock_adjustments_moves(self, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ADJUIST_FORM_ID,
            f"answers.{self.f['inv_adjust_status']}":{"$ne":"cancel"}
            }
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        match_query_stage2 = {}
        # match_query_stage2 = {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"}
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if inc_folio:
            match_query_stage2 = {"$or": [
                {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"},
                {"folio":inc_folio}
                ]}
        if product_code:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID }.{self.f['product_code']}":product_code})
        if lot_number:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.f['product_lot']}":lot_number})
        if location:
            match_query_stage2.update({f"answers.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_lot_location']}":location})
        query= [{'$match': match_query },
            {'$unwind': '$answers.{}'.format(self.f['grading_group'])},
            ]
        if match_query_stage2:
            query += [{'$match': match_query_stage2 }]
        query += [
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_OBJ_ID}.{self.f['product_code']}",
                    'adjust_in': f"$answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_in']}",
                    'adjust_out': f"$answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_out']}",
                    }
            },
            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'adjust_in': {'$sum': '$adjust_in'},
                  'adjust_out': {'$sum': '$adjust_out'},
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'total': {'$subtract' : ['$adjust_in', '$adjust_out' ]}
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        result = 0
        for r in res:
            result = r.get('total', 0)        
        return result  

    def stock_moves(self, move_type, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, status='done', **kwargs):
        if move_type not in ('in','out'):
            raise('Move type only accepts values "in" or "out" ')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MOVE_FORM_ID,
            }
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})
        if move_type =='out':
            if warehouse:
                match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if move_type =='in':
            if warehouse:
                warehouse = warehouse.lower().replace(' ', '_')
                match_query.update({f"answers.{self.f['move_new_location']}":warehouse})    
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":location})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['inv_move_qty']}",
                    }
            },

            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'total': {'$sum': '$total'},
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'total': '$total',
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        return result  

    def stock_supplier(self, date_from=None, date_to=None, product_code=None, warehouse=None, location=None, lot_number=None,  status='posted', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MOVE_FORM_ID,
            }
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})

        supplier_warehouse = self.get_warehouse(warehouse_type='Supplier')
        if supplier_warehouse:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":{"$in":supplier_warehouse}})
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":location})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['inv_move_qty']}",
                    }
            },

            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'total': {'$sum': '$total'},
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'total': '$total',
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        return result   

    ### STOCK OUT'S

    def stock_scrap(self, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": {"$in":[self.SCRAP_FORM_ID, self.GRADING_FORM_ID]}
            }
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})    
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})    
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot_location']}":location})    
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}": lot_number})    
        if status:
            match_query.update({f"answers.{self.f['inv_scrap_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [
            {'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'scrap': f"$answers.{self.f['inv_scrap_qty']}",
                    'cuarentin': f"$answers.{self.f['inv_cuarentin_qty']}",
                    }
            },

            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'total_scrap': {'$sum': '$scrap'},
                  'total_cuarentin': {'$sum': '$cuarentin'}
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'total_scrap': '$total_scrap',
                'total_cuarentin': '$total_cuarentin'
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, {'scrap':0,'cuarentin':0})        
            result[pcode]['scrap'] += r.get('total_scrap',0)
            result[pcode]['cuarentin'] += r.get('total_cuarentin',0)
        if product_code:
            result_scrap = result.get(product_code,{}).get('scrap',0)
            result_cuarentin = result.get(product_code,{}).get('cuarentin',0) 
            return result_scrap, result_cuarentin
        else:
            return result

    #### Temino heredacion para hace lote tipo string

    def stock_production(self, date_from=None, date_to=None, product_code=None, warehouse=None, location=None, lot_number=None,  status='done', **kwargs):
        print('este es el correcto.....')
        print('date_from', date_from)
        print('date_to', date_to)
        print('product_code', product_code)
        print('lot_number', lot_number)
        print('warehouse', warehouse)
        print('location', location)
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.MOVE_NEW_PRODUCTION_ID,
            }
        match_query_stage2 = {}
        if date_from or date_to:
            match_query_stage2.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=f"{self.f['set_production_date']}"))
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if status:
            match_query.update({f"answers.{self.f['move_status']}":status})
        if lot_number:
            match_query.update({f"answers.{self.f['product_lot']}":lot_number})  
        if warehouse:
            match_query_stage2.update({f"answers.{self.f['new_location_group']}.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})    
        if location:
            match_query_stage2.update({f"answers.{self.f['new_location_group']}.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})    
        query= [{'$match': match_query },
            {'$unwind': f"$answers.{self.f['new_location_group']}"},
            ]
        if match_query_stage2:
            query += [{'$match': match_query_stage2 }]
        query += [
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'container_type' : f"$answers.{self.f['plant_conteiner_type']}",
                    'racks' : f"$answers.{self.f['new_location_group']}.{self.f['new_location_racks']}",
                    'containers': f"$answers.{self.f['new_location_group']}.{self.f['new_location_containers']}",
                    }
            },
            {'$project':
                {'_id':1,
                    'product_code': "$product_code",
                    'containers': "$containers",                
                    'containers_on_rack' : { "$cond": 
                        [ 
                        {"$eq":["$container_type","baby_jar"]}, 
                        {"$multiply":["$racks", self.container_per_rack['baby_jar']]}, 
                        { "$cond": 
                            [ 
                            {"$eq":["$container_type","magenta_box"]}, 
                            {"$multiply":["$racks", self.container_per_rack['baby_jar']]}, 
                            { "$cond": 
                                [ 
                                {"$eq":["$container_type","clam_shell"]}, 
                                {"$multiply":["$racks", self.container_per_rack['clam_shell']]}, 
                                { "$cond": 
                                    [ 
                                    {"$eq":["$container_type","setis"]}, 
                                    {"$multiply":["$racks", self.container_per_rack['setis']]}, 
                                    0, 
                                 ]}, 
                             ]}, 
                         ]},
                     ]}

                }
            },
            {'$project':
                {'_id':1,
                'product_code': "$product_code",
                'total': {'$sum':['$containers','$containers_on_rack']}
                }
            },
            {'$group':
                {'_id':
                    { 'product_code': '$product_code',
                      },
                  'total': {'$sum': '$total'},
                  }
            },
            {'$project':
                {'_id': 0,
                'product_code': '$_id.product_code',
                'total': '$total',
                }
            },
            {'$sort': {'product_code': 1}}
            ]
        print('query', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}
        # print('query=',simplejson.dumps(query,indent=4))
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        return result

    def update_calc_fields(self, product_code, warehouse, lot_number, folio=None, location=None, map_type='model_2_field_id', **kwargs):
        '''
        stock = {
            'production':'Production',
            'move_in':'move_in',
            'move_out':'move_out',
            'scrapped':'scrapped',
            'cuarentin':'cuarentin',
            'sales':'sales',
            'adjustments':'adjustments',
            'actuals':'actuals',
        }
        '''
        query_dict = {
            'product_code':product_code,
            'warehouse':warehouse,
            'product_lot':lot_number,
        }
        if location:
            query_dict.update({'location':location,})
        print('----------------------------------------------------')
        print('product_code', product_code)
        print('warehouse', warehouse)
        print('lot_number', lot_number)
        print('location', location)
        stock = self.get_product_stock(product_code, warehouse=warehouse, lot_number=lot_number,location=location, **kwargs)
        print('stock111', stock)
        #production = self.stock_production( product_code=product_code, lot_number=lot_number)
        #scrap , cuarentine = self.stock_scrap( product_code=product_code, lot_number=lot_number, status='done')
        # if production:
        #     stock['scrap_perc'] = round(stock.get('scrapped',0)/stock.get('production',1),2)
        if stock['actuals'] <= 0:
            stock['status'] = 'done'
        else:
            stock['status'] = 'active'
        update_values = self.get_product_map(stock)
        if not folio:
            inv = self.get_invtory_record_by_product(self.FORM_INVENTORY_ID, product_code, lot_number, warehouse, location)
            if inv:
                folio = inv.get('folio')
        if not folio:
            return None
        query_dict = {'from_id':self.FORM_INVENTORY_ID, 'folio':folio}
        match_query = self.get_stock_query(query_dict)
       # get_match_query = get_product_map(, query_dict, map_type='model_2_field_id')
        update_res = self.cr.update_one(match_query, {'$set':update_values})
        if update_res.acknowledged:
            if folio:
                self.sync_catalog(folio)
        try:
            return update_res.raw_result
        except:
            return update_res
