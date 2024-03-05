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
        print('segundo aqui', folio_solicitud)
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)
        self.MEDIA = self.lkm.catalog_id('media')
        self.MEDIA_ID = self.MEDIA.get('id')
        self.MEDIA_OBJ_ID = self.MEDIA.get('obj_id')

        self.CATALOG_INVENTORY = self.lkm.catalog_id('lab_inventory')
        self.CATALOG_INVENTORY_ID = self.CATALOG_INVENTORY.get('id')
        self.CATALOG_INVENTORY_OBJ_ID = self.CATALOG_INVENTORY.get('obj_id')

        self.MEDIA_LOT = self.lkm.catalog_id('media_lot')
        self.MEDIA_LOT_ID = self.MEDIA_LOT.get('id')
        self.MEDIA_LOT_OBJ_ID = self.MEDIA_LOT.get('obj_id')


        self.FORM_INVENTORY_ID = self.lkm.form_id('lab_inventory','id')
        self.MOVE_NEW_PRODUCTION_ID = self.lkm.form_id('lab_move_new_production','id')
        self.PRODUCTION_FORM_ID = self.lkm.form_id('lab_production','id')
        self.STOCK_MOVE_FORM_ID = self.lkm.form_id('lab_inventory_move','id')
        self.STOCK_MANY_LOCATION_OUT = self.lkm.form_id('lab_inventory_out_pull','id')
        self.STOCK_MANY_LOCATION_2_ONE = self.lkm.form_id('lab_workorders_seleccion_de_planta','id')
        self.SCRAP_FORM_ID = self.lkm.form_id('lab_scrapping','id')
        self.ADJUIST_FORM_ID = self.lkm.form_id('lab_inventory_adjustment','id')


        # La relacion entre la forma de inventario y el catalogo utilizado para el inventario
        # por default simpre dejar los mismos nombres
        self.FORM_CATALOG_DIR = {
            self.FORM_INVENTORY_ID:self.CATALOG_INVENTORY_ID,
            }

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
            'plant_cut_day':'620a9ee0a449b98114f61d76',
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
            'reicpe_stage':'6205f73281bb36a6f1573358',
            'reicpe_start_size':'621fca56ee94313e8d8c5e2e',
            'set_production_date_out':'61f1fcf8c66d2990c8fcccc6',
            'new_cutweek':'65e241959ee4e466eadd2be3',
            'new_location_group':'63193fc51b3cefa880fefcc7',
            'new_location_racks':'c24000000000000000000001',
            'new_location_containers':'6319404d1b3cefa880fefcc8',
            'warehouse_dest':'65bdc71b3e183f49761a33b9',
            'warehouse_location':'65ac6fbc070b93e656bd7fbe',
            'warehouse_location_dest':'65c12749cfed7d3a0e1a341b',

            })

    def add_racks_and_containers(self, container_type, racks, containers):
        container_on_racks = racks * self.container_per_rack[container_type]
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

    def do_scrap(self):
        answers = self.answers
        stock = self.get_stock_info_from_catalog_inventory()
        print('stock', stock)
        scrap_qty = answers.get(self.f['inv_scrap_qty'], 0)
        print('scrap_qty', scrap_qty)
        cuarentin_qty = answers.get(self.f['inv_cuarentin_qty'], 0)
        # stock_record = self.get_inventory_record_by_folio(stock['folio'], self.FORM_INVENTORY_ID)
        # actuals = stock_record.get('answers',{}).get(self.f['product_lot_actuals'])
        if scrap_qty or cuarentin_qty:
            move_qty = scrap_qty + cuarentin_qty
            self.validate_move_qty(stock['product_code'], stock['lot_number'], stock['warehouse'], stock['warehouse_location'], move_qty, date_to=None)
            self.cache_set({
                        '_id': f"{stock['product_code']}_{stock['lot_number']}_{stock['warehouse']}_{stock['warehouse_location']}",
                        'scrapped':scrap_qty,
                        'cuarentin':cuarentin_qty,
                        'lot_number':stock['lot_number'],
                        'product_code':stock['product_code'],
                        'warehouse': stock['warehouse'],
                        'warehouse_location': stock['warehouse_location']
                        })
        # if move_qty > actuals:
        #     self.sync_catalog(folio_inventory)
        #     msg = f"You are trying to move {move_qty} units, and on the stock there is only {actuals}, please check you numbers"
        #     msg_error_app = {
        #             f"{self.f['inv_scrap_qty']}": {
        #                 "msg": [msg],
        #                 "label": "Please check your lot inventory",
        #                 "error":[]
      
        #             }
        #         }
        #     raise Exception( simplejson.dumps( msg_error_app ) )  
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=stock['folio'] )
        return res.get(stock['folio'],{}) 

    #### Temino heredacion para hace lote tipo string

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
        from_stage = production_recipe.get(self.f['reicpe_start_size'])
        to_stage = production_recipe.get(self.f['reicpe_stage'])
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

        if total_container_out_progress > 0:
            new_prod_line = self.get_production_move(total_container_out_progress, weighted_mult_rate, d_time_out)
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
        product_code, lot_number, warehouse, location = self.get_product_lot_location(answers)
        product_exist = self.product_stock_exists(product_code, location=location, warehouse=warehouse,  lot_number=lot_number)
        if product_exist:
            # res = self.update_calc_fields(product_code, lot_number, warehouse, location=location)
            res = self.update_stock(answers=product_exist.get('answers'), form_id=product_exist.get('form_id'), folios=product_exist.get('folio') )
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

    def create_proudction_lot_number(self, prod_date=None, group=None, cycle=None):
        if not prod_date:
            year = today.strftime('%Y')
            day_num = today.strftime('%j')
        else:
            year = prod_date.strftime('%Y')
            day_num = prod_date.strftime('%j')
        if not group:
            group = self.answers.get(self.f['production_working_group'])
        if not cycle:
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
                answers[self.f['status']] = 'active'
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
            
        if new_records_data:
            for x in new_records_data:
                print('2x=',simplejson.dumps(x, indent=4))
            result = self.lkf_api.post_forms_answers_list(new_records_data)
        return result

    def get_product_lot_location(self, answers=None):
        if not answers:
            answers = self.answers
        product_code = answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['product_code'])
        lot_number = answers.get(self.f['product_lot'])
        warehouse = answers.get(self.WAREHOUSE_LOCATION_OBJ_ID,{}).get(self.f['warehouse'])
        location = answers.get(self.WAREHOUSE_LOCATION_OBJ_ID,{}).get(self.f['warehouse_location'])
        return product_code, lot_number, warehouse, location

    def get_stock_info_from_catalog_inventory(self, answers=None, data={}, **kwargs):
        if not answers:
            answers = self.answers
        product_info = answers.get(self.CATALOG_INVENTORY_OBJ_ID, {})
        data['product_code'] = data.get('product_code',self.unlist(product_info.get(self.f['product_code'])))
        data['warehouse'] = data.get('warehouse',
            self.unlist(product_info.get(self.f['warehouse']))
            )
        data['warehouse_location'] = data.get('warehouse_location',
            self.unlist(product_info.get(self.f['warehouse_location']))
            )
        data['lot_number'] = data.get('lot_number',
            self.unlist(product_info.get(self.f['product_lot']))
            )
        data['actuals'] = data.get('actuals',
            self.unlist(product_info.get(self.f['product_lot_actuals']))
            )
        data['container'] = data.get('container',
            self.unlist(product_info.get(self.f['plant_conteiner_type']))
            )
        data['per_container'] = data.get('per_container',
            self.unlist(product_info.get(self.f['plant_per_container']))
            )
        data['cut_day'] = data.get('cut_day',
            self.unlist(product_info.get(self.f['production_cut_day']))
            )
        folio = self.unlist(product_info.get(self.f['cat_stock_folio']))
        if not folio:
            kwargs['require'] = kwargs.get('require',[])
            kwargs['require'].append('folio')
        if kwargs.get('require') or kwargs.get('get_record'):
            record = self.get_invtory_record_by_product(
                self.FORM_INVENTORY_ID,
                data['product_code'],  
                data['lot_number'] ,
                data['warehouse'],
                data['warehouse_location'])
            print('record', record)
            data['record'] = record
            for key in kwargs['require']:
                if record.get(key):
                    data[key] = record.get(key)
                else:
                    data[key] = self.search_4_key(record, key)
        return data
 
    def get_record_catalog_del(self):
        mango_query = {
            "selector":{"answers": {}},
            "limit":1000,
            "skip":0
            }
        product_code, lot_number, warehouse, location = self.get_product_lot_location()
        query = {
            self.f['product_code']:product_code,
            self.f['product_lot']:lot_number,
            self.f['warehouse']:warehouse,
            self.f['warehouse_location']:location,
        }
        mango_query['selector']['answers'].update(query)
        if False:
            #TODO gargabe collector
            mango_query['selector']['answers'].update({self.f['inventory_status']: "Done"})
        res = self.lkf_api.search_catalog( self.FORM_CATALOG_DIR[self.form_id], mango_query, jwt_settings_key='APIKEY_JWT_KEY')
        return res

    def inventory_adjustment(self):
        products = self.answers.get(self.f['grading_group'])
        warehouse = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
        location_id = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
        adjust_date = self.answers[self.f['grading_date']]
        comments = self.answers.get(self.f['inv_adjust_comments'],'') 
        patch_records = []
        metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
        kwargs = {"force_lote":True, "inc_folio":self.folio }
        properties = {
                "device_properties":{
                    "system": "Script",
                    "process": "Inventroy Adjustment", 
                    "accion": 'Inventroy Adjustment', 
                    "folio carga": self.folio, 
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
        # for plant in plants:
        #     product_code = plant[self.CATALOG_PRODUCT_OBJ_ID][self.f['product_code']]
        #     search_codes.append(product_code)


        # recipes = self.get_plant_recipe( search_codes, stage=[4, 'Ln72'] )
        # growth_weeks = 0
        latest_versions = versions = self.get_record_last_version(self.current_record)
        answers_version = latest_versions.get('answers',{})
        last_verions_products = {}
        if answers_version:
            version_products = answers_version.get(self.f['grading_group'])
            for ver_product in version_products:
                ver_product_code = ver_product[self.CATALOG_PRODUCT_RECIPE_OBJ_ID][self.f['product_code']]
                ver_lot_number = ver_product.get(self.f['product_lot'])
                if ver_lot_number:
                    last_verions_products[f'{ver_product_code}_{ver_lot_number}_{warehouse}_{location_id}'] = {
                        'product_code':ver_product_code,
                        'lot_number':ver_lot_number,
                        'warehouse':warehouse,
                        'location':location_id
                    }

        search_codes = {}
        for product in products:
            product_code =  product[self.CATALOG_PRODUCT_RECIPE_OBJ_ID][self.f['product_code']]
            stage =  product[self.CATALOG_PRODUCT_RECIPE_OBJ_ID][self.f['reicpe_stage']]
            stage = stage.upper()
            search_codes[stage] = search_codes.get(stage,[])
            search_codes[stage].append(product_code) if product_code not in search_codes[stage] else False
        recipes = {}
        for stage, product_codes in search_codes.items():
            recipes.update(self.get_product_recipe( product_codes, stage=stage ))
        growth_weeks = 0
        not_found = []
        for idx, product in enumerate(products):
            status = product[self.f['inv_adjust_grp_status']]
            cycle = product[self.f['plant_cycle']]
            group = product[self.f['plant_group']]
            year = product[self.f['plant_cut_year']]
            day = product[self.f['plant_cut_day']]

            contamin_code = product.get(self.f['plant_contamin_code'])
            prduction_date = datetime.strptime(f'{year}{day:03}','%Y%j')
            lot_number = self.create_proudction_lot_number(prduction_date, group, cycle)
            product[self.f['product_lot']] = lot_number
            adjust_qty = product.get(self.f['inv_adjust_grp_qty'])
            adjust_in = product.get(self.f['inv_adjust_grp_in'], 0)
            adjust_out = product.get(self.f['inv_adjust_grp_out'], 0)
            product_code = product[self.CATALOG_PRODUCT_RECIPE_OBJ_ID][self.f['product_code']]
            verify = 0
            if adjust_qty or adjust_qty ==0:
                verify +=1
                adjust_in = 0
                adjust_out = 0
            if adjust_in:
                verify +=1
            if adjust_out:
                verify +=1
            if verify > 1:
                msg = f"You can have only ONE input on product {product_code} lot number {lot_number}."
                msg +=  "Either the Actual Qty, the Adjusted In or the Adjusted Out."
                product[self.f['inv_adjust_grp_status']] = 'error'
                product[self.f['inv_adjust_grp_comments']] = msg
                continue
            if verify ==  0:
                msg = f"You must input an adjusted Qty on product {product_code}, lot number {lot_number}."
                product[self.f['inv_adjust_grp_status']] = 'error'
                product[self.f['inv_adjust_grp_comments']] = msg
                continue

            if last_verions_products.get(f'{product_code}_{lot_number}_{warehouse}_{location_id}'):
                last_verions_products.pop(f'{product_code}_{lot_number}_{warehouse}_{location_id}')
            exist = self.product_stock_exists(product_code=product_code, lot_number=lot_number, warehouse=warehouse, location=location_id)
            print('exists', exist)
            actuals = 0

            if exist:
                product_stock = self.get_product_stock(product_code, lot_number=lot_number, warehouse=warehouse, date_to=adjust_date, **{'nin_folio':self.folio})
                actuals = product_stock.get('actuals',0)

            if adjust_qty or adjust_qty == 0:
                cache_adjustment = adjust_qty - actuals
                if actuals < adjust_qty:
                    adjust_in = adjust_qty - actuals 
                elif actuals > adjust_qty:
                    adjust_out = adjust_qty - actuals
                else:
                    adjust_in  = 0
                    adjust_out = 0
            elif adjust_in:
                cache_adjustment = adjust_in
                adjust_out = None
                adjust_qty = None
            elif adjust_out:
                cache_adjustment = adjust_out * -1
                adjust_in = None
                adjust_qty = None

            product[self.f['inv_adjust_grp_qty']] = adjust_qty
            product[self.f['inv_adjust_grp_in']] = adjust_in
            product[self.f['inv_adjust_grp_out']] = abs(adjust_out)
            
            cache_values = {
                            '_id': f'{product_code}_{lot_number}_{warehouse}_{location_id}',
                            'adjustments': cache_adjustment,
                            'lot_number': lot_number,
                            'product_code':product_code,
                            'warehouse': warehouse,
                            'warehouse_location': location_id,
                            }
            if self.folio:
                cache_values.update({'kwargs': {'nin_folio':self.folio}})
            self.cache_set(cache_values)
            print('cache_values', cache_values)
            if exist:
                answers = {self.f['plant_contamin_code']:product.get(self.f['plant_contamin_code']) }
                response = self.update_stock(answers=answers, form_id=self.FORM_INVENTORY_ID, folios=exist['folio'])
                if not response:
                    comments += f'Error updating product {product_code} lot {lot_number}. '
                    product[self.f['inv_adjust_grp_status']] = 'error'
                else:
                    product[self.f['inv_adjust_grp_status']] = 'done'
                    product[self.f['inv_adjust_grp_comments']] = ""

            else:
                if recipes.get(product_code) and len(recipes[product_code]):
                    yearWeek = product.get(self.f['product_lot_created_week'])
                    answers = self.stock_inventory_model(product, recipes[product_code], cache_adjustment, cycle, group, year, day, prduction_date)
                    product[self.f['product_lot']] = lot_number
                    answers.update({
                        self.WAREHOUSE_LOCATION_OBJ_ID:{
                            self.f['warehouse']:warehouse,
                            self.f['warehouse_location']:location_id}
                            ,
                        self.f['plant_contamin_code']:contamin_code})
                    metadata['answers'] = answers
                    print('va a crear el resgistro...')
                    response_sistema = self.lkf_api.post_forms_answers(metadata)
                    try:
                        new_inv = self.get_record_by_id(response_sistema.get('id'))
                    except:
                        print('no encontro...')
                    status_code = response_sistema.get('status_code',404)
                    if status_code == 201:
                        product[self.f['inv_adjust_grp_status']] = 'done'
                        product[self.f['inv_adjust_grp_comments']] = "New Creation "
                    else:
                        error = response_sistema.get('json',{}).get('error', 'Unkown error')
                        product[self.f['inv_adjust_grp_status']] = 'error'
                        product[self.f['inv_adjust_grp_comments']] = f'Status Code: {status_code}, Error: {error}'
                else:
                        product[self.f['inv_adjust_grp_status']] = 'error'
                        product[self.f['inv_adjust_grp_comments']] = f'Recipe not found'

        if last_verions_products:
            for key, value in last_verions_products.items():
                exist = self.product_stock_exists(
                    product_code=value['product_code'], lot_number=value['lot_number'], warehouse=value['warehouse'], location=value['location'])
                print('si existe....', exist)
                if exist:
                    print('doble update o que....????')
                    response = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=exist['folio'])

        self.answers[self.f['inv_adjust_status']] = 'done'
        if not_found:
            comments += f'Codes not found: {not_found}.'

        self.answers[self.f['inv_adjust_comments']] = comments
        return True

    def stock_inventory_model(self, product, recipe, new_containers, cycle, group, year, day, production_date):
        res = {}
        stage = recipe.get('stage','S2')
        growth_weeks = recipe.get(f'{stage}_growth_weeks',0)
        res[self.PRODUCT_RECIPE_OBJ_ID ] = deepcopy(product.get(self.PRODUCT_RECIPE_OBJ_ID, {}))
        res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['product_name']] = [recipe['product_name'],]
        res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_growth_weeks']] = growth_weeks
        res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_start_size']] = recipe['start_size']
        res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['recipe_type']] = recipe['recipe_type']
        res[self.f['set_production_date']] = str(production_date.strftime('%Y-%m-%d'))
        res[self.f['plant_cut_year']] = int(production_date.strftime('%Y'))
        res[self.f['production_cut_week']] = int(production_date.strftime('%W'))
        res[self.f['production_cut_day']] = int(production_date.strftime('%j'))
        res[self.f['plant_group']] = group
        res[self.f['plant_cycle']] = cycle
        res[self.f['product_lot']] = self.create_proudction_lot_number(production_date, group, cycle)
        return res

    def merge_stock_records(self):
        form_id = self.FORM_INVENTORY_ID
        product_code, lot_number, warehouse, location = self.get_product_lot_location()
        res = self.get_invtory_record_by_product(form_id, product_code, lot_number, warehouse, location, **{'get_many':True})
        print('res', len(res))
        delete_records = []
        if len(res) >= 1:
            res.pop(0)
        for x in res:
            print('x',x.get('_id'))
            delete_records.append(x['_id'])
        if delete_records:
            print('aqui va a borrar *********************************************')
            #res = self.lkf_api.delete_records(delete_records)
            print('res', res)

    def move_location(self):
        product_info = self.answers.get(self.CATALOG_INVENTORY_OBJ_ID,{})
        # folio_inventory = product_info.get(self.f['cat_stock_folio'])
        # print('folio_inventory', folio_inventory)
        lot_number = product_info.get(self.f['product_lot'])
        product_code = product_info.get(self.f['product_code'])
        from_warehouse = product_info.get(self.f['warehouse'])
        from_location = product_info.get(self.f['warehouse_location'])
        # record_inventory_flow = self.get_inventory_record_by_folio(folio_inventory, form_id=self.FORM_INVENTORY_ID )
        record_inventory_flow = self.get_invtory_record_by_product(
            self.FORM_INVENTORY_ID,
            product_code,  
            lot_number,
            from_warehouse,
            from_location)
        print('record_inventory_flow', record_inventory_flow)

        if not record_inventory_flow:
            self.LKFException(f"folio: {record_inventory_flow} is not a valid inventory record any more, please check your stock")
        from_folio = record_inventory_flow['folio']
        inv_record = record_inventory_flow.get('answers')
        #gets the invetory as it was on that date...
        date_to = self.answers[self.f['grading_date']]
        # This are the actuals as they were on that date not including this move.
        inv_move_qty = self.answers.get(self.f['inv_move_qty'])
        print('containers to move.....',inv_move_qty)
        cache_from_location ={
            '_id': f'{product_code}_{lot_number}_{from_warehouse}_{from_location}',
            'move_out':inv_move_qty,
            'lot_number':lot_number,
            'product_code':product_code,
            'warehouse': from_warehouse,
            'warehouse_location': from_location
        }
        from_wl = f'{from_warehouse}__{from_location}'
        dest_group = self.answers.get(self.f['move_group'],[])
        self.validate_stock_move(from_wl, inv_move_qty, dest_group)
        self.validate_move_qty(product_code, lot_number, from_warehouse, from_location, inv_move_qty, date_to=date_to)
        dest_folio = []
        dest_folio_update = []
        for dest_set in dest_group:
            to_wh_info = dest_set.get(self.WAREHOUSE_LOCATION_DEST_OBJ_ID,{})
            qty_to_move = dest_set.get(self.f['move_group_qty'],0)
            to_warehouse = to_wh_info.get(self.f['warehouse_dest'])
            to_location = to_wh_info.get(self.f['warehouse_location_dest'])
            to_wl = f'{to_warehouse}__{to_location}'
            dest_warehouse_inventory = self.get_invtory_record_by_product(
                self.FORM_INVENTORY_ID, product_code, lot_number, to_warehouse, to_location  )
            print('dest_warehouse_inventory', dest_warehouse_inventory)
            self.cache_set({
                '_id': f'{product_code}_{lot_number}_{to_warehouse}_{to_location}',
                'move_in':qty_to_move,
                'lot_number':lot_number,
                'product_code':product_code,
                'warehouse': to_warehouse,
                'warehouse_location': to_location
                })

            if not dest_warehouse_inventory:
                #creates new record.
                new_inv_rec = deepcopy(inv_record)
                # stock = self.get_product_stock(product_code, warehouse=dest_warehouse, lot_number=product_lot, **{'keep_cache':True})
                # update_values = self.get_product_map(stock)
                new_inv_rec.update({
                    f"{self.WAREHOUSE_LOCATION_OBJ_ID}": {
                        self.f['warehouse']:to_warehouse,
                        self.f['warehouse_location']:to_location},
                    f"{self.f['product_lot_actuals']}": qty_to_move,
                    f"{self.f['product_lot_move_in']}": qty_to_move,
                    f"{self.f['product_lot_move_out']}": 0,
                    self.f['inventory_status']: 'active',
                })

                metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID) 
                metadata.update({
                    'properties': {
                        "device_properties":{
                            "system": "Script",
                            "process": 'Inventory Move',
                            "action": 'Create Inventory Record',
                            "from_folio": self.folio,
                            "archive": "lab_stock_utils.py"
                        }
                    }
                })
                #1 check if the hole lot is moving out ....
                # response, update_rec = update_origin_log(record_inventory_flow, inv_record, inv_move_qty, acctual_containers)
                # new_inv_rec.update(update_rec)
                metadata.update({'answers': new_inv_rec})
                response = self.lkf_api.post_forms_answers(metadata, jwt_settings_key='USER_JWT_KEY')
                if response.get('status_code') > 299 or not response.get('status_code'):
                    msg_error_app = response.get('json', 'Error al acomodar producto , favor de contactar al administrador')
                    self.LKFException( simplejson.dumps(msg_error_app) )
                x = simplejson.loads(response['data'])
                dest_folio.append(x.get('folio'))
            else:
                # Adding up to an existing lot
                # response, update_rec = update_origin_log(record_inventory_flow, inv_record, inv_move_qty, acctual_containers)
                print('TODO=', dest_warehouse_inventory)
                dest_folio_update.append(dest_warehouse_inventory.get('folio'))
                print('dest_folio', dest_warehouse_inventory.get('folio'))
                # dest_warehouse_inventory['answers'][self.f['product_lot_actuals']] += inv_move_qty
                # response = lkf_api.patch_record(dest_warehouse_inventory, jwt_settings_key='USER_JWT_KEY')
        if dest_folio_update:
            self.update_stock(folios=dest_folio_update)
            dest_folio += dest_folio_update
        self.cache_set(cache_from_location)
        self.update_stock(folios=from_folio)
        return dest_folio

    def move_multi_2_one_location(self):
        move_lines = self.answers[self.f['move_group']]

        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        product_code = self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['product_code'])
        lots_in = {}
        for moves in move_lines:
            info_plant = moves.get(self.CATALOG_INVENTORY_OBJ_ID, {})
            stock = {'product_code':product_code}
            stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=stock, **{'require':[self.f['reicpe_container']]})
            lot_number = stock.get('lot_number')
            warehouse = stock.get('warehouse')
            location = stock.get('warehouse_location')
            moves[self.f['move_dest_folio']] = stock['folio']
            set_location = f"{stock['warehouse']}__{stock['warehouse_location']}__{lot_number}"
            if set_location in move_locations:
                msg = "You are trying to move the same lot_number: {lot_number} twice from the same location. Please check"
                msg += f"warehouse: {stock['warehouse']} / Location: {stock['warehouse_location']}"
                msg_error_app = {
                    f"{self.f['warehouse_location']}": {
                        "msg": [msg],
                        "label": "Please check your selected location",
                        "error":[]
      
                    }
                }
                self.LKFException(msg_error_app)
            move_locations.append(set_location)
            if not stock.get('folio'):
                continue
            # Información que modifica el usuario
            racks = moves.get(self.f['new_location_racks'],0)
            containers = moves.get(self.f['new_location_containers'],0)
            container_type = stock.get(self.f['reicpe_container'])
            move_qty = self.add_racks_and_containers(container_type, racks, containers)
            moves[self.f['inv_move_qty']] = move_qty
          
            self.validate_move_qty(product_code, stock['lot_number'],  stock['warehouse'], stock['warehouse_location'], move_qty)
            lots_in[lot_number] = lots_in.get(lot_number,{'folio':None, 'move_qty':0})
            if stock.get('folio'):
                folios.append(stock['folio'])
                lots_in[lot_number]['folio'] = stock.get('folio')
                lots_in[lot_number]['move_qty'] += move_qty
            else:
                if lots_in.get(lot_number,{}).get('folio') and lots_in[lot_number]['folio']:
                    pass
                else:
                    lots_in[lot_number] = ""

            self.cache_set({
                        '_id': f"{product_code}_{lot_number}_{warehouse}_{location}",
                        'move_out':move_qty,
                        'product_code':product_code,
                        'product_lot':lot_number,
                        'warehouse': warehouse,
                        'warehouse_location': location
                        })
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        res ={}
        new_records_data = []
        warehouse_ans = self.swap_location_dest(self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID])
        print('self answers', self.answers)
        warehouse_dest = self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID].get(self.f['warehouse_dest'])
        location_dest = self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID].get(self.f['warehouse_location_dest'])
        for lot_number, vals in lots_in.items():
            new_lot = {}
            new_lot.update(warehouse_ans)
            new_lot[self.f['product_lot']] = lot_number
            new_lot.update(self.duplicate_lot_record(lot_number, folio=vals['folio']).get('answers',{}))
            print('warehouse_dest',warehouse_dest)
            print(f'----------------{product_code}_{lot_number}_{warehouse_dest}_{location_dest}--------------------')
            cache_data = {
                        '_id': f"{product_code}_{lot_number}_{warehouse_dest}_{location_dest}",
                        'move_in':vals['move_qty'],
                        'product_code':product_code,
                        'product_lot':lot_number,
                        'warehouse': warehouse_dest,
                        'warehouse_location': location_dest,
                        }
            if self.folio:
                cache_data.update({'kwargs': {'nin_folio':self.folio }})
            self.cache_set(cache_data)

            new_records_data.append(self.create_inventory_flow(answers=new_lot))

        folios_2_update = []
        for record in new_records_data:
            if record.get('new_record'):
                res_create = self.lkf_api.post_forms_answers_list(record['new_record'])
            else:
                print('ya existe2.....', record)
                folios_2_update.append(record.get('folio'))
        return res

    def duplicate_lot_record(self, lot_number, folio=None):
        if folio:
            match_query = {
                "deleted_at":{"$exists":False},
                "form_id": self.FORM_INVENTORY_ID,
                "folio": folio
                }
        else:
            match_query = {
                "deleted_at":{"$exists":False},
                "form_id": self.FORM_INVENTORY_ID,
                f"answers.{self.f['prouct_lot']}": lot_number
                }
        res = self.cr.find_one(match_query, {
            f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}":1, 
            f"answers.{self.MEDIA_LOT_OBJ_ID}":1, 
            f"answers.{self.f['product_lot_created_week']}":1, 
            f"answers.{self.f['production_cut_day']}":1, 
            f"answers.{self.f['production_cut_week']}":1, 
            f"answers.{self.f['plant_cycle']}":1, 
            f"answers.{self.f['plant_group']}":1, 
            f"answers.{self.f['plant_contamin_code']}":1, 
            })
        return res

    def move_out_multi_location(self):
        move_lines = self.answers[self.f['move_group']]

        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        print('ans', self.answers)
        product_code = self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['product_code'])
        print('product_code1', product_code)
        for moves in move_lines:
            print('move........', moves)
            info_plant = moves.get(self.CATALOG_INVENTORY_OBJ_ID, {})
            stock = {'product_code':product_code}
            stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=stock)
            print('product_code........',product_code)
            lot_number = stock.get('lot_number')
            warehouse = stock.get('warehouse')
            location = stock.get('warehouse_location')
            print('lot_number........',lot_number)
            print('warehouse........',warehouse)
            print('location........',location)
            moves[self.f['move_dest_folio']] = stock['folio']
            set_location = f"{stock['warehouse']}__{stock['warehouse_location']}__{lot_number}"
            if set_location in move_locations:
                msg = "You are trying to move the same lot_number: {lot_number} twice from the same location. Please check"
                msg += f"warehouse: {stock['warehouse']} / Location: {stock['warehouse_location']}"
                msg_error_app = {
                    f"{self.f['warehouse_location']}": {
                        "msg": [msg],
                        "label": "Please check your selected location",
                        "error":[]
      
                    }
                }
                self.LKFException(msg_error_app)
            move_locations.append(set_location)
            print('stock_info........',stock)
            if not stock.get('folio'):
                continue
            # Información que modifica el usuario
            move_qty = moves.get(self.f['inv_move_qty'], 0)
            print('move_qty=', move_qty)
            #record_inventory_flow = self.get_inventory_record_by_folio(folio=stock.get('folio'),form_id=self.FORM_INVENTORY_ID)
            self.validate_move_qty(product_code, stock['lot_number'],  stock['warehouse'], stock['warehouse_location'], move_qty)
            if stock.get('folio'):
                folios.append(stock['folio'])
        self.cache_set({
                    '_id': f"{product_code}_{stock['lot_number']}_{stock['warehouse']}_{stock['warehouse_location']}",
                    'move_out':move_qty,
                    'product_code':product_code,
                    'product_lot':stock['lot_number'],
                    'warehouse': stock['warehouse'],
                    'warehouse_location': stock['warehouse_location']
                    })
        print('fokios', folios)
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        print('res',res)

            # if new_actual_containers_on_hand <= 0:
            #     record_inventory_flow['answers'].update({
            #         '620ad6247a217dbcb888d175': 'done'
            #     })

            # record_inventory_flow.update({
            #     'properties': {
            #         "device_properties":{
            #             "system": "SCRIPT",
            #             "process":"Inventory Move - Out",
            #             "accion":'Update record Inventory Flow',
            #             "archive":"inventory_move_scrap.py"
            #         }
            #     }
            # })
            # print('record_inventory_flow',record_inventory_flow['answers'])
            # # Se actualiza el Inventory Flow que está seleccionado en el campo del current record
            # res_update_inventory = lkf_api.patch_record( record_inventory_flow, jwt_settings_key='USER_JWT_KEY' )
            # print('res_update_inventory =',res_update_inventory)
            # if res_update_inventory.get('status_code',0) > status_code:
            #     status_code = res_update_inventory['status_code']
        return res

    def get_plant_prodctivity(self, answers):
        group = answers.get(self.f['production_group'], [])
        total_hrs = 0
        total_containers = 0
        total_eaches = 0
        print('group', group)

        print('group', self.f['production_group'])
        for gset in group:
            product = gset.get(self.f['product_recipe'], {})

            eaches = gset.get(self.f['production_eaches_req'], 0)
            print('eaches', eaches)
            plt_container =  product.get(self.f['reicpe_per_container'],0)
            print('per_container', plt_container)
            if eaches:
                containers =  round(eaches/plt_container,0)
                gset[self.f['production_requier_containers']] = containers
                total_containers += containers
                total_eaches += eaches
            print('production_requier_containers', gset[self.f['production_requier_containers']])
            plant_per_hr = product.get(self.f['reicpe_productiviy'],[])
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
        res[self.PRODUCT_RECIPE_OBJ_ID ]['6205f73281bb36a6f1573357'] = 8
        soil_type = self.unlist(self.answers.get(self.PRODUCT_RECIPE_OBJ_ID,{}).get(self.f['reicpe_soil_type'],""))
        # res[self.PRODUCT_RECIPE_OBJ_ID ][self.f['reicpe_soil_type']] = soil_type
        res[self.TEAM_OBJ_ID] = self.answers.get( self.TEAM_OBJ_ID,{})
        res[self.MEDIA_LOT_OBJ_ID] = self.answers.get(self.MEDIA_LOT_OBJ_ID,{})

        res[self.f['set_production_date']] = str(production_date.strftime('%Y-%m-%d'))
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

    def get_product_recipe(self, all_codes, stage=[2,3,4], recipe_type='Main'):
        if type(all_codes) == str and all_codes:
            all_codes = [all_codes.upper(),]
        recipe = {}
        recipe_s2 = []
        recipe_s3 = []
        recipe_s4 = []
        stage = [2,] if stage == 'S2' else stage
        stage = [3,] if stage == 'S3' else stage
        stage = [4,] if stage == 'S4' else stage
        print('get_product_recipe=')
        print('stage=',stage)
        print('all_codes=',all_codes)
        if 2 in stage:
            mango_query = self.plant_recipe_query(all_codes, "S2", "S2", recipe_type)
            recipe_s2 = self.lkf_api.search_catalog(self.CATALOG_PRODUCT_RECIPE_ID, mango_query)
        if 3 in stage:
            mango_query = self.plant_recipe_query(all_codes, "S2", "S3", recipe_type)
            print('mango_query', mango_query)
            recipe_s3 = self.lkf_api.search_catalog(self.CATALOG_PRODUCT_RECIPE_ID, mango_query)
        if 4 in stage:
            if 'Ln72' in stage:
                mango_query = self.plant_recipe_query(all_codes, "Ln72", "S4", recipe_type)
            else:
                mango_query = self.plant_recipe_query(all_codes, "S4", "S3", recipe_type)
            recipe_s4 = self.lkf_api.search_catalog(self.CATALOG_PRODUCT_RECIPE_ID, mango_query, jwt_settings_key='APIKEY_JWT_KEY')
        if recipe_s2 and not recipe:
            for this_recipe in recipe_s2:
                plant_code = this_recipe.get(self.f['product_code'])
                if not recipe.get(plant_code):
                    recipe[plant_code] = {}
                recipe[plant_code].update({
                    'S2_growth_weeks':this_recipe.get(self.f['reicpe_growth_weeks']),
                    'cut_productivity':this_recipe.get(self.f['reicpe_productiviy']),
                    'media_tray':this_recipe.get(self.f['reicpe_container']),
                    'per_container':this_recipe.get(self.f['reicpe_per_container']),
                    'S2_mult_rate':this_recipe.get(self.f['reicpe_mult_rate']),
                    'S2_overage':this_recipe.get(self.f['reicpe_overage']),
                    'plant_code':this_recipe.get(self.f['product_code'],),
                    'product_code':this_recipe.get(self.f['product_code'],),
                    'plant_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'product_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'start_week' : this_recipe.get(self.f['reicpe_start_week']),
                    'end_week' : this_recipe.get(self.f['reicpe_end_week']),
                    'start_size': this_recipe.get(self.f['reicpe_start_size']),
                    'stage': this_recipe.get(self.f['reicpe_stage']),
                    'recipe_type': this_recipe.get(self.f['recipe_type']),
                    })
        if recipe_s3  and not recipe:
            for this_recipe in recipe_s3:
                plant_code = this_recipe.get(self.f['product_code'])
                if not recipe.get(plant_code):
                    recipe[plant_code] = {}
                recipe[plant_code].update(
                    {'S3_growth_weeks':this_recipe.get(self.f['reicpe_growth_weeks']),
                    'cut_productivity':this_recipe.get(self.f['reicpe_productiviy']),
                    'media_tray':this_recipe.get(self.f['reicpe_container']),
                    'per_container':this_recipe.get(self.f['reicpe_per_container']),
                    'plant_code':this_recipe.get(self.f['product_code']),
                    'S3_mult_rate':this_recipe.get(self.f['reicpe_mult_rate']),
                    'S3_overage':this_recipe.get(self.f['reicpe_overage']),
                    'plant_code':this_recipe.get(self.f['product_code'],),
                    'plant_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'product_code':this_recipe.get(self.f['product_code'],),
                    'product_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'start_week' : this_recipe.get(self.f['reicpe_start_week']),
                    'end_week' : this_recipe.get(self.f['reicpe_end_week']),
                    'start_size': this_recipe.get(self.f['reicpe_start_size']),
                    'stage': this_recipe.get(self.f['reicpe_stage']),
                    'recipe_type': this_recipe.get(self.f['recipe_type']),
                    }
                    )
        if recipe_s4  and not recipe:
            for this_recipe in recipe_s4:
                plant_code = this_recipe.get(self.f['product_code'])
                if not recipe.get(plant_code):
                    recipe[plant_code] = []
                recipe[plant_code].append(
                    {'S4_growth_weeks':this_recipe.get(self.f['reicpe_growth_weeks']),
                    'media_tray':this_recipe.get(self.f['reicpe_container']),
                    'cut_productivity':this_recipe.get(self.f['reicpe_productiviy']),
                    'per_container':this_recipe.get(self.f['reicpe_per_container']),
                    'S4_mult_rate':this_recipe.get(self.f['reicpe_mult_rate']),
                    'S4_overage_rate':this_recipe.get(self.f['reicpe_overage']),
                    'S4_overage': this_recipe.get(self.f['reicpe_overage']),
                    'plant_code':this_recipe.get(self.f['product_code'],),
                    'plant_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'product_code':this_recipe.get(self.f['product_code'],),
                    'product_name':this_recipe.get(self.f['product_name'],['',])[0],
                    'start_week' : this_recipe.get(self.f['reicpe_start_week']),
                    'end_week' : this_recipe.get(self.f['reicpe_end_week']),
                    'start_size': this_recipe.get(self.f['reicpe_start_size']),
                    'stage': this_recipe.get(self.f['reicpe_stage']),
                    'soil_type': this_recipe.get(self.f['reicpe_soil_type']),
                    'recipe_type': this_recipe.get(self.f['recipe_type']),
                    }
                    )
        if not recipe:
            return {}
        return recipe
  
    #### Se heredaron funciones para hacer lote tipo string

    def get_invtory_record_by_product(self, form_id, product_code, lot_number, warehouse, location, **kwargs):
        #use to be get_record_greenhouse_inventory
        get_many = kwargs.get('get_many')
        print('form_id', form_id)
        print('product_code', product_code)
        print('lot_number', lot_number)
        print('warehouse', warehouse)
        print('location', location)
        query_warehouse_inventory = {
            'form_id': form_id,
            'deleted_at': {'$exists': False},
            f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}": product_code,
            f"answers.{self.f['product_lot']}": lot_number,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}": warehouse,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}": location,
        }
        if get_many:
            records = self.cr.find(query_warehouse_inventory, 
                {'folio': 1, 'answers': 1, 'form_id': 1, 'user_id': 1,'created_at':1}).sort('created_at')
            record = [x for x in records]
        else:
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
    # def stock_adjustments(self, product_code=None, location=None, warehouse=None, lot_number=None, date_from=None, date_to=None, **kwargs):
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

    def stock_adjustments_moves(self, product_code=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.ADJUIST_FORM_ID,
            f"answers.{self.f['inv_adjust_status']}":{"$ne":"cancel"}
            }
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if warehouse:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})   
        if location:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})      
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
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID }.{self.f['product_code']}":product_code})
        if lot_number:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.f['product_lot']}":lot_number})
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
        # print('query=', simplejson.dumps(query, indent=3))
        res = self.cr.aggregate(query)
        result = 0
        for r in res:
            result = r.get('total', 0)        
        return result  
  
    def stock_moves(self, move_type, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, status='done', **kwargs):
        unwind =None
        if move_type not in ('in','out'):
            raise('Move type only accepts values "in" or "out" ')
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MOVE_FORM_ID  
            }
        unwind_query = {}
        # print('move type.................', move_type)
        # print('warehouse', warehouse)
        # print('location', location)
        # print('product_code', product_code)
        # print('lot_number', lot_number)
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))

        unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
        if move_type =='out':
            if warehouse:
                unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
            if location:
                unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})

        if move_type =='in':
            if warehouse:
                # warehouse = warehouse.lower().replace(' ', '_')
                unwind_query.update({f"answers.{self.f['move_group']}.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_dest']}":warehouse})    
            if location:
                unwind_query.update({f"answers.{self.f['move_group']}.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_location_dest']}":location})   
       
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['move_group']}.{self.f['move_group_qty']}",
                    }
            }
        

        query= [{'$match': match_query }]
        if unwind:
            query.append(unwind)
            query.append({'$match': unwind_query })
        query.append(project)
        query += [
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
        # if move_type == 'in':
        #      print('query=', simplejson.dumps(query, indent=3))
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
            if move_type == 'out':
                result += self.stock_many_locations_out(
                    product_code=product_code,
                    lot_number=lot_number,
                    warehouse=warehouse,
                    location=location,
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                    **kwargs
                )
                result += self.stock_many_locations_2_one(
                    product_code=product_code,
                    lot_number=lot_number,
                    warehouse=warehouse,
                    location=location,
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                    **kwargs
                )
            if move_type == 'in':
                result += self.stock_in_dest_location_form_many(
                    product_code=product_code,
                    lot_number=lot_number,
                    warehouse=warehouse,
                    location=location,
                    date_from=date_from,
                    date_to=date_to,
                    status=status,
                    **kwargs
                )
        return result  

    def stock_many_locations_out(self, product_code=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        unwind =None
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MANY_LOCATION_OUT,
            }
        unwind_query = {}
        unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['move_group']}.{self.f['inv_move_qty']}",
                    }
            }
        query= [{'$match': match_query }]
        query.append(unwind)
        query.append({'$match': unwind_query })
        query.append(project)
        query += [
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
        # print('query out=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        print('result PULL OUT', result)
        return result  

    def stock_many_locations_2_one(self, product_code=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        unwind =None
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MANY_LOCATION_2_ONE,
            }
        unwind_query = {}
        unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['move_group']}.{self.f['inv_move_qty']}",
                    }
            }
        query= [{'$match': match_query }]
        query.append(unwind)
        query.append({'$match': unwind_query })
        query.append(project)
        query += [
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
        #print('query=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        return result  

    def stock_in_dest_location_form_many(self, product_code=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        unwind =None
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.STOCK_MANY_LOCATION_2_ONE,
            }
        unwind_query = {}
        unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if warehouse:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_dest']}":warehouse})      
        if location:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_location_dest']}":location})
        if lot_number:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['move_group']}.{self.f['inv_move_qty']}",
                    }
            }
        query= [{'$match': match_query }]
        query.append(unwind)
        query.append({'$match': unwind_query })
        query.append(project)
        query += [
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
        # print('query=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        # print('SELECCION DE PLANTA', result)
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

    def stock_production_out(self, product_code=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, **kwargs):
        unwind =None
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.PRODUCTION_FORM_ID,
            }
        if product_code:
            match_query.update({f"answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            unwind_query.update({f"answers.{self.WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            unwind_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})
        unwind_query = {}
        unwind = {'$unwind': '$answers.{}'.format(self.f['production_group'])}
        unwind_query.update({f"answers.{self.f['production_group']}.{self.f['production_status']}":'posted'})
        if date_from or date_to:
            unwind_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['set_production_date']))
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.CATALOG_PRODUCT_RECIPE_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['production_group']}.{self.f['production_containers_in']}",
                    }
            }
        query= [{'$match': match_query }]
        query.append(unwind)
        query.append({'$match': unwind_query })
        query.append(project)
        query += [
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
        # print('query out=', simplejson.dumps(query, indent=4))
        res = self.cr.aggregate(query)
        result = {}
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        print('result PRODUCTION OUT', result)
        return result  

    ### STOCK OUT'S

    def stock_scrap(self, product_code=None, warehouse=None, location=None, lot_number=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            # "form_id": {"$in":[self.SCRAP_FORM_ID, self.GRADING_FORM_ID]}
            "form_id": self.SCRAP_FORM_ID
            }
        if product_code:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            match_query.update({f"answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location}) 
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
            return 0, 0

    #### Temino heredacion para hace lote tipo string

    def stock_production(self, date_from=None, date_to=None, product_code=None, warehouse=None, location=None, lot_number=None,  status='done', **kwargs):
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
        # print('query', simplejson.dumps(query, indent=4))
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

    def swap_location_dest(self, from_location):
        res = {self.WAREHOUSE_LOCATION_OBJ_ID:{}}
        res[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']] = from_location[self.f['warehouse_dest']]
        res[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']] = from_location[self.f['warehouse_location_dest']]
        return res

    def swap_location(self, from_location):
        res = {self.WAREHOUSE_LOCATION_DEST_OBJ_ID:{}}
        res[self.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_dest']] = from_location[self.f['warehouse']]
        res[self.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_location_dest']] = from_location[self.f['warehouse_location']]
        return res

    def update_calc_fields(self, product_code, lot_number, warehouse, location, folio=None, map_type='model_2_field_id', **kwargs):
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
            'location':location,
        }
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
                print('...folio', folio)
                self.sync_catalog(folio)
        try:
            return update_res.raw_result
        except:
            return update_res

    def update_stock(self, answers={}, form_id=None, folios="" ):
        if not answers:
            answers={"udpate":True}
        if not form_id:
            form_id = self.FORM_INVENTORY_ID
        if type(folios) in [str, ]:
            folios = [folios,]
        return self.lkf_api.patch_multi_record( answers=answers, form_id=form_id, folios=folios, threading=True )

    def validate_stock_move(self, from_wl, qty, dest_group):
        qty_to_move = 0
        for dest_set in dest_group:
            to_wh_info = dest_set.get(self.WAREHOUSE_LOCATION_DEST_OBJ_ID,{})
            qty_to_move += dest_set.get(self.f['move_group_qty'],0)
            to_warehouse = to_wh_info.get(self.f['warehouse_dest'])
            to_location = to_wh_info.get(self.f['warehouse_location_dest'])
            to_wl = f'{to_warehouse}__{to_location}'
            if from_wl == to_wl:
                msg = "You need to make the move to a new destination. "
                msg += "Your current from location is: {} and you destination location is:{}".format(
                    from_wl.replace('__', ' '), 
                    to_wl.replace('__', ' '))
                msg_error_app = {
                        f"{self.f['warehouse_location_dest']}": {
                            "msg": [msg],
                            "label": "Please check your destinations location",
                            "error":[]
          
                        }
                    }
                self.LKFException( simplejson.dumps( msg_error_app ) )

        if qty != qty_to_move:
            msg = "Your move out quantity and alocation must be the same "
            msg += f"Your are trying to move out: {qty} products and alocating on the new destination:{qty_to_move}"
            msg_error_app = {
                    f"{self.f['warehouse_location_dest']}": {
                        "msg": [msg],
                        "label": "Please check your destinations location",
                        "error":[]
      
                    }
                }
            self.LKFException( simplejson.dumps( msg_error_app ) )            
        return True

    def validate_move_qty(self, product_code, lot_number, warehouse, location, move_qty, date_to=None):
        print('product_code=',product_code)
        print('date_to=',date_to)
        print('folio=',self.folio)
        inv = self.get_product_stock(product_code, warehouse=warehouse, location=location, lot_number=lot_number, 
            date_to=date_to, **{"nin_folio":self.folio})

        acctual_containers = inv.get('actuals')
        if acctual_containers == 0:
            msg = f"This lot {lot_number} has 0 containers left, if this is NOT the case first do a inventory adjustment"
            msg_error_app = {
                    f"{self.f['product_lot_actuals']}": {
                        "msg": [msg],
                        "label": "Please check your lot inventory",
                        "error":[]
      
                    }
                }
            #TODO set inventory as done
            self.LKFException( simplejson.dumps( msg_error_app ) )   

        if move_qty > acctual_containers:
        # if False:
            #trying to move more containeres that there are...
            cont_diff = move_qty - acctual_containers
            msg = f"There actually only {acctual_containers} containers and you are trying to move {move_qty} containers."
            msg += f"Check this out...! Your are trying to move {cont_diff} more containers than they are. "
            msg += f"If this is the case, please frist make an inventory adjustment of {cont_diff} "
            msg += f"On warehouse {warehouse} at location {location} and lot number {lot_number}"
            msg_error_app = {
                    f"{self.f['inv_move_qty']}": {
                        "msg": [msg],
                        "label": "Please check your Flats to move",
                        "error":[]
      
                    }
                }
            self.LKFException( simplejson.dumps( msg_error_app ) )
        return True