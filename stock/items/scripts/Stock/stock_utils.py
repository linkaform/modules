# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.stock.stock_utils import Stock

today = date.today()
year_week = int(today.strftime('%Y%W'))



class Stock(Stock):

    # _inherit = 'employee'

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)


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


        self.CONTACTO_CENTER_FORM_ID = 111563
        self.f.update({
            'parts_group':'62c5da67f850f35cc2483346',
            })
        self.answer_label = self._labels()

    #### Se heredaron funciones para hacer lote tipo string




    #### Temino heredacion para hace lote tipo string


    # def get_stock_info_from_catalog_wl(self, answers={}, data={}):
    #     if not answers:
    #         answers = self.answers
    #     res = {}
    #     wh_info = answers.get(self.WAREHOUSE_LOCATION_OBJ_ID, {})
    #     if not wh_info:
    #         wh_info = self.get_stock_info_from_catalog_wl(answers=self.answers, data=data)
    #     res['warehouse'] = data.get('warehouse',wh_info.get(self.f['warehouse']))
    #     res['warehouse_location'] = data.get('warehouse_location', wh_info.get(self.f['warehouse_location']))
    #     return res

    # def get_stock_info_from_catalog_wld(self, answers={}, data={}):
    #     if not answers:
    #         answers = self.answers
    #     res = {}
    #     print('answers', answers)
    #     wh_info = answers.get(self.WAREHOUSE_LOCATION_DEST_OBJ_ID, {})
    #     if not wh_info:
    #         wh_info = self.get_stock_info_from_catalog_wld(answers=self.answers, data=data)
    #     res['warehouse_dest'] = data.get('warehouse_dest',wh_info.get(self.f['warehouse_dest']))
    #     res['warehouse_location_dest'] = data.get('warehouse_location_dest',wh_info.get(self.f['warehouse_location_dest']))
    #     return res



    def move_location(self):
        product_info = self.answers.get(self.STOCK_INVENTORY_OBJ_ID,{})
        # folio_inventory = product_info.get(self.f['cat_stock_folio'])
        # print('folio_inventory', folio_inventory)
        lot_number = product_info.get(self.f['product_lot'])
        product_code = product_info.get(self.f['product_code'])
        sku = product_info.get(self.f['sku'])
        from_warehouse = product_info.get(self.f['warehouse'])
        from_location = product_info.get(self.f['warehouse_location'])
        # record_inventory_flow = self.get_inventory_record_by_folio(folio_inventory, form_id=self.FORM_INVENTORY_ID )
        record_inventory_flow = self.get_invtory_record_by_product(
            self.FORM_INVENTORY_ID,
            product_code,  
            sku,  
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
            '_id': f'{product_code}_{sku}_{lot_number}_{from_warehouse}_{from_location}',
            'move_out':inv_move_qty,
            'lot_number':lot_number,
            'product_code':product_code,
            'sku':sku,
            'warehouse': from_warehouse,
            'warehouse_location': from_location,
            'record_id': self.record_id
        }
        from_wl = f'{from_warehouse}__{from_location}'
        dest_group = self.answers.get(self.f['move_group'],[])
        self.validate_stock_move(from_wl, inv_move_qty, dest_group)
        self.validate_move_qty(product_code, sku, lot_number, from_warehouse, from_location, inv_move_qty, date_to=date_to)
        dest_folio = []
        dest_folio_update = []
        for dest_set in dest_group:
            to_wh_info = dest_set.get(self.WAREHOUSE_LOCATION_DEST_OBJ_ID,{})
            qty_to_move = dest_set.get(self.f['move_group_qty'],0)
            to_warehouse = to_wh_info.get(self.f['warehouse_dest'])
            to_location = to_wh_info.get(self.f['warehouse_location_dest'])
            to_wl = f'{to_warehouse}__{to_location}'
            dest_warehouse_inventory = self.get_invtory_record_by_product(
                self.FORM_INVENTORY_ID, product_code, sku, lot_number, to_warehouse, to_location  )
            self.cache_set({
                '_id': f'{product_code}_{sku}_{lot_number}_{to_warehouse}_{to_location}',
                'move_in':qty_to_move,
                'lot_number':lot_number,
                'sku':sku,
                'product_code':product_code,
                'warehouse': to_warehouse,
                'warehouse_location': to_location,
                'record_id': self.record_id
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
                            "archive": "stock_utils.py"
                        }
                    }
                })
                #1 check if the hole lot is moving out ....
                # response, update_rec = update_origin_log(record_inventory_flow, inv_record, inv_move_qty, acctual_containers)
                # new_inv_rec.update(update_rec)
                metadata.update({'answers': new_inv_rec})
                print('new_inv_rec=',new_inv_rec)
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


        #este update stock revisarlo y se me hace 
        if dest_folio_update:
            self.update_stock(folios=dest_folio_update)
            dest_folio += dest_folio_update
        self.cache_set(cache_from_location)
        self.update_stock(folios=from_folio)
        return dest_folio

    # def move_multi_2_one_location(self):
    #     move_lines = self.answers[self.f['move_group']]

    #     # Información original del Inventory Flow
    #     status_code = 0
    #     move_locations = []
    #     folios = []
    #     product_code = self.answers.get(self.SKU_OBJ_ID,{}).get(self.f['product_code'])
    #     lots_in = {}
    #     move_data = self.get_stock_info_form_answer(answers=self.answers)
    #     warehouse_ans = self.swap_location_dest(self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID])
    #     print('warehouse_ans', warehouse_ans)
    #     warehouse_dest = move_data['warehouse_dest']
    #     location_dest = move_data['warehouse_location_dest']
    #     new_records_data = []
    #     for moves in move_lines:
    #         info_plant = moves.get(self.STOCK_INVENTORY_OBJ_ID, {})
    #         stock = {}
    #         if product_code:
    #             stock = {'product_code':product_code}
    #         stock.update(move_data)
    #         stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=stock, **{'require':[self.f['reicpe_container']]})
    #         ###################################
    #         product_code = stock.get('product_code')
    #         sku = stock.get('sku')
    #         lot_number = stock.get('lot_number')
    #         warehouse = stock.get('warehouse')
    #         location = stock.get('warehouse_location')
    #         sku_lot = f"{sku}_{lot_number}"
    #         ###################################
    #         moves[self.f['move_dest_folio']] = stock['folio']
    #         set_location = f"{stock['warehouse']}__{stock['warehouse_location']}__{lot_number}"
    #         if set_location in move_locations:
    #             msg = "You are trying to move the same lot_number: {lot_number} twice from the same location. Please check"
    #             msg += f"warehouse: {stock['warehouse']} / Location: {stock['warehouse_location']}"
    #             msg_error_app = {
    #                 f"{self.f['warehouse_location']}": {
    #                     "msg": [msg],
    #                     "label": "Please check your selected location",
    #                     "error":[]
      
    #                 }
    #             }
    #             self.LKFException(msg_error_app)
    #         move_locations.append(set_location)
    #         if not stock.get('folio'):
    #             continue
    #         # Información que modifica el usuario
    #         racks = moves.get(self.f['new_location_racks'],0)
    #         containers = moves.get(self.f['new_location_containers'],0)
    #         container_type = stock.get(self.f['reicpe_container'])
    #         move_qty = self.add_racks_and_containers(container_type, racks, containers)
    #         moves[self.f['inv_move_qty']] = move_qty
    #         self.validate_move_qty(product_code, sku, lot_number,  warehouse, location, move_qty)
    #         lots_in[sku_lot] = lots_in.get(sku_lot,{'folio':None, 'move_qty':0})
    #         if stock.get('folio'):
    #             folio = stock['folio']
    #         else:
    #             self.LKFException(f'No folio found for {product_code} / {sku} / {lot_number}')

    #         self.cache_set({
    #                     '_id': f"{product_code}_{sku}_{lot_number}_{warehouse}_{location}",
    #                     'move_out':move_qty,
    #                     'product_code':product_code,
    #                     'product_lot':lot_number,
    #                     'warehouse': warehouse,
    #                     'warehouse_location': location
    #                     })

    #         ########### Make the new Stock Record
    #         new_lot = {}
    #         new_lot[self.f['product_lot']] = lot_number
    #         new_lot.update(self.duplicate_lot_record(lot_number, folio=folio).get('answers',{}))
    #         new_lot.update(warehouse_ans)
    #         cache_data = {
    #                     '_id': f"{product_code}_{sku}_{lot_number}_{warehouse_dest}_{location_dest}",
    #                     'move_in':move_qty,
    #                     'product_code':product_code,
    #                     'sku':sku,
    #                     'product_lot':lot_number,
    #                     'warehouse': warehouse_dest,
    #                     'warehouse_location': location_dest,
    #                     }
    #         if self.folio:
    #             cache_data.update({'kwargs': {'nin_folio':self.folio }})
    #         self.cache_set(cache_data)
    #         new_records_data.append(self.create_inventory_flow(answers=new_lot))


    #     res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
    #     res ={}
        
    #     print('new_records_data',new_records_data)
    #     folios_2_update = []
    #     for record in new_records_data:
    #         if record.get('new_record'):
    #             print('es uno pro uno???? a provechar el threading....')
    #             res_create = self.lkf_api.post_forms_answers_list(record['new_record'])
    #         else:
    #             print('ya existe.....', record)
    #             folios_2_update.append(record.get('folio'))
    #     return res


    def move_in(self):
        answers = self._labels()
        print('-----------------------------answers', self.answer_label)
        warehouse = self.answer_label['warehouse']
        location = self.answer_label['warehouse_location']
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        move_lines = self.answer_label['move_group']
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        # lots_in = {}
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        print('data_from', data_from)
        new_records_data = []
        skus = self.get_group_skus(move_lines)
        metadata = self.lkf_api.get_metadata(self.FORM_INVENTORY_ID)
        for idx, moves in enumerate(move_lines):
            move_line = self.answers[self.f['move_group']][idx]
            print('moves', moves)
            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            exists = self.product_stock_exists(
                product_code=moves['product_code'], 
                sku=moves['sku'], 
                lot_number=moves['product_lot'], 
                warehouse=warehouse_to, 
                location=location_to)
            print('exists', exists)
            cache_data = {
                        '_id': f"{moves['product_code']}_{moves['sku']}_{moves['product_lot']}_{warehouse_to}_{location_to}",
                        'move_in': moves['move_group_qty'],
                        'product_lot': moves['product_lot'],
                        'product_code':moves['product_code'],
                        'sku':moves['sku'],
                        'warehouse': warehouse_to,
                        'warehouse_location': location_to
                        }
            if exists:
                self.cache_set(cache_data)
                response = self.update_stock(answers=exists.get('answers',{}), form_id=self.FORM_INVENTORY_ID, folios=exists['folio'])
                if not response:
                    comments += f"Error updating product {moves['product_lot']} lot {moves['product_lot']}. "
                    move_line[self.f['inv_adjust_grp_status']] = 'error'
                else:
                    move_line[self.f['move_dest_folio']] = exists['folio']
                    move_line[self.f['inv_adjust_grp_status']] = 'done'
                    move_line[self.f['inv_adjust_grp_comments']] = ""
                    print('moves', move_line)

            else:
                print('moves', moves)
                answers = self.stock_inventory_model(moves, skus[moves['product_code']], labels=True)
                answers.update({
                    self.WAREHOUSE_LOCATION_OBJ_ID:{
                        self.f['warehouse']:warehouse_to,
                        self.f['warehouse_location']:location_to},
                    self.f['product_lot']:moves['product_lot']
                        },
                    )
                metadata['answers'] = answers
               
                print('cache_data',cache_data)
                self.cache_set(cache_data)
                create_resp = self.lkf_api.post_forms_answers(metadata)
                print('response_sistema',create_resp)
                try:
                    new_inv = self.get_record_by_id(create_resp.get('id'))
                except:
                    print('no encontro...')
                status_code = create_resp.get('status_code',404)
                if status_code == 201:
                    folio = create_resp.get('json',{}).get('folio','')
                    move_line[self.f['inv_adjust_grp_status']] = 'done'
                    move_line[self.f['move_dest_folio']] = folio
                else:
                    error = create_resp.get('json',{}).get('error', 'Unkown error')
                    move_line[self.f['inv_adjust_grp_status']] = 'error'
                    move_line[self.f['inv_adjust_grp_comments']] = f'Status Code: {status_code}, Error: {error}'
        return True

    def move_one_many_one(self):
        move_lines = self.answers[self.f['move_group']]
        print('-----------------------------answers', move_lines)
        warehouse = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse']]
        location = self.answers[self.WAREHOUSE_LOCATION_OBJ_ID][self.f['warehouse_location']]
        warehouse_to = self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_dest']]
        location_to = self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID][self.f['warehouse_location_dest']]
        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        # lots_in = {}
        data_from = {'warehouse':warehouse, 'warehouse_location':location}
        new_records_data = []
        for moves in move_lines:
            info_product = moves.get(self.STOCK_INVENTORY_OBJ_ID, {})
            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            # lot_number = info_product.get(self.f['lot_number'])
            stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=data_from , **{'get_record':True})
            ###################################
            product_code = stock.get('product_code')
            sku = stock.get('sku')
            lot_number = stock.get('lot_number')
            warehouse = stock.get('warehouse')
            location = stock.get('warehouse_location')
            folios.append(stock['folio'])
            ###################################

            moves[self.f['move_dest_folio']] = stock['folio']
            moves[self.f['inv_adjust_grp_status']] = 'done'
            
            set_location = f"{product_code}_{sku}_{lot_number}_{warehouse_to}_{location_to}"
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
                msg = "Stock record not found Please check availability for:"
                msg += f"Product Code: {product_code} / SKU: {sku} / Lot Number: {lot_number}"
                msg_error_app = {
                    f"{self.f['warehouse_location']}": {
                        "msg": [msg],
                        "label": "Please check your availability to this location",
                        "error":[]
      
                    }
                }
                self.LKFException(msg_error_app)
            # Información que modifica el usuario
            move_qty = moves.get(self.f['move_group_qty'],0)
            print('move_qty', move_qty)
            moves[self.f['inv_move_qty']] = move_qty
            self.validate_move_qty(product_code, sku, lot_number,  warehouse, location, move_qty)
            
            move_vals_from = {'_id': f"{product_code}_{sku}_{lot_number}_{warehouse}_{location}",
                        'move_out':move_qty,
                        'product_code':product_code,
                        'product_lot':lot_number,
                        'warehouse': warehouse,
                        'warehouse_location': location
                        }
            if self.folio:
                move_vals_from.update({'kwargs': {'nin_folio':self.folio }})
            move_vals_to = deepcopy(move_vals_from)
            move_vals_to.pop('move_out')
            move_vals_to.update(
                {
                '_id': set_location,
                'warehouse': warehouse_to,
                'warehouse_location': location_to,
                'from_folio':stock['folio'], 
                'move_in':move_qty,
                'move_qty':move_qty
            })
            # lots_in[set_location] = lots_in.get(set_location, move_vals_to) 
            print('setting cache to...', move_vals_to)
            self.cache_set(move_vals_to)
            print('setting cache form...', move_vals_from)
            self.cache_set(move_vals_from)
            new_lot = stock.get('record',{}).get('answers',{})
            warehouse_ans = self.swap_location_dest(self.answers[self.WAREHOUSE_LOCATION_DEST_OBJ_ID])
            new_lot.update(warehouse_ans)
            new_records_data.append(self.create_inventory_flow(answers=new_lot))
        

        res ={}

        create_new_records = []
        for record in new_records_data:
            if record.get('new_record'):
                create_new_records.append(record['new_record'])
            else:
                print('YA EXISTE... record ya se actualizco usando cache', record)
        print('create_new_records=',create_new_records)
        print('TODO mover status a lineas de registro')
        res_create = self.lkf_api.post_forms_answers_list(create_new_records)
        #updates records from where stock was moved
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        res ={}
        print('res_create', res_create)
        #res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        return True

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
        try:
            res = self.cr.find_one(match_query, {'answers':1})
        except:
            res = None
        return res

    def move_out_multi_location(self):
        move_lines = self.answers[self.f['move_group']]

        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        print('ans', self.answers)
        product_code = self.answers.get(self.SKU_OBJ_ID,{}).get(self.f['product_code'])
        print('product_code1', product_code)
        for moves in move_lines:
            print('move........', moves)
            info_plant = moves.get(self.STOCK_INVENTORY_OBJ_ID, {})
            stock = {'product_code':product_code}
            stock = self.get_stock_info_from_catalog_inventory(answers=moves, data=stock)
            print('product_code........',product_code)
            lot_number = stock.get('lot_number')
            sku = stock.get('sku')
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
            self.validate_move_qty(product_code, sku, stock['lot_number'],  stock['warehouse'], stock['warehouse_location'], move_qty)
            if stock.get('folio'):
                folios.append(stock['folio'])
        self.cache_set({
                    '_id': f"{product_code}_{sku}_{stock['lot_number']}_{stock['warehouse']}_{stock['warehouse_location']}",
                    'move_out':move_qty,
                    'product_code':product_code,
                    'sku':sku,
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

        res[self.SKU_OBJ_ID ] = deepcopy(self.answers.get(self.SKU_OBJ_ID, {}))
        res[self.SKU_OBJ_ID ]['6205f73281bb36a6f1573357'] = 8
        soil_type = self.unlist(self.answers.get(self.SKU_OBJ_ID,{}).get(self.f['reicpe_soil_type'],""))
        # res[self.SKU_OBJ_ID ][self.f['reicpe_soil_type']] = soil_type
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

    def product_sku_query(self, all_sku, recipe_type=None):
        if not recipe_type:
            #todo agregar recipe type que va a ser el stocking format
            recipe_type='Main'
        mango_query = {
            "selector": {
                "answers": {}
                    } ,
            "limit": 1000,
            "skip": 0
                    }
        if all_sku:
            if len(all_sku) == 1:
                mango_query['selector']['answers'].update({self.f['product_code']:  all_sku[0] })
            else:
                mango_query['selector']['answers'].update({self.f['product_code']: {"$in": all_sku},})
        return mango_query

    def get_product_sku(self, all_codes):
        all_sku = []
        for sku, product_code in all_codes.items():
            if sku not in all_sku:
                all_sku.append(sku.upper())
        skus = {}
        mango_query = self.product_sku_query(all_sku)
        sku_finds = self.lkf_api.search_catalog(self.SKU_ID, mango_query)
        for this_sku in sku_finds:
                product_code = this_sku.get(self.f['product_code'])
                skus[product_code] = skus.get(product_code, {})
                skus[product_code].update({
                    'sku':this_sku.get(self.f['sku']),
                    'product_name':this_sku.get(self.f['product_name']),
                    'product_category':this_sku.get(self.f['product_category']),
                    'product_type':this_sku.get(self.f['product_type']),
                    'product_department':this_sku.get(self.f['product_department']),
                    'sku_color':this_sku.get(self.f['sku_color']),
                    'sku_image':this_sku.get(self.f['sku_image'],),
                    'sku_note':this_sku.get(self.f['sku_note'],),
                    'sku_package':this_sku.get(self.f['reicpe_container'],),
                    'sku_per_package':this_sku.get(self.f['reicpe_per_container'],),
                    'sku_size' : this_sku.get(self.f['sku_size']),
                    'sku_source' : this_sku.get(self.f['sku_source']),
                    })
        return skus

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

    def get_invtory_record_by_product(self, form_id, product_code, sku, lot_number, warehouse, location, **kwargs):
        #use to be get_record_greenhouse_inventory
        get_many = kwargs.get('get_many')
        query_warehouse_inventory = {
            'form_id': form_id,
            'deleted_at': {'$exists': False},
            f"answers.{self.SKU_OBJ_ID}.{self.f['product_code']}": product_code,
            f"answers.{self.SKU_OBJ_ID}.{self.f['sku']}": sku,
            f"answers.{self.f['product_lot']}": lot_number,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}": warehouse,
            f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}": location,
        }
        print('query', query_warehouse_inventory)
        if get_many:
            records = self.cr.find(query_warehouse_inventory, 
                {'folio': 1, 'answers': 1, 'form_id': 1, 'user_id': 1,'created_at':1}).sort('created_at')
            record = [x for x in records]
        else:
            record = self.cr.find_one(query_warehouse_inventory, {'folio': 1, 'answers': 1, 'form_id': 1, 'user_id': 1})
        return record
   
    #### Temino heredacion para hace lote tipo string

    def product_stock_exists(self, product_code, sku, lot_number=None, warehouse=None, location=None,  status=None):
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id": self.FORM_INVENTORY_ID,
            f"answers.{self.SKU_OBJ_ID}.{self.f['product_code']}": product_code,
            f"answers.{self.SKU_OBJ_ID}.{self.f['sku']}": sku,
            }
        if warehouse:
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
            match_query.update({f"answers.{self.WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))

        match_query_stage2 = {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"}
        if inc_folio:
            match_query_stage2 = {"$or": [
                {f"answers.{self.f['grading_group']}.{self.f['inv_adjust_grp_status']}": "done"},
                get_folios_match(inc_folio = inc_folio)
                ]}
        if product_code:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query_stage2.update({f"answers.{self.f['grading_group']}.{self.f['product_lot']}":lot_number})
        if location:
            match_query_stage2.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_lot_location']}":location})
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
                    'product_code': f"$answers.{self.f['grading_group']}.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
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
            match_query.update({f"answers.{self.SKU_OBJ_ID}.{self.f['product_code']}":product_code})
        if warehouse:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_dest']}":warehouse})      
        if location:
            match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_location_dest']}":location})
        if lot_number:
            unwind_query.update({f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        project = {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
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

    def stock_one_many_one(self, move_type, product_code=None, sku=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        print('11move_type=',move_type)
        res = super().stock_one_many_one(
            move_type, 
            product_code=product_code, 
            sku=sku, 
            lot_number=lot_number, 
            warehouse=warehouse, 
            location=location, 
            date_from=date_from, 
            date_to=date_to, 
            status=status, 
            **kwargs)
        if move_type == 'out':
            res += self.stock_out_ordens_de_servicio(product_code, sku, lot_number, warehouse, location, date_from, date_to, status,  **kwargs)
        return res

    def stock_out_ordens_de_servicio(self, product_code=None, sku=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        match_query = {
            "deleted_at":{"$exists":False},
            }
        form_query = {"form_id": self.CONTACTO_CENTER_FORM_ID}
        match_query.update(form_query)
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        unwind = {'$unwind': '$answers.{}'.format(self.f['parts_group'])}
        unwind_query = {}
        unwind_stage = []
        print('warehouse', warehouse)
        print('location', location)
        print('product_code', product_code)
        print('sku', sku)
        print('lot_number', lot_number)
        inc_folio = kwargs.get("inc_folio")
        nin_folio = kwargs.get("nin_folio")
        if inc_folio:
            match_query.update({"folio":inc_folio})
        if nin_folio:
            match_query.update({"folio": {"$ne":nin_folio }})

        if warehouse:
            match_query.update({f"answers.{self.WAREHOUSE_OBJ_ID}.{self.f['warehouse']}":warehouse})
        if location:
            unwind_query.update({f"answers.{self.f['parts_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location})
       
        if product_code:
            unwind_query.update(
                {f"answers.{self.f['parts_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code}
                )
        if sku:
            unwind_query.update(
                {f"answers.{self.f['parts_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['sku']}":sku}
                )
        if lot_number:
            unwind_query.update(
                {f"answers.{self.f['parts_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['lot_number']}":lot_number},
                )
        # if status:
        #     match_query.update({f"answers.{self.f['stock_status']}":status})
        # if status:
        #     unwind_query.update({f"answers.{self.f['parts_group']}.{self.f['inv_adjust_grp_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        project = {'$project':
                {'_id': 1,
                    'product_code':f"$answers.{self.f['parts_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                    'total': f"$answers.{self.f['parts_group']}.{self.f['move_group_qty']}",
                    }
            }
        query= [{'$match': match_query }]
        query.append(unwind)
        if unwind_query:
            query.append({'$match': unwind_query })
        if unwind_stage:
            query += unwind_stage
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
        # print('query=', simplejson.dumps(query, indent=3))
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
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})

        supplier_warehouse = self.get_warehouse(warehouse_type='Supplier')
        if supplier_warehouse:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['warehouse']}":{"$in":supplier_warehouse}})
        if location:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":location})
        if lot_number:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if status:
            match_query.update({f"answers.{self.f['inv_adjust_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [{'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}",
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
            # "form_id": {"$in":[self.SCRAP_FORM_ID, self.GRADING_FORM_ID]}
            "form_id": self.SCRAP_FORM_ID
            }
        if product_code:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code})
        if lot_number:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_lot']}":lot_number})
        if warehouse:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['warehouse']}":warehouse})      
        if location:
            match_query.update({f"answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}":location}) 
        if status:
            match_query.update({f"answers.{self.f['inv_scrap_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        query= [
            {'$match': match_query },
            {'$project':
                {'_id': 1,
                    'product_code': f"$answers.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}",
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
            match_query.update({f"answers.{self.SKU_OBJ_ID}.{self.f['product_code']}":product_code})
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
                    'product_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
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
            inv = self.get_invtory_record_by_product(self.FORM_INVENTORY_ID, product_code, sku, lot_number, warehouse, location)
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
        print('patch stock folio', folios)
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

    def validate_move_qty(self, product_code, sku, lot_number, warehouse, location, move_qty, date_to=None):
        inv = self.get_product_stock(product_code, sku=sku,lot_number=lot_number, warehouse=warehouse, location=location,  
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