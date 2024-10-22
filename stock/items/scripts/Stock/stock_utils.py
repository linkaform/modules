# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.stock.app import Stock
from lkf_addons.addons.stock.report import Reports


today = date.today()
year_week = int(today.strftime('%Y%W'))


class Stock(Stock, Reports):

    # _inherit = 'employee'
    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        print('stock utils.....')
        super().__init__(settings, folio_solicitud=folio_solicitud, sys_argv=sys_argv, use_api=use_api)


        # La relacion entre la forma de inventario y el catalogo utilizado para el inventario
        # por default simpre dejar los mismos nombres
        self.FORM_CATALOG_DIR = {
            self.FORM_INVENTORY_ID:self.CATALOG_INVENTORY_ID,
            }

        self.f.update({
            'parts_group':'62c5da67f850f35cc2483346',
            })
        self.answer_label = self._labels()
        self.FOLDER_FORMS_ID = self.lkm.item_id('Stock', 'form_folder').get('id')
        self.mf = {
            'xls_file': '66c797955cfca4851db2c3b8',
            'xls_onts': '66e0cd760cc8e3fb75f23803',
            'capture_num_serie': '66c75e0c0810217b0b5593ca'
        }

    def explote_kit(self, bom_lines, warehouse=None, location=None):
        bom_res = super().explote_kit(bom_lines, warehouse, location)
        add_row = 0
        rows_added = 0
        for idx, row in enumerate(bom_res):
            if row.get('bom_type') or row.get('bom_name'):
                row['move_group_qty'] = row['qty']
                if add_row > 0:
                    self.answers[self.f['move_group']].insert(idx+rows_added, self.answers[self.f['move_group']][idx+rows_added - 1])
                add_row += 1
            
        return bom_res

    def move_in(self):
        #solo se usa en pci y en pruebas de exposion de materiales
        # print('-- -- -- -- -- -- answers=',self.answers)
        answers = self._labels()

        self.answer_label = self._labels()

        print('-----------------------------answers', self.answer_label)
        warehouse = self.answer_label['warehouse']
        location = self.answer_label['warehouse_location']
        warehouse_to = self.answer_label['warehouse_dest']
        location_to = self.answer_label['warehouse_location_dest']
        move_lines = self.JIT.explote_kit(self.answer_label['move_group'])
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
            if not moves.get('product_lot'):
                moves['product_lot'] = 'LotePCI001'
                move_line[self.f['product_lot']] = 'LotePCI001'
            print('moves', moves)
            if moves.get('bom_name'):
               move_line[self.f['bom_name']] = moves['bom_name']

            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            package = move_line.get(self.f['sku_package'])
            print('package', package)
            exists = self.product_stock_exists(
                product_code=moves['product_code'], 
                sku=moves['sku'], 
                lot_number=moves['product_lot'], 
                warehouse=warehouse_to, 
                location=location_to)
            cache_data = {
                        '_id': f"{moves['product_code']}_{moves['sku']}_{moves['product_lot']}_{warehouse_to}_{location_to}",
                        'move_in': moves['move_group_qty'],
                        'product_lot': moves['product_lot'],
                        'product_code':moves['product_code'],
                        'sku':moves['sku'],
                        'warehouse': warehouse_to,
                        'warehouse_location': location_to,
                        'record_id': self.record_id
                        }
            if exists:
                if self.folio:
                    cache_data.update({'kwargs': {'nin_folio':self.folio }})
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
        #pci
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
        connection_to_assign = self.answers.pop('id_user_to_assign', None)
        print('connection_to_assign =',connection_to_assign)
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
                        'warehouse_location': location,
                        'record_id':self.record_id
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
        if connection_to_assign:
            for stock_inv_created in res_create:
                new_record = '/api/infosync/form_answer/' + stock_inv_created['json']['id'] +'/'
                response_assign = self.lkf_api.assigne_connection_records( connection_to_assign, [new_record,])
                print('response_assign =',response_assign)
        """
        Despues de crear el registro de Stock Inventory hay que asignarlo al contratista
        """
        #res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)
        return True

    def read_xls(self, id_field_xls):
        file_url_xls = self.answers.get( id_field_xls )
        if not file_url_xls:
            print(f'no hay excel de carga {id_field_xls}')
            return False
        file_url_xls = file_url_xls[0].get('file_url')
        if not hasattr(self, 'prev_version'):
            if self.folio: 
                if self.current_record.get('other_versions'):
                    # print('entra al other_versions')
                    self.prev_version = self.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                else:
                    print('Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                    self.prev_version = self.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
                print('prev_version=',self.prev_version)
        elif self.prev_version.get('answers', {}).get( id_field_xls ):
            print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',self.prev_version.get('answers', {}).get( id_field_xls ) )
            return False
        header, records = self.read_file( file_url_xls )
        return {'header': header, 'records': records}

    def read_xls__temp(self, id_field_xls):
        file_url_xls = self.answers.get( id_field_xls )
        if not file_url_xls:
            print(f'no hay excel de carga {id_field_xls}')
            return False
        file_url_xls = file_url_xls[0].get('file_url')
        prev_version = {}
        if self.folio: 
            if self.current_record.get('other_versions'):
                # print('entra al other_versions')
                prev_version = self.get_prev_version(self.current_record['other_versions'], select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
            else:
                print('Ya tiene folio pero aun no hay mas versiones... revisando el current_record en la BD')
                prev_version = self.get_record_from_db(self.form_id, self.folio, select_columns=[ 'answers.{}'.format(self.mf['xls_file']), 'answers.{}'.format(self.mf['xls_onts']) ])
            print('prev_version=',prev_version)
        if prev_version.get('answers', {}).get( id_field_xls ):
            print( 'ya hay un excel previamente cargado... se ignora en esta ejecucion =',prev_version.get('answers', {}).get( id_field_xls ) )
            return False
        header, records = self.read_file( file_url_xls )
        return {'header': header, 'records': records}

    def stock_one_many_one(self, move_type, product_code=None, sku=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        unwind =None
        if move_type not in ('in','out'):
            raise('Move type only accepts values "in" or "out" ')
        match_query = {
            "deleted_at":{"$exists":False},
            }
        match_query.update(self.stock_kwargs_query(**kwargs))
        query_forms = self.STOCK_ONE_MANY_ONE_FORMS
        if len(query_forms) > 1:
            form_query = {"form_id":{"$in":query_forms}}
        else:
            form_query = {"form_id":self.STOCK_ONE_MANY_ONE_FORMS[0]}
        match_query.update(form_query)
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))

        unwind = {'$unwind': '$answers.{}'.format(self.f['move_group'])}
        unwind_query = {}
        unwind_stage = []
        # print('move type.............', move_type)
        # print('warehouse', warehouse)
        # print('location', location)
        # print('product_code', product_code)
        # print('sku', sku)
        # print('lot_number', lot_number)
        if move_type =='in':
            if warehouse:
                match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_dest']}":warehouse})
            if location:
                match_query.update({f"answers.{self.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f['warehouse_location_dest']}":location})        
        if move_type =='out':
            if warehouse:
                match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}":warehouse})
            if location:
                match_query.update({f"answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}":location})
       
        if product_code:
            p_code_query = {"$or":[
                {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['product_code']}":product_code},
                {f"answers.{self.f['move_group']}.{self.SKU_OBJ_ID}.{self.f['product_code']}":product_code}
            ]
            }
            unwind_stage.append({'$match': p_code_query })
        if sku:
            sku_query = {"$or":[
                {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['sku']}":sku},
                {f"answers.{self.f['move_group']}.{self.SKU_OBJ_ID}.{self.f['sku']}":sku}
            ]
            }
            unwind_stage.append({'$match': sku_query })
        if lot_number:
            lot_number_query = {"$or":[
                {f"answers.{self.f['move_group']}.{self.STOCK_INVENTORY_OBJ_ID}.{self.f['lot_number']}":lot_number},
                {f"answers.{self.f['move_group']}.{self.SKU_OBJ_ID}.{self.f['lot_number']}":lot_number},
                {f"answers.{self.f['move_group']}.{self.f['lot_number']}":lot_number}
            ]
            }
            unwind_stage.append({'$match': lot_number_query })
        if status:
            match_query.update({f"answers.{self.f['stock_status']}":status})
        if date_from or date_to:
            match_query.update(self.get_date_query(date_from=date_from, date_to=date_to, date_field_id=self.f['grading_date']))
        project = {'$project':
                {'_id': 1,
                    'product_code':{"$ifNull":[
                        f"$answers.{self.f['move_group']}.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['product_code']}",
                        f"$answers.{self.f['move_group']}.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                    ] } ,
                    'total': f"$answers.{self.f['move_group']}.{self.f['move_group_qty']}",
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
        for r in res:
            pcode = r.get('product_code')
            result[pcode] = result.get(pcode, 0)        
            result[pcode] += r.get('total',0)
        if product_code:
            result = result.get(product_code,0)
        return result 
