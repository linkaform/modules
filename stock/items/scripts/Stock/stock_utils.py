# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.stock.app import Stock

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

        self.f.update({
            'parts_group':'62c5da67f850f35cc2483346',
            })
        self.answer_label = self._labels()
        self.FOLDER_FORMS_ID = self.lkm.item_id('Stock', 'form_folder').get('id')

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
        # move_lines = self.answer_label['move_group']
        move_lines = self.explote_kit(self.answer_label['move_group'], warehouse=warehouse, location=location )
        print('move lines EXPLOE', move_lines)
        print('move lines EXPLOE', revisar_bien)
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


    #######
    def do_scrap(self):
        answers = self.answers
        stock = self.get_stock_info_from_catalog_inventory()
        scrap_qty = answers.get(self.f['inv_scrap_qty'], 0)
        cuarentin_qty = answers.get(self.f['inv_cuarentin_qty'], 0)
        if scrap_qty or cuarentin_qty:
            move_qty = scrap_qty + cuarentin_qty
            self.validate_move_qty(stock['product_code'], stock['sku'], stock['lot_number'], stock['warehouse'], stock['warehouse_location'], move_qty, date_to=None)
            self.cache_set({
                        '_id': f"{stock['product_code']}_{stock['sku']}_{stock['lot_number']}_{stock['warehouse']}_{stock['warehouse_location']}",
                        'scrapped':scrap_qty,
                        'cuarentin':cuarentin_qty,
                        'lot_number':stock['lot_number'],
                        'product_code':stock['product_code'],
                        'sku':stock['sku'],
                        'warehouse': stock['warehouse'],
                        'warehouse_location': stock['warehouse_location'],
                        'record_id': self.record_id
                        })
        res = self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=stock['folio'] )
        return res.get(stock['folio'],{}) 

    def get_stock_info_from_catalog_wl(self, answers={}, data={}, search_key=None):
        print('---eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-------')
        if not answers:
            answers = self.answers
        res = {}
        if not search_key:
            search_key = self.WH.WAREHOUSE_LOCATION_OBJ_ID
        wh_info = answers.get(search_key, {})
        if not wh_info:
            data = self.get_stock_info_from_catalog_wl(answers=self.answers, data=data, search_key=self.STOCK_INVENTORY_OBJ_ID)
        res['warehouse'] = data.get('warehouse',wh_info.get(self.f['warehouse']))
        res['warehouse_location'] = data.get('warehouse_location', wh_info.get(self.f['warehouse_location']))
        return res

    def get_stock_info_from_catalog_wld(self, answers={}, data={},  search_key=None):
        print('oooooooooooooooooooooooooooooooooooooooooooooooooooo')
        if not answers:
            answers = self.answers
        res = {}
        if not search_key:
            search_key = self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID
        wh_info = answers.get(search_key, {})
        if not wh_info:
            data = self.get_stock_info_from_catalog_wld(answers=self.answers, data=data, search_key=self.STOCK_INVENTORY_OBJ_ID)
        res['warehouse_dest'] = data.get('warehouse_dest',wh_info.get(self.f['warehouse_dest']))
        res['warehouse_location_dest'] = data.get('warehouse_location_dest',wh_info.get(self.f['warehouse_location_dest']))
        return res