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
        self.FOLDER_FORMS_ID = self.lkm.item_id('Stock', 'form_folder').get('id')

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
        move_lines = self.explote_kit(self.answer_label['move_group'])
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
            # product_code = info_product.get(self.f['product_code'])
            # sku = info_product.get(self.f['sku'])
            package = move_line.get(self.f['sku_package'])
            print('package', package)
            print('package', packadge)
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

    def get_product_sku(self, all_codes):
        #migrara a branch de magnolia
        all_sku = []
        for sku, product_code in all_codes.items():
            if sku not in all_sku:
                all_sku.append(sku.upper())
        skus = {}
        mango_query = self.product_sku_query(all_sku)
        print('SKU_ID =',self.SKU_ID)
        print('mango_query =',mango_query)
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

    def stock_one_many_one(self, move_type, product_code=None, sku=None, lot_number=None, warehouse=None, location=None, date_from=None, date_to=None, status='done', **kwargs):
        #revisar con roman parece q este cambio es de Impco ya que hace referenci a contact_center
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

