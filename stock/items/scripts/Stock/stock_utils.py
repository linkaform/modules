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


        self.CONTACTO_CENTER_FORM_ID = 111563
        
        self.f.update({
            'parts_group':'62c5da67f850f35cc2483346',
            })
        self.answer_label = self._labels()

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




