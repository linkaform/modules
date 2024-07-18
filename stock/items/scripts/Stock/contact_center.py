# -*- coding: utf-8 -*-
import sys, simplejson

from stock_utils import Stock

from account_settings import *


class Stock(Stock):

    def contact_center_stock_out(self, **kwargs):
        parts = self.answers.get(self.f['parts_group'])
        warehouse = self.answers.get(self.WAREHOUSE_OBJ_ID,{}).get(self.f['warehouse'])
        if parts:
            for part in parts:
                print('part', part)
                product_code = part.get(self.STOCK_INVENTORY_OBJ_ID,{}).get(self.f['product_code'])
                sku = part.get(self.STOCK_INVENTORY_OBJ_ID,{}).get(self.f['sku'])
                lot_number = part.get(self.STOCK_INVENTORY_OBJ_ID,{}).get(self.f['lot_number'])
                location = part.get(self.STOCK_INVENTORY_OBJ_ID,{}).get(self.f['warehouse_location'])
                print('product_code', product_code)
                print('sku', sku)
                print('lot_number', lot_number)
                qty_to_move = part.get(self.f['move_group_qty'])
                print('qty_to_move', qty_to_move)
                part_inv = self.get_invtory_record_by_product(self.FORM_INVENTORY_ID, product_code, sku, lot_number, warehouse, location)
                print('part_inv', part_inv)
                if part_inv and part_inv.get('folio'):
                    self.cache_set({
                        '_id': f'{product_code}_{sku}_{lot_number}_{warehouse}_{location}',
                        'move_out':qty_to_move,
                        'lot_number':lot_number,
                        'sku':sku,
                        'product_code':product_code,
                        'warehouse': warehouse,
                        'warehouse_location': location,
                        'record_id': self.record_id
                    })
                    self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios= part_inv.get('folio'))


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.contact_center_stock_out()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans':  stock_obj.answers
    }))
