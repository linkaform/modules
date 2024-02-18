# -*- coding: utf-8 -*-
import sys, simplejson

from lab_stock_utils import Stock

from account_settings import *


class Stock(Stock):

    def move_location(self):
        move_lines = self.answers[self.f['move_group']]

        # Información original del Inventory Flow
        status_code = 0
        move_locations = []
        folios = []
        for moves in move_lines:
            print('move........', moves)
            info_plant = moves.get(self.CATALOG_INVENTORY_OBJ_ID, {})
            product_code, lot_number, warehouse, location = self.get_product_lot_location()
            print('product_code........',product_code)
            stock  = self.get_stock_info_from_catalog_inventory(moves)
            set_location = f"{stock['warehouse']}__{stock['warehouse_location']}"
            if set_location in move_locations:
                msg = "You are trying to move on more than one set, producto from the same location."
                msg += f"warehouse: {stock['warehouse']} / Location: {stock['warehouse_location']}"
                msg_error_app = {
                    f"{self.f['warehouse_location']}": {
                        "msg": [msg],
                        "label": "Please check your selected location",
                        "error":[]
      
                    }
                }
                self.LKFException()
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
        print('se va por cache o por no cache.......', self.record_id)
        print('se va por cache o por no folio.......', self.folio)
        if self.record_id:
            update_query = {'_id':self.record_id}
        elif self.folio:
            update_query = {'folio':self.folio}
        else:
            update_query = None
        if update_query: 
            print('self answers', self.answers)
            print('self update_query', update_query)
            mongores = self.cr.update_one(update_query,{'$set':{'answers':self.answers}})
            print('mongores', dir(mongores))
            print('mongores', mongores)
            print('mongores', mongores.raw_result)
        else:
            self.cache_set({
                        '_id': f"{product_code}_{stock['lot_number']}_{stock['warehouse']}_{stock['warehouse_location']}",
                        'move_out':move_qty,
                        'product_code':product_code,
                        'product_lot':stock['lot_number'],
                        'warehouse': stock['warehouse'],
                        'warehouse_location': stock['warehouse_location']
                        })
        self.update_stock(answers={}, form_id=self.FORM_INVENTORY_ID, folios=folios)


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
        return status_code

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.move_location()
    stock_obj.answers[stock_obj.f['move_status']] =  'done'
    stock_obj.answers[stock_obj.f['inv_adjust_status']] =  'done'
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
        }))
