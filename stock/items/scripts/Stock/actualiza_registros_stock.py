# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *



class Stock(Stock):


    def update_stock_record(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        query = {
            'form_id':self.FORM_INVENTORY_ID, 
            'deleted_at': {'$exists': False},
            f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            f"answers.{self.f['folio_recepcion']}":"SAL-2663",
            # f"answers.{self.f['actuals']}":{'$exists':False},
             }
        print('query=', simplejson.dumps(query, indent=3))
        print(d)
        records = self.cr.find(query,{'folio':1, 'answers':1}).limit(7500)
        update_ids = []
        for rec in records:
            answers = rec.get('answers',{})
            if not answers:
                continue
            lot_number = answers.get(self.f['lot_number'])
            rec_id = rec.get('_id')
            query = {
            'form_id':self.FORM_INVENTORY_ID,
            'deleted_at': {'$exists': False},
            '_id': {'$ne':rec_id},
            f"answers.{self.Product.SKU_OBJ_ID}.{self.Product.f['product_code']}":'1468270',
            f"answers.{self.f['lot_number']}":lot_number,
             }
            old_rec = self.cr.find(query,{'folio':1})
            for x in old_rec:
                update_ids.append(str(x.get('_id')))
        print('Size of updated ids:' , len(update_ids))
        if update_ids:
            answer = {self.f['product_grading_pending']:'auditado'}
            print('Doing patch_multi_record')
            res = self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  record_id=update_ids, threading=True)
                

        return True


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    stock_obj.update_stock_record()
