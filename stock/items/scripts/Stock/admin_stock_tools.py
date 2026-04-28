# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *



class Stock(Stock):



    def get_active_stock(self):
        query = {
            "deleted_at":{"$exists":False},
            "form_id": self.FORM_INVENTORY_ID,
            f"answers.{self.f['status']}":"active",
            }
        res = self.cr.find(query,{'_id':1})
        return [str(x['_id']) for x in res]

    def update_stock_status(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        records = self.get_active_stock()
        print('va a acutalziar ', len(records), 'registros...')
        answer = {self.f['product_grading_pending']:'auditado'}
        res = self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  record_id=records, threading=True)
        return res


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    data = stock_obj.data.get('data',{})
    option = data.get("option",'')
    print('option', option)
    if option == 'update_status':
        print('corriendo update')
        response = stock_obj.update_stock_status()
    print('Termina....')



