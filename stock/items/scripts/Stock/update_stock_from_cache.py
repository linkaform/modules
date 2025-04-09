# -*- coding: utf-8 -*-
import sys, simplejson, copy

from stock_utils import Stock

from account_settings import *



class Stock(Stock):



    def update_stock_cache(self):
        """
        Se busca en la collecion de Stock el cache para hacer update
        """
        cache_stock = self.cache_get({"cache.cache_type" :"direct_move_in" },**{'keep_cache':False})
        print('cache_stock',cache_stock)
        for cache in cache_stock:
            cach = cache.get('cache')
            inserted_ids = [str(x) for x in cach.get('inserted_ids',[])]
            print('inserted_ids',inserted_ids)
            answer = {self.f['product_grading_pending']:'auditado'}
            if inserted_ids:
                self.lkf_api.patch_multi_record(answer, self.FORM_INVENTORY_ID,  record_id=inserted_ids, threading=True)
                

        return True


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv, use_api=True)
    stock_obj.console_run()
    stock_obj.update_stock_cache()
