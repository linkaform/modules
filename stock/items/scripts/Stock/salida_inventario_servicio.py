# -*- coding: utf-8 -*-
import sys, simplejson
from bson import ObjectId

from stock_utils import Stock

from account_settings import *

class Stock(Stock):
    
    def set_destination_warehouse(self):
        #     "67338de0f35b6aebd68e7ea2":{
        # "65bdc71b3e183f49761a33b9":"Clientes", 
        # "65c12749cfed7d3a0e1a341b":"Clientes"
        # }
        dest_location = {
            self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: {
                self.WH.f['warehouse_dest']:"Clientes",
                self.WH.f['warehouse_location_dest']:"Clientes"
            },
        self.f['stock_status']:"to_do"
        }

        
        #status = {self.f['stock_status']:"To do"}
        self.answers.update(dest_location)


        print('set_destination_warehouse')
        print('Respuesta', dest_location)

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    stock_obj.set_destination_warehouse()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers
    }))