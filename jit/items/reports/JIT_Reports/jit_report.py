# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date

print('Arranca jit - Report en modulo jit....')
from lkf_addons.addons.jit.report import Reports

#Se agrega path para que obtenga el archivo de Stock de este modulo
sys.path.append('/srv/scripts/addons/modules/jit/items/scripts/JIT')
from jit_utils import JIT


today = date.today()
year_week = int(today.strftime('%Y%W'))

class Reports(Reports, JIT):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False):
        #base.LKF_Base.__init__(self, settings, sys_argv=sys_argv, use_api=use_api)
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        

    def get_rutas_transpaso(self, product_codes=None):
        match_query ={ 
             'form_id': self.RUTAS_TRANSPASO,  
             #'form_id': 125127,  
             'deleted_at' : {'$exists':False},
         } 

        if type(product_codes) == list:
            match_query.update({
                 f"answers.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}": {"$in": product_codes}
                })
        elif product_codes:
            match_query.update({
                f"answers.{self.Product.SKU_OBJ_ID}.{self.f['product_code']}": product_codes
                 }) 
        query = [
            {'$match': match_query},
            {'$sort': {'created_at': 1}},
            # {'$limit':1},
            {'$unwind':f'$answers.{self.mf["rutas_group"]}'},
            {'$project':{
                    '_id':0,
                    'product_code':f'$answers.{self.Product.SKU_OBJ_ID}.{self.f["product_code"]}',
                    'sku':f'$answers.{self.Product.SKU_OBJ_ID}.{self.f["sku"]}',
                    'standar_pack':f'$answers.{self.mf["rutas_group"]}.{self.mf["standar_pack"]}',
                    'unit_of_measure':f'$answers.{self.mf["rutas_group"]}.{self.UOM_OBJ_ID}.{self.f["uom"]}',
                    'warehouse':f'$answers.{self.mf["rutas_group"]}.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f["warehouse"]}',
                    'warehouse_location':f'$answers.{self.mf["rutas_group"]}.{self.WH.WAREHOUSE_LOCATION_OBJ_ID}.{self.f["warehouse_location"]}',
                    'warehouse_dest':f'$answers.{self.mf["rutas_group"]}.{self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f["warehouse_dest"]}',
                    'warehouse_location_dest':f'$answers.{self.mf["rutas_group"]}.{self.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID}.{self.f["warehouse_location_dest"]}',
            }},
            ]
        # print('rrrquery', simplejson.dumps(query,indent=4))
        res =  self.format_cr(self.cr.aggregate(query))
        return res