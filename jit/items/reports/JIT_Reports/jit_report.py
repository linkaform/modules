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


    def get_elements(self, status):
        match_query = {
            "form_id": self.REGLAS_REORDEN,
            "deleted_at":{"$exists":False},
        }
        if status:
            match_query.update({'answers.620ad6247a217dbcb888d175': status})
        query = [
            {"$match": match_query},
            {"$project": {
                "_id":1,
                "folio":"$folio",
                "product_code": "$answers.66dfc4d9a306e1ac7f6cd02c.61ef32bcdf0ec2ba73dec33d",
                "stock_maximum": "$answers.66ea62dac9aefada5b04b73a",
                "warehouse": f"$answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse']}",
                "location":  f"$answers.{self.WAREHOUSE_LOCATION_OBJ_ID}.{self.f['warehouse_location']}",
            }},
            {"$group":{
                "_id":{
                    "warehouse":"$warehouse",
                    "location":"$location",
                    "product_code":"$product_code",
                },
                "stock_maximum":{"$last":"$stock_maximum"}
            }},
            {"$project":{
                "_id":0,
                "warehouse":"$_id.warehouse",
                "location":"$_id.location",
                "product_code":"$_id.product_code",
                "stock_maximum": "$stock_maximum"
            }},
        ]
        results = self.cr.aggregate(query)
        for result in results:
            print(result)
        #res_format = format_firts_element(result)
        return
