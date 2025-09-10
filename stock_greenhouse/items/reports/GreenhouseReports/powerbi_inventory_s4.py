#-*- coding: utf-8 -*-

from textwrap import indent
import simplejson, sys

from gh_stock_report import Reports

from account_settings import *

class Reports(Reports):


    def get_query(self):
        query = {
            "collection":"form_answer",
            "db_name":"infosync_answers_client_9908",
            "query" :[
                    {"$match":{
                        "form_id":  self.FORM_INVENTORY_ID,
                        f"answers.{self.f['inventory_status']}":  'active',
                        "deleted_at":{"$exists":False},
                        }
                    },
                    {"$project":{
                        "_id":1,
                        'plant_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                        'size': f"$answers.{self.SKU_OBJ_ID}.{self.f['reicpe_start_size']}",
                        'warehouse': f"$answers.{self.WAREHOUSE_OBJ_ID}.{self.f['warehouse']}",
                        "qty_per_container": {
                            "$convert": {
                            "input": {
                                "$arrayElemAt": [
                                f"$answers.{self.SKU_OBJ_ID}.{self.f['prod_qty_per_container']}",
                                0
                                ]
                            },
                            "to": "int",
                            "onError": None,
                            "onNull": None
                            }
                        },
                        'ready_year_week': f"$answers.{self.f['product_lot']}",
                        'production': f"$answers.{self.f['production']}",
                        'sales': f"$answers.{self.f['sales']}",
                        'adjustments': f"$answers.{self.f['adjustments']}",
                        'scrapped': f"$answers.{self.f['scrapped']}",
                        'inventory_status': f"$answers.{self.f['inventory_status']}",
                        'actuals': f"$answers.{self.f['actuals']}",
                        }
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "qty_per_container": "text" },
              { "ready_year_week": "integer" },
              { "actuals": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "qty_per_container": "integer" },
              { "ready_year_week": "integer" },
              { "actuals": "integer" },
            ]
        }
        return query

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query['query'], indent=4))