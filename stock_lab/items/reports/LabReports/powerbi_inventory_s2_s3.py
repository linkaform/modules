#-*- coding: utf-8 -*-

import simplejson, sys

from stock_report import Reports

from account_settings import *

class Reports(Reports):

    def get_query(self):
        query = {
            "collection":"form_answer",
            "db_name":"infosync_answers_client_9908",
            "query" :[
                    {"$match":{
                        "form_id":  self.FORM_INVENTORY_ID,
                        f"answers.{self.f['status']}":  'active',
                        "deleted_at":{"$exists":False},
                        }
                    },
                    {"$project":{
                        "_id":1,
                        'plant_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                        'stage': f"$answers.61ef32bcdf0ec2ba73dec33c.{self.f['reicpe_stage']}",
                        'new_cutweek': f"$answers.{self.f['new_cutweek']}",
                        'product_lot': f"$answers.{self.f['product_lot']}",
                        'actuals': f"$answers.{self.f['actuals']}",
                        'actual_eaches': f"$answers.{self.f['actual_eaches_on_hand']}",
                        }
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "stage": "text" },
              { "new_cutweek": "integer" },
              { "product_lot": "text" },
              { "actuals": "integer" },
              { "actual_eaches": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "stage": "text" },
              { "new_cutweek": "integer" },
              { "product_lot": "text" },
              { "actuals": "integer" },
              { "actual_eaches": "integer" },
            ]
        }
        return query

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query))