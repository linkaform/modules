#-*- coding: utf-8 -*-

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
                        "form_id":  81434,
                        "deleted_at":{"$exists":False},
                        }
                    },
                    {"$unwind":"$answers.6232c4db8ace7e82661dc0e6"},
                    {"$project":{
                        "_id":1,
                        'plant_code': f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
                        'month': "$answers.6206b9ae8209a9677f9b8bd9",
                        'year': "$answers.6206b9ae8209a9677f9b8bda",
                        'stage': "$answers.620985602aee88510aca8bff",
                        'qty': "$answers.6206b9ae8209a9677f9b8bdb",
                        'ready_date': "$answers.6232c4db8ace7e82661dc0e6.6232c528ba5509ed101dc089",
                        'foracast': "$answers.6232c4db8ace7e82661dc0e6.6232c528ba5509ed101dc08b",
                        }
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "month": "integer" },
              { "year": "integer" },
              { "stage": "text" },
              { "qty": "integer" },
              { "ready_date": "integer" },
              { "forcast": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "month": "integer" },
              { "year": "integer" },
              { "stage": "text" },
              { "qty": "integer" },
              { "ready_date": "integer" },
              { "forcast": "integer" },
            ]
        }
        return query

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query))