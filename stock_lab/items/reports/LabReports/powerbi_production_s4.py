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
                        "form_id": self.PRODUCTION_FORM_ID,
                        "deleted_at":{"$exists":False},
                        }
                    },
                    {"$project":{
                        "_id":1,
                        'plant_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                        'qty_per_container': {"$arrayElemAt": [f"$answers.{self.SKU_OBJ_ID}.6205f73281bb36a6f157335b",0]},
                        'required': f"$answers.{self.f['production_requier_containers']}",
                        'year': f"$answers.{self.f['production_year']}",
                        'week': f"$answers.{self.f['production_week']}",
                        'total_produced': f"$answers.{self.f['total_produced']}",
                        }
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "qty_per_container": "text" },
              { "required": "integer" },
              { "year": "integer" },
              { "week": "integer" },
              { "total_produced": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "qty_per_container": "integer" },
              { "required": "integer" },
              { "year": "integer" },
              { "week": "integer" },
              { "total_produced": "integer" },
            ]
        }
        return query


if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query))