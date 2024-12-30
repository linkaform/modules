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
                            "deleted_at":{"$exists":False},
                            "form_id":self.MOVE_NEW_PRODUCTION_ID,
                        }
                    },
                    {"$project":{
                        "_id":1,
                        'plant_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                        'stage': f"$answers.{self.SKU_OBJ_ID}.{self.f['reicpe_stage']}",
                        'cut_week': f"$answers.{self.f['production_cut_week']}",
                        'cut_year': f"$answers.{self.f['plant_cut_year']}",
                        'containers': f"$answers.{self.f['actuals']}",
                        'eaches': f"$answers.{self.f['actual_eaches_on_hand']}"}
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "stage": "text" },
              { "cut_week": "integer" },
              { "cut_year": "integer" },
              { "containers": "integer" },
              { "eaches": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "stage": "text" },
              { "cut_week": "integer" },
              { "cut_year": "integer" },
              { "containers": "integer" },
              { "eaches": "integer" },
            ]
        }
        return query


if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query))

