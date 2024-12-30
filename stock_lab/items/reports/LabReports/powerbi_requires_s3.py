#-*- coding: utf-8 -*-

import simplejson, sys

from stock_report import Reports

from account_settings import *

class Reports(Reports):

    def get_S3_requiers_by_year_week(self):
        query = {
            "collection":"form_answer",
            "db_name":"infosync_answers_client_9908",
            "query" :[
                    {"$match":{
                            "form_id":self.PRODUCTION_PLAN, "deleted_at":{"$exists":False}
                        }
                    },
                    {"$unwind": f"$answers.{self.f['prod_plan_development_group']}"},
                    {"$project":{
                        "_id":1,
                        "plant_code":f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
                        "year_week":f"$answers.{self.f['prod_plan_S3_requier_yearweek']}",
                        "req":f"$answers.{self.f['prod_plan_require_S3']}",
                    }},
                    {"$project":{
                        "_id":0,
                        "plant_code":"$plant_code",
                        "year_week":"$year_week",
                        "req":"$req",
                    }},
                    {"$sort": {"year_week": 1}},
                    {"$group":{
                        "_id":{
                            "plant_code":"$plant_code",
                            "year_week":"$year_week",
                            # "cycle":"$cycle",
                            # "starter":"$starter"
                            },
                        "req":{"$sum":"$req"}
                    }},
                    {"$project":{
                        "_id":0,
                        "plant_code":"$_id.plant_code",
                        "year_week":"$_id.year_week",
                        "req":"$req",
                    }},
                    {"$sort": {"plant_code":1,"year_week": 1}}
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "plant_code": "text" },
              { "year_week": "integer" },
              { "req": "integer" },
            ],
            "output_schema":
            [
              { "plant_code": "text" },
              { "year_week": "integer" },
              { "req": "integer" },
            ]
        }
        return query


if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_S3_requiers_by_year_week()
    print(simplejson.dumps(query))
