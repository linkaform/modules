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
                        'folio': "$folio",
                        'warehouse': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse']}",
                        'warehouse_location': f"$answers.{self.CATALOG_INVENTORY_OBJ_ID}.{self.f['warehouse_location']}",
                        'plant_code': f"$answers.{self.SKU_OBJ_ID}.{self.f['product_code']}",
                        'from_stage': f"$answers.{self.SKU_OBJ_ID}.{self.f['reicpe_start_size']}",
                        'stage': f"$answers.61ef32bcdf0ec2ba73dec33c.{self.f['reicpe_stage']}",
                        'recipe_type': f"$answers.{self.SKU_OBJ_ID}.63483f8e2c8c769718b102b1",
                        'growth_weeks': f"$answers.{self.SKU_OBJ_ID}.6205f73281bb36a6f1573357",
                        'media_tray': f"$answers.{self.SKU_OBJ_ID}.6209705080c17c97320e3382",
                        'qty_per_container': f"$answers.{self.SKU_OBJ_ID}.6205f73281bb36a6f157335b",
                        'botanical_name': f"$answers.{self.SKU_OBJ_ID}.61ef32bcdf0ec2ba73dec33e",
                        'product_lot': f"$answers.{self.f['product_lot']}",
                        'planted_yearweek': f"$answers.{self.f['plant_cut_yearweek']}",
                        'new_cutweek': f"$answers.{self.f['new_cutweek']}",
                        'media_name': f"$answers.{self.MEDIA_LOT_OBJ_ID}.{self.f['media_name']}",
                        'media_lot': f"$answers.{self.MEDIA_LOT_OBJ_ID}.{self.f['media_lot']}",
                        'cut_day': f"$answers.{self.f['production_cut_day']}",
                        'cut_week': f"$answers.{self.f['production_cut_week']}",
                        'cycle': f"$answers.{self.f['plant_cycle']}",
                        'group': f"$answers.{self.f['plant_group']}",
                        'produced_containers': "$answers.6271dc35e84e2577579eafeb",
                        'move_in': "$answers.620ad6247a217dbcb888d000",
                        'move_out': "$answers.620ad6247a217dbcb888d17e",
                        'sell_flats': "$answers.6442e2fbc0dd855fe856f1da",
                        'cuarentin_flats': "$answers.6442e2fbc0dd855fe856fddd",
                        'scrapped_flats': "$answers.620ad6247a217dbcb888d16d",
                        'adjustments': "$answers.aaaaa0000000000000000000",
                        'actuals': f"$answers.{self.f['actuals']}",
                        'actual_eaches': f"$answers.{self.f['actual_eaches_on_hand']}",
                        'contamin_code': f"$answers.{self.f['plant_contamin_code']}",
                        'inventory_status': f"$answers.{self.f['status']}",
                        'grading_status': "$answers.644c36f1d20db114694a495a",
                        }
                    },
                    ],
            "command": "aggregate",
            "selectColumns":[],
            "input_schema":  [
              { "folio": "text" },
              { "warehouse": "text" },
              { "warehouse_location": "text" },
              { "plant_code": "text" },
              { "from_stage": "text" },
              { "stage": "text" },
              { "recipe_type": "text" },
              { "growth_weeks": "integer" },
              { "media_tray": "text" },
              { "qty_per_container": "integer" },
              { "botanical_name": "text" },
              { "product_lot": "text" },
              { "planted_yearweek": "integer" },
              { "new_cutweek": "integer" },
              { "media_name": "text" },
              { "media_lot": "text" },
              { "cut_day": "integer" },
              { "cut_week": "integer" },
              { "cycle": "text" },
              { "group": "integer" },
              { "produced_containers": "integer" },
              { "move_in": "integer" },
              { "move_out": "integer" },
              { "sell_flats": "integer" },
              { "cuarentin_flats": "integer" },
              { "scrapped_flats": "integer" },
              { "adjustments": "integer" },
              { "actuals": "integer" },
              { "actual_eaches": "integer" },
              { "contamin_code": "text" },
              { "inventory_status": "text" },
              { "grading_status": "text" },
            ],
            "output_schema":
            [
              { "folio": "text" },
              { "warehouse": "text" },
              { "warehouse_location": "text" },
              { "plant_code": "text" },
              { "from_stage": "text" },
              { "stage": "text" },
              { "recipe_type": "text" },
              { "growth_weeks": "integer" },
              { "media_tray": "text" },
              { "qty_per_container": "integer" },
              { "botanical_name": "text" },
              { "product_lot": "text" },
              { "planted_yearweek": "integer" },
              { "new_cutweek": "integer" },
              { "media_name": "text" },
              { "media_lot": "text" },
              { "cut_day": "integer" },
              { "cut_week": "integer" },
              { "cycle": "text" },
              { "group": "integer" },
              { "produced_containers": "integer" },
              { "move_in": "integer" },
              { "move_out": "integer" },
              { "sell_flats": "integer" },
              { "cuarentin_flats": "integer" },
              { "scrapped_flats": "integer" },
              { "adjustments": "integer" },
              { "actuals": "integer" },
              { "actual_eaches": "integer" },
              { "contamin_code": "text" },
              { "inventory_status": "text" },
              { "grading_status": "text" },
            ]
        }
        return query

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    query = report_obj.get_query()
    print(simplejson.dumps(query))