#-*- coding: utf-8 -*-

import simplejson, sys
from copy import deepcopy
from linkaform_api import settings
from bson import ObjectId
import time
from datetime import datetime, timedelta, date

from stock_report import Reports

from account_settings import *


table_data = []
plants = {}
WEEKS = []

class Reports(Reports):

    def arrange_info(self, planned, produced):
        total_planned = 0
        total_planned_s2 = 0
        total_planned_s3 = 0
        total_produced = 0
        total_produced_s2 = 0
        total_produced_s3 = 0
        teams = {}
        for x in planned:
            team = x['team']
            total_planned += x.get('total')
            teams[team] = teams.get(team,{'range':0,'value':0,'stage2':{'range':0, 'value':0}, 'stage3':{'range':0, 'value':0}})
            teams[team]['range'] += x.get('total')
            if x['stage'] == 'S2':
                total_planned_s2 += x.get('total')
                teams[team]['stage2']['range'] += x.get('total')
            if x['stage'] == 'S3':
                total_planned_s3 += x.get('total')
                teams[team]['stage3']['range'] += x.get('total')
        for x in produced:
            team = x['team']
            total_produced += x.get('total')
            teams[team] = teams.get(team, {'range':0,'value':0,'stage2':{'range':0, 'value':0}, 'stage3':{'range':0, 'value':0}})
            teams[team]['value'] += x.get('total')
            if x['stage'] == '2' or x['stage'] == 2:
                total_produced_s2 += x.get('total')
                teams[team]['stage2']['value'] += x.get('total')
            if x['stage'] == '3' or x['stage'] == 3:
                total_produced_s3 += x.get('total')
                teams[team]['stage3']['value'] += x.get('total')
        res = []
        row1 = [
            {"label":"Total", "range":total_planned,  "value":total_produced},
            {"label":"Stage 2", "range":total_planned_s2,"value":total_produced_s2 },
            {"label":"Stage 3", "range":total_planned_s3,"value":total_produced_s3 },
            ]
        res.append(row1)
        for t, val in teams.items():
            row = [{},{},{}]
            row[1] = val.pop('stage2')
            row[2] = val.pop('stage3')
            row[0] = val
            row[1]['label'] = 'Stage 2'
            row[2]['label'] = 'Stage 3'
            row[0]['label'] = t
            res.append(row)
        return res

    def week_hours(self, hours, hours_w, worked_hours, estimated_hours):
        teams = {'total':{'available':0, 'worked':0, 'estimated':0, 'consumed':0}}
        for x in hours:
            print('x=',x)
            team = x['team']
            teams[team]=  teams.get(team,{'available':0, 'worked':0, 'stage2':0, 'stage3':0,  'estimated':0, 'consumed':0})
            teams[team]['available']= x.get('hours', 0)
            teams[team]['estimated'] =round( estimated_hours.get(team,{}).get('S2',0) +  estimated_hours.get(team,{}).get('S3',0),2)
            teams[team]['stage2_estimated'] = estimated_hours.get(team,{}).get('S2',0)
            teams[team]['stage3_estimated'] = estimated_hours.get(team,{}).get('S3',0)
        for x in hours_w:
            print('2x=',x)
            team = x['team']
            teams[team]=  teams.get(team,{'available':0, 'worked':2, 'stage2':0, 'stage3':0, 'estimated':0, 'consumed':0})
            teams[team]['available'] = x.get('hours', 0)
            teams[team]['estimated'] =round( estimated_hours.get(team,{}).get('S2',0) +  estimated_hours.get(team,{}).get('S3',0),2)
            teams[team]['stage2_estimated'] = estimated_hours.get(team,{}).get('S2',0)
            teams[team]['stage3_estimated'] = estimated_hours.get(team,{}).get('S3',0)
        for x in worked_hours:
            team = x['team']
            teams[team]=  teams.get(team, {'available':0, 'worked':0, 'stage2':0, 'stage3':0, 'estimated':0, 'consumed':0})
            teams[team]['worked'] += round(x.get('hours', 0),2)
            teams[team]['estimated'] = round(estimated_hours.get(team,{}).get('S2',0) +  estimated_hours.get(team,{}).get('S3',0),2)
            if x['stage'] == 'S2':
                teams[team]['stage2'] = round(x.get('hours', 0),2)
            if x['stage'] == 'S3':
                teams[team]['stage3'] = round(x.get('hours', 0),2)
        for t, val in teams.items():
            teams['total']['available'] += val['available']
            teams['total']['worked'] += val['worked']
            teams['total']['estimated'] += round(val['estimated'],2)
            if val['available'] > 0:
                teams[t]['consumed'] =  round(val['worked'] / float(val['available']) * 100, 2)
        teams['total']['consumed'] = round(teams['total']['worked'] / float(teams['total']['available']) * 100, 2)
        return teams

    def plants_table(self, produced_plants):
        rows = []
        for code, stage in produced_plants.items():
            plant_name = stage.get('plant_name')
            for stage, container in stage.items():
                if stage == 'plant_name':
                    continue
                for name, res in container.items():
                    row = {
                        'plant_code': code,
                        'plant_name': plant_name,
                        'container': name,
                        'stage': stage,
                        'produced': res.get('produced',0),
                        'planned': res.get('planned',0),
                        }
                    if row['planned'] == 0:
                        row['progress'] = 0
                    else:
                        row['progress'] = round(row['produced']/row['planned']*100,2)
                    rows.append(row)
        return rows

    def get_report(self, cut_year, cut_week):
        global plants, WEEKS, yearweek

        to_week  = cut_week
        from_week = cut_week
        print('cut_year', cut_year)
        print('cut_week', cut_week)
        requierd = self.get_S2_requiers(cut_year, cut_week)
        print('requierd', requierd)
        planned = self.get_production_plan(cut_year, cut_week)
        produced = self.get_produced(cut_week, cut_year, from_week, to_week)
        produced_plants = self.get_produced_by_plant(cut_year, from_week, to_week)
        produced_plants = self.get_plan_by_plant(cut_year, cut_week, produced_plants)
        plant_rows = self.plants_table(produced_plants)
        hours = self.available_hours()
        hours_w = self.available_hours(yearweek)
        worked_hours = self.get_worked_hours(cut_week, cut_year, from_week, to_week)
        # print('worked_hours=',worked_hours)
        estimated_hours = self.get_estimated_hours(cut_year, cut_week)
        hours = self.week_hours(hours, hours_w, worked_hours, estimated_hours)
        res = self.arrange_info(planned, produced,)
        return plant_rows, res, hours

    def get_S2_requiers(self, cut_year, cut_week, plant_code=None):
        global cycles
        year_week = f'{cut_year}{cut_week}'
        match_query = {
            "deleted_at":{"$exists":False},
            "form_id":self.PRODUCTION_PLAN,
            }
        if plant_code:
            match_query.update({f"answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}":plant_code,})
        query = [
            {"$match": match_query },
            {"$unwind": f"$answers.{self.f['prod_plan_development_group']}"},
            {"$match": {f"answers.{self.f['prod_plan_development_group']}.{self.f['prod_plan_S2_requier_yearweek']}": {"$gte":year_week  }}},
            {"$project":{
                "_id":1,
                'folio':"$folio",
                "demand":f"$answers.{self.f['prod_plan_ready_date']}",
                "demand_plan":f"$answers.{self.f['prod_plan_demand_plan']}",
                "demand_client":f"$answers.{self.f['prod_plan_demand_client']}",
                "plant_code":f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_code']}",
                "plant_name":{ "$arrayElemAt": [ f"$answers.{self.PRODUCT_OBJ_ID}.{self.f['product_name']}", 0 ] },
                "year_week":f"$answers.{self.f['prod_plan_development_group']}.{self.f['prod_plan_S2_requier_yearweek']}",
                "req":f"$answers.{self.f['prod_plan_development_group']}.{self.f['prod_plan_require_S2']}",
                "starter":f"$answers.{self.f['prod_plan_development_group']}.{self.f['prod_plan_starter']}",
            }},
            {"$project":{
                "_id":0,
                'folio':"$folio",
                'demand':"$demand",
                'demand_plan':"$demand_plan",
                'demand_client':"$demand_client",
                "plant_code":"$plant_code",
                "plant_name": "$plant_name",
                "year_week":"$year_week",
                "req":"$req",
                # "starter":{"$cond": [{"$and":[{"$lte":["$req",stage_1_min]}, {"$eq":["$starter","yes"]}]},'yes','no']}
            }},
            {"$sort": {"year_week": 1}},
            {"$group":{
                "_id":{
                    'folio':"$folio",
                    'demand':"$demand",
                    'demand_plan':"$demand_plan",
                    'demand_client':"$demand_client",
                    "plant_code":"$plant_code",
                    # "cycle":"$cycle",
                    # "starter":"$starter"
                    },
                "year_week":{"$min":"$year_week"},
                "req":{"$min":"$req"}
            }},
            {"$project":{
                "_id":0,
                "folio":"$_id.folio",
                "demand":"$_id.demand",
                "demand_plan":"$_id.demand_plan",
                "demand_client":"$_id.demand_client",
                "plant_code":"$_id.plant_code",
                "year_week":"$year_week",
                # "starter":"$_id.starter",
                "req":"$req",
            }},
            {"$sort": {"plant_code":1,"year_week": 1}}
        ]
        ## TODO:
        # 1.- acomodar por ciclos, obtener el cylco dependiendo de la semana de cultivo o la semana de arranque
        cr_result = self.cr.aggregate(query)

        result = self.get_starters(cr_result)
        req_by_week = self.setup_plants(result, 'stage2', 'req')
        return req_by_week

if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    data = report_obj.data
    test = report_obj.data.get('test')

    # plant_code = data.get("plant_code")
    # plant_code = all_data.get("plant_code")
    # report_model = ReportModel()
    #try:
    if True:
        #jwt_complete = simplejson.loads(sys.argv[2])
        #config["USER_JWT_KEY"] = jwt_complete
        #settings.config.update(config)
        tnow = time.time()
        tnow = datetime.fromtimestamp(tnow)
        yearweek = tnow.strftime('%Y%W')
        cut_year = int(str(yearweek)[:4])
        cut_week =   int(str(yearweek)[-2:])
        plant_rows, response, hours = report_obj.get_report(cut_year, cut_week)
        if not test:
            sys.stdout.write(simplejson.dumps(
                {"firstElement":{
                    'hours':hours
                    },
                "secondElement":{
                    'tabledata':response
                    },
                "thirdElement":{
                    'tabledata':plant_rows
                    },
                "params":{"week":cut_week}
                })
            )
    # except:
    #     sys.stdout.write(simplejson.dumps({"firstElement": {"error":"Something went wrong"}}))
