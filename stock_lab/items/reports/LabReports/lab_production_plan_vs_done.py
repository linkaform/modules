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


def get_plan_by_plant(produced_plants):
    # stage ='pull'
    global cut_week, cut_year
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":80815,
        "answers.61f1da41b112fe4e7fe8582f":cut_year,
        "answers.61f1da41b112fe4e7fe85830":cut_week,
        "answers.62e4bd2ed9814e169a3f6bef":'execute', #status
        }
    query= [
        {'$match': match_query },
        {'$unwind':'$answers.62e4babc46ff76c5a6bee76c'}]
    query += [
        {'$project':
            {'_id': 1,
                'plant_code': '$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.61ef32bcdf0ec2ba73dec33d',
                'plant_name': {'$arrayElemAt':['$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.61ef32bcdf0ec2ba73dec33e',0]},
                'container': '$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.6209705080c17c97320e3382',
                'stage': '$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f1573358',
                'plants':{ '$multiply':[
                    '$answers.62e4babc46ff76c5a6bee76c.62e4bc58d9814e169a3f6beb',
                    '$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f157335b']}
                }},
        ]
    query += [
        {'$group':
            {'_id':
                { 'plant_code': '$plant_code',
                  'plant_name': '$plant_name',
                  'container': '$container',
                  'stage': '$stage',
                  },
              'total': {'$sum': '$plants'}}},
        {'$project':
            {'_id': 0,
            'plant_code': '$_id.plant_code',
            'plant_name': '$_id.plant_name',
            'container': '$_id.container',
            'stage': '$_id.stage',
            'total': '$total'
            }
        },
        {'$sort': { 'plant_code': 1, 'stage': 1, 'total':1 }}
        ]
    res = cr.aggregate(query)
    # result = [r for r in res]
    for r in res:
        code = r.get('plant_code')
        pname = r.get('plant_name','')
        stage = int(r.get('stage')[1])
        container = r.get('container','').replace('_',' ').title()
        produced_plants[code] = produced_plants.get(code,{2:{},3:{}, 'plant_name':pname })
        produced_plants[code][stage] = produced_plants[code].get(stage, {})
        produced_plants[code][stage][container] = produced_plants[code][stage].get(container, {'produced':0, 'planned':0})
        produced_plants[code][stage][container]['planned'] = r.get('total',0)
    return produced_plants

def available_hours(yearweek=None):
    # stage ='pull'
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":94948,
        }

    if yearweek:
        match_query.update({"answers.63d06a8f01fe398a70e4166f":int(yearweek)})
    else:
        match_query.update({"answers.63d06a8f01fe398a70e4166f":{"$exists":False}})
    print('match_query=',simplejson.dumps(match_query))
    query = [
        {'$match': match_query },
        {'$project':
            {'_id': 1,
                'team': '$answers.62c5ff0162a70c261328845c.62c5ff0162a70c261328845d',
                'hours': '$answers.63d06a8f01fe398a70e4166e',
                }
        },
        {'$group':
            {'_id':
                { 'team': '$team',
                  },
              'hours': {'$sum': '$hours'}}
          },
          {'$project':
            {'_id': 0,
            'team': '$_id.team',
            'hours': '$hours',
          }
          },
            {'$sort': {'team': 1, 'hours': 1}}
        ]
    print('match_query=',simplejson.dumps(query))
    res = cr.aggregate(query)
    result = [r for r in res]
    return result

def get_produced_by_plant():
    global cut_week, cut_year, from_week, to_week
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":87499,
        "answers.620a9ee0a449b98114f61d75": {"$gte": int(cut_year),"$lte": int(cut_year) },
        "answers.622bb9946d0d6fef17fe0842": {"$gte": int(from_week),"$lte": int(to_week) },
        }
    query= [{'$match': match_query },
        # {'$unwind':'$answers.61f1fab3ce39f01fe8a7ca8c'},
        {'$project':
            {   '_id': 0,
                'year': '$answers.620a9ee0a449b98114f61d75',
                'week': '$answers.622bb9946d0d6fef17fe0842',
                'stage': '$answers.621007e60718d93b752312c4',
                'container': '$answers.620ad6247a217dbcb888d16f',
                'plant_code': '$answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33d',
                'plant_name': {'$arrayElemAt':['$answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33e',0]},
                'eaches': '$answers.620ad6247a217dbcb888d172'}
                },
        {'$group':
            {'_id':
                {
                'plant_code': '$plant_code',
                'plant_name': '$plant_name',
                'container': '$container',
                'stage': '$stage',
                  },
              'total': {'$sum': '$eaches'}}},
        {'$project':
            {
                '_id':0,
                'plant_code':'$_id.plant_code',
                'plant_name':'$_id.plant_name',
                'container':'$_id.container',
                'stage':'$_id.stage',
                'total':'$total',
            }
        },
        {'$sort': {'team': 1,'stage':1 }}
        ]
    # print('query=', simplejson.dumps(query))
    res = cr.aggregate(query)
    result = {}
    for r in res:
        code = r.get('plant_code')
        pname = r.get('plant_name','')
        stage = r.get('stage','')
        container = r.get('container','').replace('_',' ').title()
        result[code] = result.get(code,{2:{},3:{}, 'plant_name':pname })
        result[code][stage] = result[code].get(stage, {})
        result[code][stage][container] = result[code][stage].get(container, {'produced':0, 'planned':0})
        result[code][stage][container]['produced'] = r.get('total',0)
    return result


def get_worked_hours():
    now = datetime.now()
    monday = now - timedelta(days = now.weekday())
    sunday = monday + timedelta(days = 6)

    global cut_week, cut_year, from_week, to_week
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":80817,
        # "$or":
        #     [{"answers.61f1da41b112fe4e7fe8582f": {"$gte": int(cut_year),"$lte": int(cut_year) }},
        #     {"answers.61f1da41b112fe4e7fe8582f": {"$gte": str(cut_year),"$lte": str(cut_year) }},
        # ],
        # "$or":
        #     [{"answers.62e8343e236c89c216a7cec3": {"$gte": int(from_week),"$lte": int(to_week) }},
        #     {"answers.62e8343e236c89c216a7cec3": {"$gte": str(from_week),"$lte": str(to_week) }},
        # ],
        }
    query= [{'$match': match_query },
        {'$unwind':'$answers.61f1fab3ce39f01fe8a7ca8c'},
        {'$match':{'answers.61f1fab3ce39f01fe8a7ca8c.61f1fcf8c66d2990c8fc7cc4':
            {
            "$gte": monday.strftime('%Y-%m-%d'),
            "$lte": sunday.strftime('%Y-%m-%d')
        }}},
        {'$project':
            {   '_id': 1,
                'folio':'$folio',
                'stage': '$answers.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f1573358',
                'team': '$answers.62c5ff0162a70c261328845c.62c5ff0162a70c261328845d',
                'hours': '$answers.61f1fab3ce39f01fe8a7ca8c.61f1fcf8c66d2990c8fc7cc7',
                }
                },
        {'$group':
            {'_id':
                {
                'team': '$team',
                'stage': '$stage',
                  },
              'total': {'$sum': '$hours'}}},
        {'$project':
            {
                '_id':0,
                'team':'$_id.team',
                'stage':'$_id.stage',
                'hours':'$total',
            }
        },
        {'$sort': {'team': 1,'stage':1 }}
        ]
    res = cr.aggregate(query)
    result = [r for r in res]
    return result

def get_estimated_hours(yearweek):
    global cut_week, cut_year, from_week, to_week
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":80815,
        "answers.61f1da41b112fe4e7fe8582f": {"$gte": int(cut_year),"$lte": int(cut_year) },
        "answers.61f1da41b112fe4e7fe85830": {"$gte": int(from_week),"$lte": int(to_week) },
        }
    query= [{'$match': match_query },
        {'$unwind':'$answers.62e4babc46ff76c5a6bee76c'},
        {'$project':
            {   '_id': 0,
                'year': '$answers.61f1da41b112fe4e7fe8582f',
                'week': '$answers.61f1da41b112fe4e7fe85830',
                'stage': '$answers.62e4babc46ff76c5a6bee76c.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f1573358',
                'team': '$answers.62e4babc46ff76c5a6bee76c.62c5ff0162a70c261328845c.62c5ff0162a70c261328845d',
                'hours': '$answers.62e4babc46ff76c5a6bee76c.ab000000000000000000a111',
                }
            },
        {'$group':
            {'_id':
                {
                'team': '$team',
                'stage': '$stage',
                  },
              'total': {'$sum': '$hours'}}},
        {'$project':
            {
                '_id':0,
                'team':'$_id.team',
                'stage':'$_id.stage',
                'hours':'$total',
            }
        },
        {'$sort': {'team': 1,'stage':1 }}
        ]

    res = cr.aggregate(query)
    result = {}
    for r in res:
        team = r.get('team')
        stage = r.get('stage')
        result[team] = result.get(team, {'S2':0, 'S3':0,'total':0})
        result[team][stage] = r.get('hours',0)
        result[team]['total'] = result[team]['S2'] + result[team]['S3']
    return result

def arrange_info(planned, produced):
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

def week_hours(hours, hours_w, worked_hours, estimated_hours):
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

def plants_table(produced_plants):
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

def get_report(cut_year, cut_week):
    global plants, WEEKS, yearweek

    to_week  = cut_week
    from_week = cut_week
    planned = report_obj.get_production_plan(cut_year, cut_week)
    produced = report_obj.get_produced(cut_week, cut_year, from_week, to_week)
    produced_plants = get_produced_by_plant()
    produced_plants = get_plan_by_plant(produced_plants)
    plant_rows = plants_table(produced_plants)
    hours = available_hours()
    hours_w = available_hours(yearweek)
    worked_hours = get_worked_hours()
    estimated_hours = get_estimated_hours(yearweek)
    hours = week_hours(hours, hours_w, worked_hours, estimated_hours)
    res = arrange_info(planned, produced,)
    return plant_rows, res , hours



if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    data = report_obj.data

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

        plant_rows, response, hours = get_report(cut_year, cut_week)
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
