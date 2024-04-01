#-*- coding: utf-8 -*-

import simplejson, sys
from copy import deepcopy
from linkaform_api import settings, network, utils
from bson import ObjectId
import time
from datetime import datetime, timedelta, date

from magnolia_settings import *
from account_utils import get_plant_recipe, get_plant_names



table_data = []
plants = {}
WEEKS = []
columsTable_title = [
  { "title":"Production Year", "field":'production_year',"hozAlign":"left","width":70},
  { "title":"Week", "field":'week',"hozAlign":"right","width":85},
  { "title":"Week Production: Plant: Plant Code", "field":'plant_code',"hozAlign":"left","width":75},
  { "title":"Week Production: Plant: Plant Name", "field":'plant_name',"hozAlign":"left","width":175},
  { "title":"Plan Required Total", "field":'required',"hozAlign":"right","width":125, "formatter": "money",
    "formatterParams": {"symbol": "", "symbolAfter": "",  "thousand": ",", "precision": 0}}
    ]
col_req ={ "title":"Plan Required Total", "field":'required',"hozAlign":"right","width":125, "formatter": "money",
  "formatterParams": {"symbol": "", "symbolAfter": "",  "thousand": ",", "precision": 0}}

col_stok ={ "title":"Stock Week", "field":'required',"hozAlign":"right","width":75, "formatter": "money",
  "formatterParams": {"symbol": "", "symbolAfter": "",  "thousand": ",", "precision": 0}}

columsTable_stock = [
  # { "title":"Available eaches", "field":'stock_available',"hozAlign":"right","width":75,"formatter": "money",
   # "formatterParams": {"symbol": "", "symbolAfter": "", "thousand": ",", "precision": 0}},
  { "title":"PreviewsWeeks eaches", "field":'stock_previwes',"hozAlign":"right","width":75,"formatter": "money",
   "formatterParams": {"symbol": "", "symbolAfter": "", "thousand": ",", "precision": 0}},
   ]

columsTable_stock_last = [
  { "title":"TotalStock eaches", "field":'stock_total',"hozAlign":"right","width":75,"formatter": "money",
   "formatterParams": {"symbol": "", "symbolAfter": "", "thousand": ",", "precision": 0}},
  { "title":"Week Production: Requierd Eaches Qty", "field":'requierd_eaches_qty',"hozAlign":"right","width":75},
  { "title":"Week Production: Priority", "field":'priority',"hozAlign":"left","width":75},
  { "title":"Week Production: Comments", "field":'comments',"hozAlign":"left","width":75},
]


def get_requierd_plan(yearWeek_from, yearWeek_to):
    global plants, columsTable_title
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":81420,
        "answers.626c2792f7f680727fdba0f5": {"$gte": int(yearWeek_from),"$lte": int(yearWeek_to) }
        }
    query = [
        {"$match": match_query},
        {'$project':
            {'_id': 1,
                'plant_code': '$answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33d',
                "plant_name":{ "$arrayElemAt": ['$answers.6205f7690e752ce25bb30102.61ef32bcdf0ec2ba73dec33e',0]},
                'week': '$answers.626c2792f7f680727fdba0f5',
                'eaches': '$answers.6206b6186c0b3b00535d60d8'}},
        {'$group':
            {'_id':
                {
                  'plant_code': '$plant_code',
                  'week': '$week'
                  },
              'total': {'$sum': '$eaches'}}},
        {'$project':
            {'_id': 0,
            'plant_code': '$_id.plant_code',
            'week':'$_id.week',
            'total': '$total'
            }
        },
        {'$sort': {'plant_code': 1, 'week':1}}
    ]
    result = cr.aggregate(query)
    rweeks = int(yearWeek_to) - int(yearWeek_from) + 1
    req_weeks = {}
    for x in range(rweeks):
        req_weeks['required_{}'.format(int(yearWeek_from) + x)] = 0
        this_col = deepcopy(col_req)
        this_col['field'] = 'required_{}'.format(int(yearWeek_from) + x)
        this_col['title'] = 'Week {}'.format(int(yearWeek_from) + x)
        columsTable_title.append(this_col)
    default_schema = {'available':0, 'previwes':0, 'required':0, 'plant_name':""}
    default_schema.update(req_weeks)
    for r in result:
        pcode = r.get('plant_code')
        week = r.get('week')
        required = r.get('total',0)
        plants[pcode] = plants.get(pcode, deepcopy(default_schema))
        plants[pcode]['required_{}'.format(week)] += required
        plants[pcode]['required'] += required
    return True

def query_get_stock(yearWeek_from, yearWeek_to):
    global plants, col_stok, columsTable_stock
    stage ='S3'
    match_query = {
        "deleted_at":{"$exists":False},
        "form_id":113823,
        "answer.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f1573358": stage,
        "answer.620ad6247a217dbcb888d175":"active",
        # "answers.620a9ee0a449b98114f61d77":  {"$lte": int(yearWeek_to) }
        }
    query= [{'$match': match_query },
        {'$project':
            {'_id': 1,
                'plant_code': '$answers.61ef32bcdf0ec2ba73dec33c.61ef32bcdf0ec2ba73dec33d',
                'plant_name':{ "$arrayElemAt": ['$answers.61ef32bcdf0ec2ba73dec33c.61ef32bcdf0ec2ba73dec33e',0]},
                'planted_yearweek':{"$toInt": '$answers.620a9ee0a449b98114f61d75' },
                'growth_week': {"$toInt": '$answers.61ef32bcdf0ec2ba73dec33c.6205f73281bb36a6f1573357' },
                'eaches': '$answers.620ad6247a217dbcb888d172'}},
        {'$project':
            {'_id': 1,
                'plant_code': '$plant_code',
                'plant_name':'$plant_name',
                'cutweek':{"$sum" :['$planted_yearweek','$growth_week'] },
                'eaches': '$eaches'}},
        {"$match": {'cutweek': {"$lte": int(yearWeek_to) }}},
        {'$group':
            {'_id':
                {
                'plant_code': '$plant_code',
                'plant_name': '$plant_name',
                'cutweek': '$cutweek',
                  },
              'total': {'$sum': '$eaches'}}},
        {'$project':
            {'_id': 0,
            'plant_code': '$_id.plant_code',
            'plant_name': '$_id.plant_name',
            'cutweek': '$_id.cutweek',
            'total': '$total'
            }
        },
        {'$sort': {'plant_code': 1, 'cutweek':1}}
        ]
    res = cr.aggregate(query)
    result = {}

    rweeks = int(yearWeek_to) - int(yearWeek_from) + 1
    req_weeks = {}
    for x in range(rweeks):
        req_weeks['wstock_{}'.format(int(yearWeek_from) + x)] = 0
        this_col = deepcopy(col_req)
        this_col['field'] = 'wstock_{}'.format(int(yearWeek_from) + x)
        this_col['title'] = 'Stock Week {}'.format(int(yearWeek_from) + x)
        columsTable_stock.append(this_col)
    default_schema = {'available':0, 'previwes':0, 'required':0, 'plant_name':""}
    default_schema.update(req_weeks)
    for r in res:
        pcode = r.get('plant_code')
        week = r.get('cutweek')
        plants[pcode] = plants.get(pcode, deepcopy(default_schema))
        plants[pcode]['plant_name'] = r.get('plant_name')
        if week < int(yearWeek_from):
            plants[pcode]['previwes'] += r['total']
        else:
            plants[pcode]['available'] += r['total']
            #if week > int(yearWeek_from):
            plants[pcode]['wstock_{}'.format(week)] = r['total']
    return True

def get_req_keys(row):
    res = {}
    for key, val in row.items():
        if 'required_' in key:
            res[key] = val
        if 'wstock_' in key:
            res[key] = val
    return res

def arrange_info(yearWeek):
    global plants
    res = []
    x = 0
    for pcode, data in plants.items():
        row = {}
        if x == 0:
            row = {
                "production_year":yearWeek[:4],
                "week":yearWeek[-2:]
                }
        row.update({
            "priority":"",
            "plant_code":pcode,
            "plant_name":data.get('plant_name'),
            "required":data.get('required'),
            "stock_available":data.get('available'),
            "stock_previwes":data.get('previwes'),
            "stock_total":data.get('available') + data.get('previwes'),
            "requierd_eaches_qty":"",
            "comments":"",
            })
        row.update(get_req_keys(data))
        res.append(row)
        x += 1
    return res


if __name__ == "__main__":
    # print(sys.argv)
    plants = {}
    all_data = simplejson.loads(sys.argv[2])
    data = all_data.get("data", {})
    test = data.get("test", False)
    #jwt_complete = simplejson.loads(sys.argv[2])
    #config["USER_JWT_KEY"] = jwt_complete
    #settings.config.update(config)
    lkf_api = utils.Cache(settings)
    jwt_parent = lkf_api.get_jwt(api_key="e38ea1b666e739577e7c7c53abce771ae7ead028")
    config["USER_JWT_KEY"] = jwt_parent
    config["JWT_KEY"] = jwt_parent
    # print('jwot', jwt_parent)
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    cr = net.get_collections()
    tnow = time.time()
    tnow = datetime.fromtimestamp(tnow)

    yearWeek_from = data.get("year_week_from", tnow.strftime('%Y%W'))
    tnow = tnow + timedelta(weeks=2)

    yearWeek_to = data.get("year_week_to", tnow.strftime('%Y%W'))

    # yearweek = 202304
    get_requierd_plan(yearWeek_from, yearWeek_to)
    query_get_stock(yearWeek_from, yearWeek_to)
    response = arrange_info(yearWeek_from,)
    if not test:
        sys.stdout.write(simplejson.dumps(
            {"firstElement":{
                'tabledata':response,
                'colsData':columsTable_title + columsTable_stock + columsTable_stock_last,
                }
            }
            )
        )
