# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta

from linkaform_api import settings, network, utils
from account_utils import get_inventory_flow, unlist
from stock_utils import get_product_stock
from account_settings import *

from bson import ObjectId
import time, pytz, math
from pytz import timezone
from datetime import datetime, timedelta, date



def get_formater_first(data_query):
    global report_model
    data = [x for x in data_query]
    list_children = []

    for element in data:
        folio = element.get('folio','')
        plant_code = element.get('plant_code', '')
        ready_date = element.get('ready_date','')
        date_coplete = str(element.get('date',''))
        date_hr = datetime.strptime(date_coplete, "%Y-%m-%d %H:%M:%S")
        date = date_hr.strftime("%Y-%m-%d")
        green_house = element.get('green_house','')
        #ready_dateTwo = element.get('ready_dateTwo', '')
        #flats = element.get('flats',0)
        list_grading = element.get('list_grading',[])
        total_hrs = element.get('total_hrs', 0.0)

        cont = 0
        for grading in list_grading:
            ready_dateTwo = grading.get('64edf8aeffeaaa1febca2a06', 0)
            flats = grading.get('644bf9a04b1761305b080013', 0)
            if cont == 0:
                list_children.append({
                    "plant_code": plant_code,
                    "ready_date": ready_date,
                    "green_house": green_house,
                    "date": date,
                    "ready_dateTwo":ready_dateTwo,
                    "flats": flats,
                    "total_hrs" : total_hrs
                })

            cont += 1
            list_children.append({
                    "plant_code": '',
                    "ready_date": '',
                    "green_house": '',
                    "date": '',
                    "ready_dateTwo":ready_dateTwo,
                    "flats": flats,
                    "total_hrs" : ''
                })

        report_model.json['firstElement']['data'] = list_children

def query_report_first(date_from, date_to):
    global report_model

    match_query = {
        "form_id":99536,
        "deleted_at":{"$exists":False}
    }

    if date_from or date_to:
        match_query.update(get_date_query(date_from=date_from, date_to=date_to, date_field_id='start_date'))

    query = [{'$match': match_query},
             {'$project':{
                '_id':0,
                'folio': '$folio',
                'plant_code': '$answers.6442cbafb1b1234eb68ec178.61ef32bcdf0ec2ba73dec33d',
                'date': '$answers.000000000000000000000111',
                'green_house': '$answers.6442cbafb1b1234eb68ec178.6442e4831198daf81456f274',
                'ready_date': '$answers.6442cbafb1b1234eb68ec178.620a9ee0a449b98114f61d77',
                'list_grading': '$answers.644bf7ccfa9830903f087867',
                'total_hrs': '$answers.61f1fcf8c66d2990c8fc7cc7'
             }},
             {'$sort': {'ready_date':1}}
    ]

    res = cr.aggregate(query)
    get_formater_first(res)

def get_formater_second(data_query):
    global report_model
    
    data = [x for x in data_query]

    list_date_cutter = {}
    for element in data:
        date_coplete = str(element.get('date',''))
        date_hr = datetime.strptime(date_coplete, "%Y-%m-%d %H:%M:%S")
        date = date_hr.strftime("%Y-%m-%d")
        cutter = element.get('cutter','')
        list_grading = element.get('list_grading', [])
        total_hrs = element.get('total_hrs', 0)
        
        list_flats = []

        for grading in list_grading:
            flats = grading.get('644bf9a04b1761305b080013', 0)
            list_flats.append(flats)
        
        total_flast = sum(list_flats)

        key = date + '_' + cutter

        if (list_date_cutter.get(key) == None):
            avg = round((total_flast / total_hrs), 1)
            list_date_cutter[key] = {
                'date': date,
                'cutter': cutter,
                'flats_graded': total_flast,
                'total_hrs': total_hrs,
                'flats_hrs': avg
            }

        else:
            print('Datos repetidos')
            second_child = list_date_cutter.get(key)
            flats_second = second_child.get('flats_graded')
            total_hrs_second = second_child.get('total_hrs')
            flats_hrs_total = second_child.get('flats_hrs')

            flats_second = flats_second + total_flast
            total_hrs_second = total_hrs_second + total_hrs
            flats_hrs_total = round((flats_second / total_hrs_second), 1)

            list_date_cutter[key] = {
                'date': date,
                'cutter': cutter,
                'flats_graded': flats_second,
                'total_hrs': total_hrs_second,
                'flats_hrs': flats_hrs_total
            }

    for value in list_date_cutter.values():
        report_model.json['secondElement']['data'].append(value)

    #----FORMATEO DEL PRIMER GRAFICO
    labels = []
    list_flats = []
    list_hours = []
    for element in report_model.json['secondElement']['data']:
        date = element.get('date','')
        flats = element.get('flats_graded',0)
        hrs = element.get('total_hrs',0.0)

        labels.append(date)
        list_flats.append(flats)
        list_hours.append(hrs)

    report_model.json['thirdElement']['data'] = {
        'labels': labels,
        'datasets': [
            {
                'type': 'bar',
                'label': 'Flats',
                'data': list_flats,
                'backgroundColor': "rgba(52, 152, 219, 0.5)",
                'borderColor': "#21618c",
            },
            {
                'type': 'line',
                'label': 'Hours',
                'data': list_hours,
                'backgroundColor': "#EF6262",
                'borderColor': "#EF6262",
            },
        ]
    }

    #----FORMATEO DEL SEGUNDO GRÁFICO
    colores = [
    "#d94052", "#ee7e4c", "#ead56c", "#bb8fce", "#a9cce3", "#a3e4d7", "#d4ac0d",
    "#8BC34A", "#FF4081", "#03A9F4", "#FF5722", "#673AB7", "#795548", "#CDDC39",
    "#F50057", "#3F51B5", "#FFC107", "#9E9E9E", "#009688", "#FFEB3B", "#FF6E40",
    "#009688", "#4CAF50", "#FF4081", "#3F51B5", "#FF9800", "#E91E63", "#673AB7",
    "#FF5252", "#8BC34A", "#9C27B0", "#607D8B", "#2196F3", "#CDDC39", "#F50057",
    "#FFC107", "#795548", "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#673AB7",
    "#FF6E40", "#E91E63", "#4CAF50", "#FF4081", "#607D8B", "#FF9800", "#3F51B5",
    "#2196F3", "#9C27B0", "#CDDC39", "#673AB7", "#795548", "#F50057", "#FFC107",
    "#03A9F4", "#FF5722", "#FFEB3B", "#009688", "#FF6E40", "#E91E63", "#4CAF50",
    "#FF4081", "#2196F3", "#FF9800", "#3F51B5", "#607D8B", "#CDDC39", "#9C27B0",
    "#673AB7", "#795548", "#FF5722", "#03A9F4", "#FFEB3B", "#FFC107", "#009688",
    "#E91E63", "#FF6E40", "#4CAF50", "#FF4081", "#2196F3", "#FF9800", "#9C27B0",
    "#607D8B", "#673AB7", "#795548", "#03A9F4", "#FF5722", "#CDDC39", "#009688",
    "#FFEB3B", "#F50057", "#E91E63", "#4CAF50", "#2196F3", "#FF4081", "#FF9800",
    "#673AB7", "#9C27B0", "#03A9F4", "#607D8B", "#FF5722", "#CDDC39", "#795548",
    "#009688", "#FFEB3B", "#E91E63", "#4CAF50", "#FF4081", "#2196F3", "#FF9800",
    "#3F51B5", "#9C27B0", "#607D8B", "#CDDC39", "#673AB7", "#03A9F4", "#795548",
    "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50", "#FF4081", "#2196F3",
    "#FF9800", "#9C27B0", "#CDDC39", "#673AB7", "#607D8B", "#795548", "#03A9F4",
    "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50", "#FF4081", "#2196F3",
    "#FF9800", "#3F51B5", "#9C27B0", "#607D8B", "#CDDC39", "#673AB7", "#03A9F4",
    "#795548", "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50", "#FF4081",
    "#2196F3", "#FF9800", "#9C27B0", "#CDDC39", "#607D8B", "#673AB7", "#795548",
    "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50", "#FF4081",
    "#2196F3", "#FF9800", "#3F51B5", "#9C27B0", "#CDDC39", "#673AB7", "#607D8B",
    "#795548", "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50",
    "#FF4081", "#2196F3", "#FF9800", "#9C27B0", "#CDDC39", "#673AB7", "#607D8B",
    "#795548", "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#E91E63", "#4CAF50",
    "#FF4081", "#2196F3", "#FF9800", "#3F51B5", "#9C27B0", "#CDDC39", "#673AB7",
    "#795548", "#607D8B", "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#E91E63",
    "#4CAF50", "#FF4081", "#2196F3", "#FF9800", "#9C27B0", "#CDDC39", "#673AB7",
    "#607D8B", "#795548", "#03A9F4", "#FF5722", "#009688", "#FFEB3B", "#E91E63",
    "#4CAF50", "#FF4081", "#2196F3", "#FF9800", "#3F51B5", "#9C27B0", "#CDDC39",
    "#673AB7", "#607D8B", "#795548", "#03A9F4", "#FF5722", "#009688", "#FFEB3B",
    "#E91E63", "#4CAF50", "#FF4081", "#2196F3", "#FF9800", "#9C27B0", "#CDDC39",
    "#673AB7", "#795548", "#607D8B", "#03A9F4", "#FF5722", "#009688", "#FFEB3B",
    "#E91E63", "#4CAF50", "#FF4081", "#2196F3", "#FF9800", "#3F51B5", "#9C27B0",
    "#CDDC39", "#673AB7", "#607D8B", "#795548", "#03A9F4", "#FF5722", "#009688",
    "#FFEB3B", "#E91E63"
]
    labels = []
    data_hrs = []
    list_cutter = []
    background = []
    cont_colors = 0
    for element in report_model.json['secondElement']['data']:
        cutter = element.get('cutter','')
        hrs = element.get('total_hrs',0)
        hrs = round(hrs,1)
        if cutter not in labels:
            labels.append(cutter)
            data_hrs.append(hrs)
        else:
            indice = labels.index(cutter)
            hrs_second = data_hrs[indice]
            hrs_second += hrs
            data_hrs[indice] = round(hrs_second, 1)

    cont_colors = len(labels) #Contador de elementos
    background = colores[:cont_colors] #Selecciona n números de elementos de la lista de colores

    report_model.json['fiveElement']['data'] = {
        'labels': labels,
        'datasets': [
            {
                'label': 'Flats',
                'data': data_hrs,
                'background': background,
            },
        ]
        }
    

def query_report_second(date_from, date_to):
    global report_model

    match_query = {
        "form_id":99536,
        "deleted_at":{"$exists":False}
    }

    if date_from or date_to:
        match_query.update(get_date_query(date_from=date_from, date_to=date_to, date_field_id='start_date'))

    query = [{'$match': match_query},
             {'$project':{
                '_id':0,
                'folio': '$folio',
                'date': '$answers.000000000000000000000111',
                'cutter': '$answers.62c5ff243c63280985580087.62c5ff407febce07043024dd',
                'list_grading': '$answers.644bf7ccfa9830903f087867',
                'total_hrs': '$answers.61f1fcf8c66d2990c8fc7cc7'
             }},
             {'$sort': {'date':1}}
    ]

    res = cr.aggregate(query)
    get_formater_second(res)

if __name__ == "__main__":
    print(sys.argv)
    all_data = simplejson.loads(sys.argv[2])

    #----FILTERS
    data = all_data.get('data', {})
    date_from = data.get('date_from','')
    date_to = data.get('date_to', '')

    lkf_api = utils.Cache(settings)
    jwt_parent = lkf_api.get_jwt(api_key=config['API_KEY'])
    config['USER_JWT_KEY'] = jwt_parent
    config['JWT_KEY'] = jwt_parent
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    report_model = ReportModel()
    cr = net.get_collections()

    if date_from or date_to:
        query_report_first(date_from, date_to)
        query_report_second(date_from, date_to)
        sys.stdout.write(simplejson.dumps(report_model.print()))