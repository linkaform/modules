#-*- coding: utf-8 -*-
import sys, simplejson, time
from datetime import datetime, timedelta, date
from linkaform_api import settings, network, utils, base
from account_settings import *
from calendar import month_name
import calendar

meses_es = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre"
}

class ReportModel():
    def __init__(self):
        self.json = {
            "firstElement":{},
            "secondElement":{},
            "thirdElement":{},
            "fourthElement":{},
            "fifthElement":{},
        }

    def print(self):
        res = {'json':{}}
        for x in self.json:
            res['json'][x] = self.json[x]
        return res
    
def process_dates(date_from_str, date_to_str):
    """
    Convierte las fechas de formato "YYYY-MM" a fechas con primer y último día del mes.
    """
    date_from = datetime.strptime(date_from_str, "%Y-%m")
    date_to = datetime.strptime(date_to_str, "%Y-%m")
    
    start_date = datetime(date_from.year, date_from.month, 1)
    
    last_day_of_month = calendar.monthrange(date_to.year, date_to.month)[1]
    end_date = datetime(date_to.year, date_to.month, last_day_of_month, 23, 59, 59)
    
    print(start_date, end_date)
    return start_date, end_date
    
def get_format_firstElement(query_results):
    print('Formateando informacion del primer elemento...')
    data_table = {}

    for result in query_results:
        year = result['_id']['year']
        month = result['_id']['month']
        
        if year not in data_table:
            data_table[year] = {"mes": str(year), "_children": []}

        month_name_str = month_name[month]

        month_data = {
            "mes": month_name_str,
            "leads_generados": result["leads_generados"],
            "leads_calificados": result["leads_calificados"],
            "demos": result["leads_demos"],
            "leads_ganados": result["leads_ganados"],
            "porcentaje_demos": f'{result["porcentaje_demos_x_lead"]}',
            "porcentaje_leads_calificados": f'{result["porcentaje_leads_calificados"]}',
            "porcentaje_cierre": f'{result["porcentaje_cierre"]}',
            "licencias_en_proceso": result.get("licencias_en_proceso", 0),
            "sp_en_proceso": result.get("sp_en_proceso", 0),
            "licencias_vendidas": result.get("licencias_vendidas", 0),
            "sp_vendidos": result.get("sp_vendidos", 0),
            "clientes": result.get("empresas", [])
        }

        data_table[year]["_children"].append(month_data)

    # print(list(data_table.values()))
    transformed_data = list(data_table.values())

    for year_data in transformed_data:
        for month_data in year_data["_children"]:
            month_data["mes"] = meses_es.get(month_data["mes"], month_data["mes"])

    # print(simplejson.dumps(transformed_data, indent=4))
    return transformed_data

def get_format_secondElement(query_results):
    print('Formateando informacion del segundo elemento...')
    data_table = []

    for result in query_results:
        month = result['_id']['month']
        
        month_name_str = meses_es.get(month, month)

        porcentaje_si = result['porcentaje_leads_calificados_si']
        porcentaje_no = result['porcentaje_leads_calificados_no']
        
        month_data = {
            "mes": month_name_str,
            "anio": result["_id"]["year"],
            "porcentaje_si": porcentaje_si,
            "porcentaje_no": porcentaje_no
        }

        data_table.append(month_data)

    return data_table

def get_format_porcentaje_cierre(query_results):
    print('Formateando informacion del porcentaje de cierre...')
    data_table = []

    for result in query_results:
        month = result['_id']['month']
        month_name_str = meses_es.get(month, month)

        porcentaje_cierre = result['porcentaje_cierre']

        month_data = {
            "mes": month_name_str,
            "anio": result["_id"]["year"],
            "porcentaje_cierre": porcentaje_cierre
        }

        data_table.append(month_data)

    return data_table

def get_format_cantidad_cuentas_cerradas(query_results):
    print('Formateando informacion de la cantidad cuentas cerradas...')
    data_table = []

    for result in query_results:
        month = result['_id']['month']
        month_name_str = meses_es.get(month, month)

        cantidad_cuentas_cerradas = result['leads_ganados']

        month_data = {
            "mes": month_name_str,
            "anio": result["_id"]["year"],
            "cantidad_cuentas_cerradas": cantidad_cuentas_cerradas
        }

        data_table.append(month_data)

    return data_table

def get_format_ventas_mensuales(query_results):
    print('Formateando informacion de la cantidad cuentas cerradas...')
    data_table = []

    for result in query_results:
        month = result['_id']['month']
        month_name_str = meses_es.get(month, month)

        licencias_vendidas = result['licencias_vendidas']
        sp_vendidos = result['sp_vendidos']

        month_data = {
            "mes": month_name_str,
            "anio": result["_id"]["year"],
            "licencias_vendidas": licencias_vendidas,
            "sp_vendidos": sp_vendidos,
        }

        data_table.append(month_data)

    return data_table

def calcular_indicadores_en_leads(leads_data, costos_ventas_mensuales):
    print("Agregando indicadores de ventas a los leads...")

    costos_indexados = {
        (registro['anio'], registro['mes'].capitalize()): {
            'costo_venta': registro['costo_venta'],
            'tipo_cambio': registro['tipo_cambio']
        }
        for registro in costos_ventas_mensuales
    }

    print("Índice de costos de ventas:", costos_indexados)

    for anio_data in leads_data:
        anio = str(anio_data['mes'])
        meses = anio_data['_children']

        for mes_data in meses:
            mes_nombre = mes_data["mes"].capitalize()

            licencias_vendidas = mes_data.get("licencias_vendidas", 0)

            costos = costos_indexados.get((anio, mes_nombre), None)

            if costos is None:
                print(f"Advertencia: No se encontró costo de venta y tipo de cambio para el año {anio} y mes {mes_nombre}")
                mes_data["ltv_cac"] = 0
                continue

            costo_venta = costos['costo_venta']
            tipo_cambio = costos['tipo_cambio']

            print(f"Costo de venta encontrado: {costo_venta} y tipo de cambio: {tipo_cambio} para el mes {mes_nombre} y año {anio}")

            indicador_ventas = (licencias_vendidas * 36) * tipo_cambio

            indicador_ventas_mxn = round(indicador_ventas / costo_venta, 2) if costo_venta > 0 else 0

            mes_data["ltv_cac"] = round(indicador_ventas_mxn, 2)

    return leads_data

def query_report_first(date_from=None, date_to=None):
    print('Entra a query_report_first')
    global report_model
    res = {}

    match_query_report = {
        "form_id": 101171,
        "deleted_at": {"$exists": False},
    }

    if date_from and date_to:
        start_date, end_date = process_dates(date_from, date_to)
        match_query_report['created_at'] = {
            '$gte': start_date,
            '$lte': end_date
        }
    
    proyect_fields_report = {
        '_id': 0,
        'created_at': 1,
        'answers.651ed6024c947a9a1c81d70e': 1,
        'answers.676d8e8a08e7b06a94055ec4': 1,
        'answers.60775e95eeef08e482d669b9': 1,
        'answers.60775dc5ad862f384d520048': 1,
        'answers.6298e6b01400fe678f781c49': 1,
        'answers.6298e6b01400fe678f781c4a': 1,
    }

    group_by_report = {
        '_id': {
            'year': {'$year': "$created_at"},
            'month': {'$month': "$created_at"},
        },
        'leads_generados': {'$sum': 1},
        'leads_demos': {
            '$sum': {
                '$reduce': {
                    'input': {
                        '$filter': {
                            'input': '$answers.651ed6024c947a9a1c81d70e',
                            'as': 'obj',
                            'cond': {'$eq': ['$$obj.651ed6245eeb9fb94bcfb5b4', 'cita_para_demo']}
                        }
                    },
                    'initialValue': 0,
                    'in': {'$add': ['$$value', 1]}
                }
            }
        },
        'leads_calificados': {
            '$sum': {
                '$cond': [
                    {'$eq': ['$answers.676d8e8a08e7b06a94055ec4', 'sí']},
                    1,
                    0
                ]
            }
        },
        'leads_ganados': {
            '$sum': {
                '$cond': [
                    {'$eq': ['$answers.60775e95eeef08e482d669b9', 'ganado']},
                    1,
                    0
                ]
            }
        },
        'licencias_proceso': {
            '$sum': {
                '$cond': [
                    {'$in': ['$answers.60775e95eeef08e482d669b9', ['cotizacion', 'cocinando', 'pruebas', 'toma_de_decisiones']]},
                    '$answers.6298e6b01400fe678f781c49',
                    0
                ]
            }
        },
        'sp_en_proceso': {
            '$sum': {
                '$cond': [
                    {'$in': ['$answers.60775e95eeef08e482d669b9', ['cotizacion', 'cocinando', 'pruebas', 'toma_de_decisiones']]},
                    '$answers.6298e6b01400fe678f781c4a',
                    0
                ]
            }
        },
        'licencias_vendidas': {
            '$sum': {
                '$cond': [
                    {'$eq': ['$answers.60775e95eeef08e482d669b9', 'ganado']},
                    '$answers.6298e6b01400fe678f781c49',
                    0
                ]
            }
        },
        'sp_vendidos': {
            '$sum': {
                '$cond': [
                    {'$eq': ['$answers.60775e95eeef08e482d669b9', 'ganado']},
                    '$answers.6298e6b01400fe678f781c4a',
                    0
                ]
            }
        },
        'empresas': {
            '$addToSet': {
                '$cond': [
                    {'$eq': ['$answers.60775e95eeef08e482d669b9', 'ganado']},
                    '$answers.60775dc5ad862f384d520048',
                    None
                ]
            }
        }
    }

    query_visitas = [
        {'$match': match_query_report},
        {'$project': proyect_fields_report},
        {'$group': group_by_report},
        {'$project': {
            'empresas': {
                '$filter': {
                    'input': '$empresas',
                    'as': 'empresa',
                    'cond': {'$ne': ['$$empresa', None]}
                }
            },
            'leads_generados': 1,
            'leads_demos': 1,
            'leads_calificados': 1,
            'leads_ganados': 1,
            'licencias_proceso': 1,
            'sp_en_proceso': 1,
            'licencias_vendidas': 1,
            'sp_vendidos': 1,
        }},
        {'$sort': {'_id.year': 1, '_id.month': 1}},
        # {'$limit': 10}
    ]

    resultado = list(script_obj.cr.aggregate(query_visitas))

    for result in resultado:
        result['porcentaje_demos_x_lead'] = round((result['leads_demos'] / result['leads_generados']) * 100, 2)
        result['porcentaje_leads_calificados'] = round((result['leads_calificados'] / result['leads_generados']) * 100, 2)
        result['porcentaje_cierre'] = round((result['leads_ganados'] / result['leads_generados']) * 100, 2)
        result['licencias_proceso'] = round(result['licencias_proceso'] / 12, 2)
        result['licencias_vendidas'] = round(result['licencias_vendidas'] / 12, 2)
        # print(result)

    resultado_formateado = get_format_firstElement(resultado)
    cuentas_cerradas_formateado = get_format_cantidad_cuentas_cerradas(resultado)
    ventas_menuales = get_format_ventas_mensuales(resultado)
    costo = query_costo_ventas_mensuales()
    resultado_formateado_mas_ltcv_cac = calcular_indicadores_en_leads(resultado_formateado, costo)
    # report_model.json['firstElement']['data'] = resultado_formateado

    # print(porcentaje_formateado)
    report_model.json['firstElement']['data'] = resultado_formateado_mas_ltcv_cac
    report_model.json['fourthElement']['data'] = ventas_menuales
    report_model.json['fifthElement']['data'] = cuentas_cerradas_formateado


def query_report_second(date_from=None, date_to=None):
    print('Entra a query_report_second')

    match_query_report = {
        "form_id": 101171,
        "deleted_at": {"$exists": False},
    }

    if date_from and date_to:
        start_date, end_date = process_dates(date_from, date_to)
        match_query_report['created_at'] = {
            '$gte': start_date,
            '$lte': end_date
        }

    proyect_fields_report = {
        '_id': 0,
        'answers.60775e95eeef08e482d669b9': 1,
    }

    group_by_report = {
        '_id': '$answers.60775e95eeef08e482d669b9',
        'conteo': {'$sum': 1},
    }

    query_visitas = [
        {'$match': match_query_report},
        {'$project': proyect_fields_report},
        {'$group': group_by_report},
        {'$sort': {'conteo': -1}},
    ]

    resultado = list(script_obj.cr.aggregate(query_visitas))

    # for result in resultado:
        # print(result)

    report_model.json['secondElement']['data'] = resultado

def query_report_third(date_from=None, date_to=None):
    print('Entra a query_report_third')
    global report_model
    res = {}

    match_query_report = {
        "form_id": 101171,
        "deleted_at": {"$exists": False},
    }

    if date_from and date_to:
        start_date, end_date = process_dates(date_from, date_to)
        match_query_report['created_at'] = {
            '$gte': start_date,
            '$lte': end_date
        }
    
    proyect_fields_report = {
        '_id': 0,
        'created_at': 1,
        'answers.6131531eff7840189a8a0752': 1,
        'answers.676d8e8a08e7b06a94055ec4': 1,
        'answers.60775e95eeef08e482d669b9': 1,
        'answers.60775dc5ad862f384d520048': 1,
    }

    group_by_report = {
        '_id': {
            'year': {'$year': "$created_at"},
            'month': {'$month': "$created_at"},
        },
        'leads_generados': {'$sum': 1},
        'leads_calificados': {
            '$sum': {
                '$cond': [
                    {'$eq': ['$answers.676d8e8a08e7b06a94055ec4', 'sí']},
                    1,
                    0
                ]
            }
        },
    }

    query_visitas = [
        {'$match': match_query_report},
        {'$project': proyect_fields_report},
        {'$group': group_by_report},
        {'$sort': {'_id.year': 1, '_id.month': 1}},
        # {'$limit': 10}
    ]

    resultado = list(script_obj.cr.aggregate(query_visitas))

    for result in resultado:
        result['porcentaje_leads_calificados_si'] = round((result['leads_calificados'] / result['leads_generados']) * 100, 2)
        result['porcentaje_leads_calificados_no'] = round(100 - result['porcentaje_leads_calificados_si'], 2)
        # print(result)

    resultado_formateado = get_format_secondElement(resultado)
    # print(resultado_formateado)
    report_model.json['thirdElement']['data'] = resultado_formateado

def query_costo_ventas_mensuales():
    print('Entra a query_costo_ventas_mensuales')
    global report_model

    match_query_report = {
        "form_id": 127630,
        "deleted_at": {"$exists": False},
    }

    proyect_fields_report = {
        '_id': 0,
        'answers.67780704b7895012fda1eb8a': 1,
        'answers.67780704b7895012fda1eb89': 1,
        'answers.677806c104a25414576c9868': 1,
        'answers.677806c104a25414576c9869': 1,
    }

    group_by_report = {
        '_id': None,
        'registros': {
            '$push': {
                'anio': '$answers.67780704b7895012fda1eb8a',
                'mes': '$answers.67780704b7895012fda1eb89',
                'costo_venta': '$answers.677806c104a25414576c9868',
                'tipo_cambio': '$answers.677806c104a25414576c9869',
            }
        }
    }

    query_visitas = [
        {'$match': match_query_report},
        {'$project': proyect_fields_report},
        {'$group': group_by_report},
    ]

    resultado = list(script_obj.cr.aggregate(query_visitas))

    for result in resultado:
        print(result)
    return resultado[0]['registros']

if __name__ == "__main__":
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    all_data = simplejson.loads(sys.argv[2])
    data = all_data.get("data", {})
    date_from = data.get("date_from")
    date_to = data.get("date_to")
    print('///////////////////', date_from, date_to)

    report_model = ReportModel()
    jwt_complete = simplejson.loads(sys.argv[2])
    config["USER_JWT_KEY"] = jwt_complete
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    net = network.Network(settings)
    cr = net.get_collections()
    if date_from and date_to:
        res = query_report_first(date_from=date_from, date_to=date_to)
        res_second = query_report_second(date_from=date_from, date_to=date_to)
        res_third = query_report_third(date_from=date_from, date_to=date_to)
    else:
        res = query_report_first()
        res_second = query_report_second()
        res_third = query_report_third()
    # query_report_first(date_from=date_from, date_to=date_to)
    #Debug -- 
    # Imprimir los resultados
    # print(report_model.print())
    # sys.stdout.write(simplejson.dumps(report_model.print()))

    script_obj.HttpResponse({
        "dataReport": report_model.json,
    })


