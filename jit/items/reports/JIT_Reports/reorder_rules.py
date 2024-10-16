# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from linkaform_api import settings
from account_settings import *

print('inicia....')
from jit_report import Reports



if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()
    #-FILTROS
    data = report_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')
    status = data.get('status', '')
    # res_first = get_elements(status)
    # print('info: ', res_first)
    # #-FILTROS
    # data = script_obj.data
    # data = data.get('data',[])
    # option = data.get('option','get_report')
    # wharehouse = data.get('wharehouse', '')
    # familia = data.get('familia', '')
    if option == 'get_report':
        res_first = report_obj.get_elements(status)
    #     '''
    #     script_obj.HttpResponse({
    #         "firstElement":res_first,
    #     })
    #     '''
    # elif option == 'get_catalog':
    #     #res_catalog = get_catalog_wharehouse()
    #     print('Hola catalogo')
    #     '''
    #     script_obj.HttpResponse({
    #         "dataCatalog":res_catalog,
    #     })
    #     '''   