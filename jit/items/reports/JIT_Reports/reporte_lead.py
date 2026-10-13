# -*- coding: utf-8 -*-
import sys, simplejson, math
import json
import math
from datetime import timedelta, datetime

from linkaform_api import settings, base
from account_settings import *

print('inicia....')
from jit_report import Reports


if __name__ == "__main__":
    # res = script_obj.format_cr(script_obj.cr.aggregate(query))
    script_obj = base.LKF_Base(settings, sys_argv=sys.argv, use_api=True)
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    report_obj.console_run()

    data = report_obj.data
    data = data.get('data',[])
    option = data.get('option','get_report')

    # script_obj.HttpResponse({
    #     "stockInfo": stock_list,
    # })