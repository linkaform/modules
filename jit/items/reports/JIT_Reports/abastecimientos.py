# -*- coding: utf-8 -*-
import sys, simplejson, math
from datetime import timedelta, datetime

from jit_report import Reports

from account_settings import *

if __name__ == '__main__':
    print('starting abastesimientos', sys.argv)
    print('aaaaaaaaaaaaaaaaaaaaaaaa1111111111111111111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    report_obj = Reports(settings, sys_argv=sys.argv)
    report_obj.console_run()
    data = report_obj.data.get('data',{})
    print('data', data)
    res = {}
    sys.stdout.write(simplejson.dumps(res))

