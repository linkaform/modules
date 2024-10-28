# -*- coding: utf-8 -*-
import sys, simplejson, copy

from jit_utils import JIT

from account_settings import *


if __name__ == '__main__':
    print('-------------------------------------------------------------------------')
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<.')
    jit_obj.console_run()
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.')
    response = jit_obj.ave_daily_demand()
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': jit_obj.answers,
        }))
