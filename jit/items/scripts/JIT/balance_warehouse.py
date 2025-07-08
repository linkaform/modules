# -*- coding: utf-8 -*-
import sys, simplejson, copy

from jit_utils import JIT
from lkf_addons.addons.base.app import CargaUniversal


from account_settings import *


if __name__ == '__main__':
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    jit_obj.console_run()
    cu_obj = CargaUniversal(settings, sys_argv=sys.argv, use_api=True)

    
    response = jit_obj.balance_warehouse(rule_type='xfer')
    print('response',response)
    print('TODO: revisar si un create no estuvo bien y ponerlo en error o algo')
    res = cu_obj.update_status_record('balanceo_realizado')

