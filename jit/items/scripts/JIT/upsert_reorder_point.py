# -*- coding: utf-8 -*-
import sys, simplejson, copy

from jit_utils import JIT
from lkf_addons.addons.base.app import CargaUniversal

from account_settings import *


if __name__ == '__main__':
    jit_obj = JIT(settings, sys_argv=sys.argv, use_api=True)
    cu_obj = CargaUniversal(settings, sys_argv=sys.argv, use_api=True)
    jit_obj.console_run()
    response = jit_obj.upsert_reorder_point()
    res = cu_obj.update_status_record('reglas_reorden')
    print('==========res',res)
    # res = class_obj.update_status_record(estatus)

    # sys.stdout.write(simplejson.dumps({
    #     'status': 101,
    #     'replace_ans': jit_obj.answers,
    #     }))
