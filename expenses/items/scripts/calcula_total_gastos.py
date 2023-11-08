# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime
from copy import deepcopy

from lkf_addons.addons.expenses.expense_utils import Expenses

from account_settings import *

if __name__ == '__main__':
    exp_obj = Expenses(settings, sys_argv=sys.argv)
    exp_obj.console_run()
    answers = exp_obj.current_record['answers']
    exp_obj.current_record['answers'] = exp_obj.get_total(answers)
    catalog_obj_id = exp_obj.CATALOG_SOL_VIAJE_OBJ_ID
    folio = answers.get(catalog_obj_id,{}).get(exp_obj.f['cat_folio'])
    update_ok = exp_obj.update_solicitud(folio, run_validations=True, background=True)
    if update_ok:
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': exp_obj.current_record['answers']
            }))
    else:
        msg_error_app = {
            "610419b5d28657c73e36fcd3":{"msg": ["Error al actualizar la solicitud"], "label": "Numero de Solicitud", "error":[]},
        }
        raise Exception(simplejson.dumps(msg_error_app))