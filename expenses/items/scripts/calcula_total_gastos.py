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
    catalog_obj_id = exp_obj.CATALOG_SOL_VIAJE_OBJ_ID

    '''
    # Validar que la Fecha del gasto esté dentro del periodo de la solicitud
    '''
    fecha_del_gasto = answers.get( exp_obj.f['fecha_gasto'] )
    if fecha_del_gasto:
        fecha_salida = exp_obj.unlist(answers.get( catalog_obj_id, {} ).get(exp_obj.f['fecha_salida'], []))
        fecha_regreso = exp_obj.unlist(answers.get( catalog_obj_id, {} ).get(exp_obj.f['fecha_regreso'], []))
        print('fecha_salida = ',fecha_salida)
        print('fecha_regreso = ',fecha_regreso)

        if not ( exp_obj.str_to_date(fecha_salida) <= exp_obj.str_to_date(fecha_del_gasto) <= exp_obj.str_to_date(fecha_regreso) ):
            msg_error_app = {
                exp_obj.f['fecha_gasto']:{"msg": ["La fecha del gasto debe estar dentro del periodo de la solicitud"], "label": "Fecha del Gasto", "error":[]},
            }
            raise Exception(simplejson.dumps(msg_error_app))

    exp_obj.current_record['answers'] = exp_obj.get_total(answers)
    folio = answers.get(catalog_obj_id,{}).get(exp_obj.f['cat_folio'])
    update_ok = exp_obj.update_solicitud(folio, run_validations=True, background=True)
    if update_ok:
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': exp_obj.current_record['answers']
            }))
    else:
        msg_error_app = {
            exp_obj.f['cat_folio']:{"msg": ["Error al actualizar la solicitud"], "label": "Numero de Solicitud", "error":[]},
        }
        raise Exception(simplejson.dumps(msg_error_app))