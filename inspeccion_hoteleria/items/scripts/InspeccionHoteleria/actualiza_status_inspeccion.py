# -*- coding: utf-8 -*-
import sys

from inspeccion_hoteleria_utils import Inspeccion_Hoteleria

from account_settings import *


preguntas_revisar = ['67ed67dfc61cd5bd89f317f9',
                    '67ed67dfc61cd5bd89f317fa',
                    '67ed662ab178b35ea5a103b8',
                    '67ed662ab178b35ea5a103b9']

class Inspeccion_Hoteleria(Inspeccion_Hoteleria):


    def update_status_record(self,  status, msg_comentarios='' ):
        print('status...', status)
        answers = {self.field_id_status:status}
        res = self.lkf_api.patch_multi_record( answers = answers, form_id=self.form_id, record_id=[self.record_id])
        print('res',res)
        return res

    def check_pending_answers(self):
        status = 'proceso'
        t = 0
        for q_id in preguntas_revisar:
            if self.answers.get(q_id):
                if self.answers.get(q_id) in ('si','no', 'sí'):
                   t += 1
        if t == len(preguntas_revisar):
            status = 'completada'
        return status


if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=True)
    module_obj.console_run()
    print('folio', module_obj.folio)
    status = module_obj.check_pending_answers()
    print('Record status', module_obj.answers[module_obj.field_id_status])
    print('status to update', status)
    module_obj.answers[module_obj.field_id_status] = status
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': module_obj.answers,
        'metadata':{'editable':False},
    }))
    # if status != module_obj.answers[module_obj.field_id_status]:
    #     res = module_obj.update_status_record(status)
    #     print('r1111es',res['status_code'])
    #     if res.get('status_code') in (200, 201):
    #         print('updating')
    #         update_res = module_obj.cr.update_many({'folio':module_obj.folio}, {'$set':{'editable': False}})
    #         print('updating',module_obj.cr)

