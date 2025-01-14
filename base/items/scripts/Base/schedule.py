# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta
from linkaform_api import settings, network, utils
from bson import errors, ObjectId

from lkf_addons.addons.base.app import Schedule

from account_settings import *


class Schedule(Schedule):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):

        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.mf.update({
            'pais_localidad':'631fccdd844ed53c7d989718',
            'localidad':'631fc1e48d9fe191da0c3daf',
            })


        self.LOCALIDADES = self.lkm.catalog_id('localidades')
        self.LOCALIDADES_ID = self.LOCALIDADES.get('id')
        self.LOCALIDADES_OBJ_ID = self.LOCALIDADES.get('obj_id')

    def get_answers_map(self, task, description, first_date, due_date, status, field_map, group_field_map ):
        ans = super().get_answers_map(task, description, first_date, due_date, status, field_map, group_field_map )
        localidad = self.answers.get(self.LOCALIDADES_OBJ_ID)
        if localidad:
            ans.update({self.LOCALIDADES_OBJ_ID:localidad})
        return ans


    def get_dag_dates(self, data):
        res = {}
        print('data=', data)
        dag_info = self.unlist(data.get('dag_info',{}))
        print('dag_info=', dag_info)
        next_run = dag_info.get('next_dagrun')
        create_after = dag_info.get('next_dagrun_create_after')
        if next_run and create_after:
            res ={
                'abcde000100000000000f000': self.lkf_date(next_run),
                'abcde000100000000000f001': self.lkf_date(create_after),
            }
        return res   

if __name__ == "__main__":
    # print(sys.argv)
    schedule_obj = Schedule(settings, sys_argv=sys.argv, use_api=True)
    schedule_obj.console_run()
    lkf_api = schedule_obj.lkf_api
    res = schedule_obj.schedule_task()
    data = res.get('data')
    print('res', res)
    if res.get('status_code') == 0:
        print('Ningun cambio')
    elif res.get('status_code') == 200:
        if res.get('deleted'):
            schedule_obj.answers['abcde00010000000a0000000'] = 'eliminado'
        else:
            schedule_obj.answers['abcde0001000000000000000'] = data.get('dag_id')
            try:
                schedule_obj.answers.update(schedule_obj.get_dag_dates(data))
            except:
                print('no pudo obtener proximas fechas.')
            if res.get('is_paused') == True:
                schedule_obj.answers['abcde00010000000a0000000'] = 'pausado'
            else:
                schedule_obj.answers['abcde00010000000a0000000'] = 'corriendo'

        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': schedule_obj.answers,
        }))
    else:
        print('res===0',res)

        msg_error_app = "Something went wrong!!!"
        if res.get('json',{}).get('error') or res.get('status_code') == 400:
            if res.get('json',{}).get('error'):
                msg_error_app = res['json']['error']
            else:
                msg_error_app = res['json'].get('message','Something went wrong!!!')
        else:
            msg_error_app = {
                "error":{"msg": [msg_error_app], "label": "Cron Id", "error":[msg_error_app]},
            }
        raise Exception(simplejson.dumps(msg_error_app))