# coding: utf-8
import sys, simplejson, json, pytz, os
import tempfile, unicodedata, threading

from datetime import datetime
from bson.objectid import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed
from hmac import new
import time, random


from accesos_utils import Accesos
from account_settings import *
from linkaform_api import settings


class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    data_rondin = acceso_obj.current_record
    acceso_obj.user_name = data_rondin.get('created_by_name', '')
    acceso_obj.user_id = data_rondin.get('created_by_id', acceso_obj.user.get('user_id', 0))
    acceso_obj.user_email = data_rondin.get('created_by_email', acceso_obj.user.get('email', ''))
    acceso_obj.geolocation = data_rondin.get('geolocation', [])
    script_attr = acceso_obj.data
    data = acceso_obj.data.get('data', {})
    acceso_obj.test = acceso_obj.data.get('test')
    option = data.get("option", script_attr.get('option', ''))
    _id = data.get("_id", None)
    _rev = data.get("_rev", None)
    records = data.get("records", [])
    user_to_assign = data.get("user_to_assign", {})
    response = {}
    db_name = f'clave_{acceso_obj.user_id}'
    acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
    if option == 'get_user_catalogs':
        response = acceso_obj.get_user_catalogs()
    # elif option == 'sync_to_lkf':
    #     db_name = f'clave_{acceso_obj.user_id}'
    #     acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
    #     record = acceso_obj.get_couch_record(_id=_id, _rev=_rev)
    #     record = dict(record)
    #     type_sync = record.get('type', '')
    #     if type_sync == 'incidencia':
    #         response = acceso_obj.sync_incidence_to_lkf(record=record)
        # elif type_sync == 'check_area':
        #     response = acceso_obj.sync_check_area_to_lkf(complete_record=record)
        # elif type_sync == 'rondin':
        #     response = acceso_obj.sync_rondin_to_lkf(data=record)
        # elif type_sync == 'error':
        #     response = record
        # else:
        #     response = {'status_code': 400, 'type': 'error', 'msg': 'Unknown error', 'data': {
        #         'type_sync': type_sync,
        #         'record': record,
        #         'db_name': db_name,
        #     }}
    elif option == 'assign_user_inbox':
        response = acceso_obj.assign_user_inbox(data=acceso_obj.answers)
    elif option == 'complete_rondines':
        response = acceso_obj.complete_rondines(records=records)
    elif option == 'delete_rondines':
        response = acceso_obj.delete_rondines(records=records)
    elif option == 'reasignar_rondines':
        response = acceso_obj.reasignar_rondines(records=records, user_to_assign=user_to_assign)
    elif option == 'get_active_guards':
        response = acceso_obj.get_active_guards()
    elif option in ('sync','synced','rondin','check_area','sync_to_lkf'):
        #todo quitar sync_to_lkf meterlo al new worflos
        if  option == 'sync_to_lkf':
            db_name = f'clave_{acceso_obj.user_id}'
            acceso_obj.cr_db = acceso_obj.get_couch_user_db(db_name)
            record = acceso_obj.get_couch_record(_id=_id, _rev=_rev)
            record = dict(record)
            type_sync = record.get('type', '')
            if type_sync == 'incidencia':
                response = acceso_obj.sync_incidence_to_lkf(record=record)
            else:
                response = acceso_obj.sync_records(records)
        else:
            response = acceso_obj.sync_records(records)
    elif option == 'clean_db':
        response = acceso_obj.clean_db()
    elif option == 'fix':
        response = acceso_obj.fix_rondines()

    else:
        response = {'status_code': 400, 'type': 'error', 'msg': 'Invalid option', 'data': {}}

    sys.stdout.write(simplejson.dumps(response))