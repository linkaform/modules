# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timezone, timedelta
from linkaform_api import settings, base #, utils, network #base
from account_settings import *

def get_end_dates():
    # datos del registro
    end_ts = current_record['end_timestamp']
    offset_min = current_record.get('tz_offset', 0)

    # datetime local del registro
    dt_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    dt_local = dt_utc + timedelta(minutes=offset_min)

    # obtener las 00:00 locales de esa fecha
    dt_local_midnight = dt_local.replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # convertir esas 00:00 locales a UTC
    dt_midnight_utc = dt_local_midnight - timedelta(minutes=offset_min)

    return dt_local.strftime(format_date), dt_midnight_utc.strftime(format_date)

def get_record_check_in_today(str_date_from):
    return lkf.cr.find_one({
        'form_id': lkf.form_id,
        'deleted_at': {'$exists': False},
        'created_at': {'$gte': datetime.strptime( str_date_from, format_date )},
        'answers.a00000000000000000000007': 'check_in',
        'created_by_id': user_id
    },{'folio': 1, 'created_at': 1})

def update_record_action(action, dic_record, date_checkin, date_checkout):
    # record_id = dic_record.get('_id').get('$oid')
    #--Geolocation
    dic_geolocation = { 'latitude': 0, 'longitude': 0 }
    geolocalitation = dic_record.get('geolocation',[])
    if len(geolocalitation) == 2:
        dic_geolocation['latitude'] = geolocalitation[0]
        dic_geolocation['longitude'] = geolocalitation[1]
    #--Date
    
    # end_timestamp = current_record.get('end_timestamp') + current_record.get('tz_offset',0)*60
    # date_str = datetime.fromtimestamp(end_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    date_str, start_today_utc = get_end_dates()

    print(f'\n +++ user_id= {user_id} date_str= {date_str} start_today_utc= {start_today_utc} \n')

    
    if not lkf.folio:
        record_check = get_record_check_in_today(start_today_utc)
        print('     record_check= ',record_check)

        if type_check == 'check_in' and record_check:
            msg_error_app = {
                "a00000000000000000000007":{
                    "msg": ["Solo puede crear 1 check in al día, si ya realizaste checkin en este dia recuerda hacer checkout desde el menú de Inbox"],
                    "label": "Accion", "error":[]
                }
            }
            raise Exception(simplejson.dumps(msg_error_app))
        elif type_check == 'check_out':
            msg_error_app = {
                "a00000000000000000000007":{
                    "msg": ["No se puede hacer Checkout sin su checkin previo"],
                    "label": "Accion", "error":[]
                }
            }
            raise Exception(simplejson.dumps(msg_error_app))
    
    if type_check == 'check_in' and not date_checkin:
        current_record['answers']['a00000000000000000000001'] = date_str
        current_record['answers']['a00000000000000000000005'] = dic_geolocation

    elif type_check == 'check_out' and not date_checkout:
        current_record['answers']['a00000000000000000000002'] = date_str
        current_record['answers']['a00000000000000000000006'] = dic_geolocation

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': current_record['answers']
    }))

if __name__ == '__main__':
    # print(sys.argv)
    # current_record = simplejson.loads(sys.argv[1])
    lkf = base.LKF_Base(settings, sys_argv=sys.argv)
    lkf.console_run()
    current_record = lkf.current_record
    user_id = lkf.user['user_id']

    format_date = '%Y-%m-%d %H:%M:%S'

    type_check = lkf.answers.get('a00000000000000000000007','')
    date_checkin = lkf.answers.get('a00000000000000000000001','')
    date_checkout = lkf.answers.get('a00000000000000000000002','')
    update_record_action(type_check, current_record, date_checkin, date_checkout)
