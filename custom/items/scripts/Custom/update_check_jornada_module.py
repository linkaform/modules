# -*- coding: utf-8 -*-
import sys, simplejson
# from datetime import datetime, timezone, timedelta
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.type_check = self.answers.get('a00000000000000000000007','')
        self.date_checkin = self.answers.get('a00000000000000000000001','')
        self.date_checkout = self.answers.get('a00000000000000000000002','')
        self.users_no_validate = [29827, 29826]

    def error_checkin(self, msg_err):
        msg_error_app = {
            "a00000000000000000000007": {
                "msg": [msg_err], "label": "Accion", "error":[]
            }
        }
        raise Exception(simplejson.dumps(msg_error_app))

    def update_record_action(self):
        #--Geolocation
        dic_geolocation = { 'latitude': 0, 'longitude': 0 }
        geolocalitation = self.current_record.get('geolocation',[])
        if len(geolocalitation) == 2:
            dic_geolocation['latitude'] = geolocalitation[0]
            dic_geolocation['longitude'] = geolocalitation[1]
        #--Date
        
        # end_timestamp = current_record.get('end_timestamp') + current_record.get('tz_offset',0)*60
        # date_str = datetime.fromtimestamp(end_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        date_str, start_today_utc = self.get_end_dates()

        print(f'\n +++ user_id= {self.user_id} date_str= {date_str} start_today_utc= {start_today_utc} \n')

        
        if not lkf.folio and self.user_id not in self.users_no_validate:
            record_check = self.get_record_check_in_today(start_today_utc)
            print('     record_check= ',record_check)

            if self.type_check == 'check_in' and record_check:
                self.error_checkin("Solo puede crear 1 check in al día, si ya realizaste checkin en este dia recuerda hacer checkout desde el menú de Inbox")
            elif self.type_check == 'check_out':
                self.error_checkin("No se puede hacer Checkout sin su checkin previo")
        
        if self.type_check == 'check_in' and not self.date_checkin:
            self.current_record['answers']['a00000000000000000000001'] = date_str
            self.current_record['answers']['a00000000000000000000005'] = dic_geolocation

        elif self.type_check == 'check_out' and not self.date_checkout:
            self.current_record['answers']['a00000000000000000000002'] = date_str
            self.current_record['answers']['a00000000000000000000006'] = dic_geolocation

        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': self.current_record['answers']
        }))

if __name__ == '__main__':
    lkf = Custom(settings, sys_argv=sys.argv)
    lkf.console_run()
    
    lkf.update_record_action()