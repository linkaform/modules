# -*- coding: utf-8 -*-
import sys, simplejson
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def complete_current_record(self):
        date_str, start_today_utc = self.get_end_dates()
        record_check = self.get_record_check_in_today(start_today_utc, get_catalog_data=True)

        print(f'\n +++ user_id= {self.user_id} date_str= {date_str} start_today_utc= {start_today_utc} \n')

        if not record_check:
            msg_error_app = {
                "folio": { "msg": ["Debes realizar checkin primero"], "label": "Folio", "error":[] }
            }
            raise Exception(simplejson.dumps(msg_error_app))
        
        # data_catalog = {
        #     "69667a148942065f5657f079": "Sin checkin",
        #     "69667a148942065f5657f078": "Sin checkin",
        #     "69667a148942065f5657f077": "Sin checkin",
        #     "69667a148942065f5657f076": ["Sin checkin"]
        # }

        # if record_check:
        data_catalog = record_check.get('answers', {}).get('69667a148942065f5657f075')

        self.current_record['answers']['69667a148942065f5657f075'] = data_catalog
        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': self.current_record['answers']
        }))

if __name__ == '__main__':
    lkf = Custom(settings, sys_argv=sys.argv)
    lkf.console_run()
    
    lkf.complete_current_record()