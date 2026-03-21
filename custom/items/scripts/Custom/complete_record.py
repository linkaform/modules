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
        

        # if record_check:
        data_catalog_check = record_check.get('answers', {}).get('69667a148942065f5657f075', {})

        print(f'\n-- data_catalog_check = {data_catalog_check}\n')

        data_catalog = {}

        for fId in ["69667a148942065f5657f079", "69667a148942065f5657f078", "69a1ca103aa6ba9b4b24e674", "69667a148942065f5657f077"]:
            if data_catalog_check.get(fId) is not None:
                data_catalog[fId] = self.unlist(data_catalog_check[fId])

        if data_catalog_check.get('69667a148942065f5657f076'):
            data_catalog['69667a148942065f5657f076'] = [ self.unlist(data_catalog_check.get("69667a148942065f5657f076")) ]

        
        # data_catalog = {
        #     "69667a148942065f5657f079": data_catalog_check.get("69667a148942065f5657f079"), # Estado
        #     "69667a148942065f5657f078": data_catalog_check.get("69667a148942065f5657f078"), # Cadena
        #     "69a1ca103aa6ba9b4b24e674": data_catalog_check.get("69a1ca103aa6ba9b4b24e674"), # Supervisor
        #     "69667a148942065f5657f077": data_catalog_check.get("69667a148942065f5657f077"), # Tienda
        #     "69667a148942065f5657f076": [ self.unlist(data_catalog_check.get("69667a148942065f5657f076")) ], # Codigo
        # }

        self.current_record['answers']['69667a148942065f5657f075'] = data_catalog
        sys.stdout.write(simplejson.dumps({
            'status': 101,
            'replace_ans': self.current_record['answers']
        }))

if __name__ == '__main__':
    lkf = Custom(settings, sys_argv=sys.argv)
    lkf.console_run()
    
    lkf.complete_current_record()