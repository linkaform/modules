# coding: utf-8
from copy import deepcopy
import sys, simplejson, time, datetime
from linkaform_api import settings
from account_settings import *

from oracle import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.class_cr = self.get_db_cr('Oracle')
        #use self.lkm.catalog_id() to get catalog id
        self.name =  __class__.__name__
        self.settings = settings



    def run_query(self):
        db_name = 'PRODUCCION.VW_LinkAForm_Hora'
        query = "SELECT * FROM PRODUCCION.VW_LinkAForm_Hora "
        query += " WHERE "
        query += " VARIA='LECBRIXJCLARO'"
        query += """ AND TRUNC(DATA) + (TO_DATE(HORA, 'HH24:MI') - TRUNC(TO_DATE(HORA, 'HH24:MI')))
                >= TO_DATE('2025-11-19 00:00', 'YYYY-MM-DD HH24:MI')
        """
        print('query=',query)
        for x in range(3):
            header, data = self.query_view(db_name, query=query, date_format=True)
            # print('data=',data)
            print('date', datetime.datetime.now())
            res = ''
            for d in data:
                res +='row = DATA: {} , Varia: {}, Valor: {}, Hora: {}'.format(d['DATA'],d['VARIA'],d['VALOR'],d['HORA'])
                res += "\n"
            print('data=',res)
            print('='*70)
            time.sleep(1)



if __name__ == "__main__":
    """
    Formato last_update_date = '2025-08-25 15:00'
    """
    oo  = settings.config['ORACLE_HOST']
    module_obj = Oracle(settings, sys_argv=sys.argv)
    module_obj.console_run()
    module_obj.db_updated_at = time.time()
    #gg = module_obj.search_views()
    # print('account settings', module_obj.settings.config)
    data = module_obj.data.get('data',{})
    option = data.get("option",'read')
    module_obj.run_query()